use super::*;
use crate::{WriterSettings, WriterSettingsSnapshot};
#[cfg(feature = "io-fault-injection")]
use std::time::Duration;

fn message(n: u8) -> Message {
    Message {
        flags: 0,
        headers: vec![],
        payload: vec![n; 64],
    }
}
// Most cases check written records that no sync has covered yet.
fn config() -> KeratinConfig {
    KeratinConfig {
        background_sync_interval: std::time::Duration::ZERO,
        ..KeratinConfig::test_default()
    }
}

fn enqueue_update(
    log: &Keratin,
    expected: u64,
    settings: WriterSettings,
) -> oneshot::Receiver<io::Result<WriterSettingsSnapshot>> {
    let (respond_to, reply) = oneshot::channel();
    log.tx
        .send(WriterCmd::ReplaceSettings {
            expected_revision: expected,
            settings,
            respond_to,
        })
        .unwrap();
    reply
}

#[tokio::test]
async fn live_settings_upgrade_persists_prefix_and_keeps_explicit_overrides() {
    let dir = crate::test_dir!("live_settings_upgrade");
    let cfg = KeratinConfig {
        default_durability: KDurability::AfterFlush,
        ..config()
    };
    let log = Keratin::open(&dir.root, cfg).await.unwrap();
    log.append_batch(vec![message(0)], None).await.unwrap();
    assert!(log.reader().scan_from(0, 1).unwrap().is_empty());
    let old = log.writer_settings().await.unwrap();
    let mut next = old.settings;
    next.default_durability = KDurability::AfterFsync;
    next.max_batch_bytes = 1; // A single oversized append must still progress.
    next.max_batch_records = 1;
    next.batch_linger_ms = 0;
    next.fsync_interval_ms = 10;
    let applied = log
        .replace_writer_settings(old.revision, next)
        .await
        .unwrap();
    assert_eq!(applied.revision, 1);
    assert_eq!(log.reader().scan_from(0, 1).unwrap().len(), 1);
    log.append_batch(vec![message(1)], None).await.unwrap();
    assert_eq!(log.reader().scan_from(0, 2).unwrap().len(), 2);
    log.append_batch(vec![message(2)], Some(KDurability::AfterFlush))
        .await
        .unwrap();
    assert!(log.reader().scan_from(2, 1).unwrap().is_empty());
    log.shutdown().await.unwrap();
    drop(log);
    let reopened = Keratin::open(&dir.root, cfg).await.unwrap();
    assert_eq!(
        reopened.writer_settings().await.unwrap(),
        WriterSettingsSnapshot {
            revision: 0,
            settings: cfg.into()
        }
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn live_settings_cas_invalid_updates_and_lost_response() {
    let dir = crate::test_dir!("live_settings_cas");
    let log = Keratin::open(&dir.root, config()).await.unwrap();
    let initial = log.writer_settings().await.unwrap();
    let mut invalid = initial.settings;
    invalid.max_batch_records = 0;
    assert_eq!(
        log.replace_writer_settings(0, invalid)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    // The writer validates too, even if a caller bypasses the convenience API.
    assert_eq!(
        enqueue_update(&log, 0, invalid)
            .await
            .unwrap()
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(log.writer_settings().await.unwrap(), initial);
    let mut next = initial.settings;
    next.max_batch_bytes /= 2;
    drop(enqueue_update(&log, 0, next)); // Admitted update survives caller cancellation.
    let stale = enqueue_update(&log, 0, initial.settings);
    assert_eq!(
        stale.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(
        log.writer_settings().await.unwrap(),
        WriterSettingsSnapshot {
            revision: 1,
            settings: next
        }
    );
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn live_settings_apply_to_follower_default_and_preserve_override() {
    let dir = crate::test_dir!("live_settings_follower");
    let log = Keratin::open(&dir.root, config()).await.unwrap();
    log.become_follower();
    let old = log.writer_settings().await.unwrap();
    let mut next = old.settings;
    next.default_durability = KDurability::AfterFlush;
    log.replace_writer_settings(0, next).await.unwrap();
    log.append_replicated_batch(1, 0, vec![message(0)], None)
        .await
        .unwrap();
    assert!(log.reader().scan_from(0, 1).unwrap().is_empty());
    log.append_replicated_batch(1, 1, vec![message(1)], Some(KDurability::AfterFsync))
        .await
        .unwrap();
    assert_eq!(log.reader().scan_from(0, 2).unwrap().len(), 2);
    log.shutdown().await.unwrap();
}

#[cfg(feature = "io-fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_settings_fifo_downgrade_never_releases_old_durable_ack_early() {
    use crate::io_fault_injection::{Fault, arm_gated};
    let dir = crate::test_dir!("live_settings_fifo");
    let log = Keratin::open(&dir.root, config()).await.unwrap();
    let original = log.writer_settings().await.unwrap();
    let gate = arm_gated(
        &dir.root.join("segments/00000000000000000000.log"),
        Fault::FsyncPause,
    )
    .unwrap();
    let mut before = log.append_enqueue_receiver(message(0), None).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while gate.hits() == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let mut next = original.settings;
    next.default_durability = KDurability::AfterFlush;
    next.fsync_interval_ms = 60_000; // Cannot postpone the old receipt to this timer.
    let mut update = enqueue_update(&log, 0, next);
    let mut after = log.append_enqueue_receiver(message(1), None).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(matches!(
        before.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        update.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(matches!(
        after.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    gate.release();
    tokio::time::timeout(Duration::from_secs(3), async {
        assert_eq!(before.await.unwrap().unwrap().base_offset, 0);
        assert_eq!(update.await.unwrap().unwrap().revision, 1);
        assert_eq!(after.await.unwrap().unwrap().base_offset, 1);
    })
    .await
    .unwrap();
    assert_eq!(log.reader().scan_from(0, 2).unwrap().len(), 1);
    // A caller can still demand durability after a weaker default is adopted.
    log.append_batch(vec![message(2)], Some(KDurability::AfterFsync))
        .await
        .unwrap();
    assert_eq!(log.reader().scan_from(0, 3).unwrap().len(), 3);
    log.shutdown().await.unwrap();
}

#[cfg(feature = "io-fault-injection")]
#[tokio::test]
async fn live_settings_sync_failure_latches_the_log() {
    use crate::io_fault_injection::{Fault, arm};
    let dir = crate::test_dir!("live_settings_failed_sync");
    let cfg = KeratinConfig {
        default_durability: KDurability::AfterFlush,
        ..config()
    };
    let log = Keratin::open(&dir.root, cfg).await.unwrap();
    log.append_batch(vec![message(0)], None).await.unwrap();
    let initial = log.writer_settings().await.unwrap();
    let fault = arm(
        &dir.root.join("segments/00000000000000000000.log"),
        Fault::Fsync,
    )
    .unwrap();
    let mut next = initial.settings;
    next.default_durability = KDurability::AfterFsync;
    let update = enqueue_update(&log, 0, next);
    let after = log.append_enqueue_receiver(message(1), None).unwrap();
    assert!(update.await.unwrap().is_err());
    assert_eq!(fault.hits(), 1);
    let after = after.await.unwrap().unwrap_err();
    assert!(after.to_string().contains("reopen required"), "{after}");
    assert!(log.writer_settings().await.is_err());
    assert!(log.sync().await.is_err());
    assert!(log.reader().scan_from(0, 1).unwrap().is_empty());
    log.shutdown().await.unwrap();
}

#[cfg(feature = "io-fault-injection")]
#[tokio::test]
async fn live_settings_full_mailbox_does_not_block_async_runtime() {
    use crate::io_fault_injection::{Fault, arm_gated};
    let dir = crate::test_dir!("live_settings_backpressure");
    let cfg = KeratinConfig {
        writer_buffer_factor: 1,
        ..config()
    };
    let log = Keratin::open(&dir.root, cfg).await.unwrap();
    let old = log.writer_settings().await.unwrap();
    let gate = arm_gated(
        &dir.root.join("segments/00000000000000000000.log"),
        Fault::FsyncPause,
    )
    .unwrap();
    let update = enqueue_update(&log, 0, old.settings);
    tokio::time::timeout(Duration::from_secs(3), async {
        while gate.hits() == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    for _ in 0..log.tx.capacity().unwrap() {
        let (respond_to, _) = oneshot::channel();
        log.tx
            .try_send(WriterCmd::ReadSettings { respond_to })
            .unwrap();
    }
    // Single-thread runtime remains able to drive its timer on a full mailbox.
    assert!(
        tokio::time::timeout(Duration::from_millis(20), log.writer_settings())
            .await
            .is_err()
    );
    gate.release();
    update.await.unwrap().unwrap();
    assert_eq!(log.writer_settings().await.unwrap().revision, 1);
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn live_settings_closure_edits_copy_and_validates_before_adoption() {
    let dir = crate::test_dir!("live_settings_closure");
    let log = Keratin::open(&dir.root, config()).await.unwrap();
    let original = log.writer_settings().await.unwrap();
    let applied = log
        .update_writer_settings(|next| {
            next.max_batch_bytes = 256 * 1024;
            next.max_batch_records = 64;
        })
        .await
        .unwrap();
    assert_eq!(applied.revision, original.revision + 1);
    assert_eq!(applied.settings.max_batch_bytes, 256 * 1024);
    assert_eq!(applied.settings.max_batch_records, 64);
    assert_eq!(
        applied.settings.default_durability,
        original.settings.default_durability
    );
    assert_eq!(log.writer_settings().await.unwrap(), applied);
    assert_eq!(
        log.update_writer_settings(|next| next.max_batch_bytes = 0)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(log.writer_settings().await.unwrap(), applied);
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn live_settings_closure_conflict_does_not_replay_or_overwrite() {
    let dir = crate::test_dir!("live_settings_closure_conflict");
    let log = Keratin::open(&dir.root, config()).await.unwrap();
    let original = log.writer_settings().await.unwrap();
    let mut calls = 0;
    let captured = String::from("consumed once");
    let result = log
        .update_writer_settings(|next| {
            drop(captured); // FnOnce closures need not be reusable.
            calls += 1;
            let mut competing = *next;
            competing.max_batch_records = 7;
            // Another edit enters the FIFO before this helper's replacement.
            drop(enqueue_update(&log, original.revision, competing));
            next.max_batch_bytes = 123;
        })
        .await;
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    assert_eq!(calls, 1);
    let winner = log.writer_settings().await.unwrap();
    assert_eq!(winner.revision, original.revision + 1);
    assert_eq!(winner.settings.max_batch_records, 7);
    assert_eq!(
        winner.settings.max_batch_bytes,
        original.settings.max_batch_bytes
    );
    log.shutdown().await.unwrap();
}
