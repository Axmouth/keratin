#![cfg(feature = "io-fault-injection")]
use keratin_log::io_fault_injection::{Fault, arm};
use keratin_log::*;

fn message(payload: impl Into<Vec<u8>>) -> Message {
    Message {
        payload: payload.into(),
        flags: 0,
        headers: vec![],
    }
}

// These tests arm faults around records that are written but not synced, so
// a background sync must not cover them first.
fn config() -> KeratinConfig {
    KeratinConfig {
        background_sync_interval: std::time::Duration::ZERO,
        ..KeratinConfig::test_default()
    }
}

fn manifest_clean_flag(root: &std::path::Path) -> bool {
    let bytes = std::fs::read(root.join("manifest.bin")).unwrap();
    let flags = u16::from_be_bytes(bytes[10..12].try_into().unwrap());
    flags & 0x0001 != 0
}

// After a failed fsync the kernel may drop the dirty pages and report success
// on the next fsync, so the handle must stop instead of letting a later sync
// cover the failed range.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_fsync_stops_the_log_until_reopen() {
    for fault in [Fault::Fsync, Fault::FsyncAfterSuccess] {
        let dir = test_dir!("failed_fsync_stops_log");
        let log = Keratin::open(&dir.root, config())
            .await
            .unwrap();
        log.append(message(vec![1; 64]), Some(KDurability::AfterFsync))
            .await
            .unwrap();
        let durable = log.durable_offset();
        let armed = arm(&dir.root.join("segments/00000000000000000000.log"), fault).unwrap();
        assert!(
            log.append(message(vec![2; 64]), Some(KDurability::AfterFsync))
                .await
                .is_err()
        );
        assert_eq!(armed.hits(), 1);
        drop(armed);

        let later = log
            .append(message(vec![3; 64]), Some(KDurability::AfterFsync))
            .await;
        assert!(later.is_err(), "{fault:?}: append after failure must fail");
        assert!(log.sync().await.is_err());
        assert!(log.truncate_before(1).await.is_err());
        assert_eq!(log.durable_offset(), durable);
        assert!(log.reader().scan_from(1, 10).unwrap().is_empty());

        // Shutdown still releases the handle but leaves the log dirty, so the
        // next open runs the recovery scan.
        log.shutdown().await.unwrap();
        assert!(!manifest_clean_flag(&dir.root));
        drop(log);

        let reopened = Keratin::open(&dir.root, config())
            .await
            .unwrap();
        let next = reopened
            .append(message(vec![4; 64]), Some(KDurability::AfterFsync))
            .await
            .unwrap();
        assert!(next.base_offset >= 1);
        reopened.shutdown().await.unwrap();
    }
}

// An AfterFlush write failure goes through the same stop as a failed fsync:
// later appends and syncs are refused, the durable frontier stays put, and the
// log stays dirty until a reopen runs the recovery scan.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_after_flush_write_stops_the_log_until_reopen() {
    for fault in [Fault::Write, Fault::PartialWrite] {
        let dir = test_dir!("failed_after_flush_stops_log");
        let log = Keratin::open(&dir.root, config())
            .await
            .unwrap();
        log.append(message(vec![1; 64]), Some(KDurability::AfterFsync))
            .await
            .unwrap();
        let durable = log.durable_offset();
        let armed = arm(&dir.root.join("segments/00000000000000000000.log"), fault).unwrap();
        assert!(
            log.append(message(vec![2; 64]), Some(KDurability::AfterFlush))
                .await
                .is_err()
        );
        assert_eq!(armed.hits(), 1);
        drop(armed);

        let later = log
            .append(message(vec![3; 64]), Some(KDurability::AfterFlush))
            .await
            .expect_err("append after failure must fail");
        assert!(
            later.to_string().contains("reopen required"),
            "{fault:?}: {later}"
        );
        assert!(log.sync().await.is_err());
        assert!(log.truncate_before(1).await.is_err());
        assert_eq!(log.durable_offset(), durable);

        log.shutdown().await.unwrap();
        assert!(!manifest_clean_flag(&dir.root));
        drop(log);

        let reopened = Keratin::open(&dir.root, config())
            .await
            .unwrap();
        let next = reopened
            .append(message(vec![4; 64]), Some(KDurability::AfterFsync))
            .await
            .unwrap();
        assert!(next.base_offset >= 1);
        reopened.shutdown().await.unwrap();
    }
}

fn messages(range: std::ops::Range<u8>) -> Vec<Message> {
    range.map(|value| message(vec![value; 64])).collect()
}

// A replicated AfterFsync append syncs on the writer thread instead of the
// fsync worker. Its failure must stop the log the same way, so a retry over
// the failed range cannot come back as AlreadyPresent and count as durable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_replicated_fsync_stops_the_log_until_reopen() {
    for fault in [Fault::Fsync, Fault::FsyncAfterSuccess] {
        let dir = test_dir!("failed_replicated_fsync_stops_log");
        let log = Keratin::open(&dir.root, config())
            .await
            .unwrap();
        log.become_follower();
        let epoch = log.current_epoch();
        log.append_replicated_batch(epoch, 0, messages(0..2), Some(KDurability::AfterFsync))
            .await
            .unwrap();
        let durable = log.durable_next();
        assert_eq!(durable, 2);

        let armed = arm(&dir.root.join("segments/00000000000000000000.log"), fault).unwrap();
        assert!(
            log.append_replicated_batch(epoch, 2, messages(2..4), Some(KDurability::AfterFsync))
                .await
                .is_err()
        );
        assert_eq!(armed.hits(), 1, "{fault:?}");
        drop(armed);
        assert_eq!(log.durable_next(), durable);

        for (first, batch, durability) in [
            (2, messages(2..4), KDurability::AfterFsync),
            (0, messages(0..2), KDurability::AfterFsync),
            (4, messages(4..5), KDurability::AfterFsync),
            (2, messages(2..4), KDurability::AfterWrite),
            (2, messages(2..4), KDurability::AfterFlush),
            (3, vec![], KDurability::AfterFsync),
        ] {
            let error = log
                .append_replicated_batch(epoch, first, batch, Some(durability))
                .await
                .expect_err("replicated append after failure must fail");
            assert!(
                error.to_string().contains("reopen required"),
                "{fault:?} {durability:?} at {first}: {error}"
            );
        }
        assert!(log.sync().await.is_err());
        assert_eq!(log.durable_next(), durable);

        log.shutdown().await.unwrap();
        assert!(!manifest_clean_flag(&dir.root));
        drop(log);

        let reopened = Keratin::open(&dir.root, config())
            .await
            .unwrap();
        assert!(reopened.durable_next() >= durable);
        reopened.shutdown().await.unwrap();
    }
}

// Segment roll and checkpoint reset sync the outgoing segment before they
// change any files. A failure there must stop the log like any other sync.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_roll_or_reset_fsync_stops_the_log_until_reopen() {
    for reset in [false, true] {
        let dir = test_dir!("failed_roll_or_reset_fsync_stops_log");
        let cfg = KeratinConfig {
            segment_max_bytes: 4096,
            ..config()
        };
        let log = Keratin::open(&dir.root, cfg).await.unwrap();
        log.become_follower();
        let epoch = log.current_epoch();
        log.append_replicated_batch(epoch, 0, messages(0..2), Some(KDurability::AfterWrite))
            .await
            .unwrap();

        let armed = arm(
            &dir.root.join("segments/00000000000000000000.log"),
            Fault::Fsync,
        )
        .unwrap();
        if reset {
            assert!(log.destructive_reset_to_checkpoint(10).await.is_err());
        } else {
            // Large enough to force a roll before staging.
            let big = vec![message(vec![7; 8192])];
            assert!(
                log.append_replicated_batch(epoch, 2, big, Some(KDurability::AfterWrite))
                    .await
                    .is_err()
            );
        }
        assert_eq!(armed.hits(), 1, "reset={reset}");
        drop(armed);

        let error = log
            .append_replicated_batch(epoch, 2, messages(2..3), Some(KDurability::AfterWrite))
            .await
            .expect_err("replicated append after failure must fail");
        assert!(error.to_string().contains("reopen required"), "{error}");
        assert!(log.sync().await.is_err());

        log.shutdown().await.unwrap();
        assert!(!manifest_clean_flag(&dir.root));
        drop(log);

        // The failed sync covered both records, so the reopen drops them
        // rather than trusting pages it cannot prove reached the disk. A
        // follower fetches them again from its owner.
        let reopened = Keratin::open(&dir.root, cfg).await.unwrap();
        assert_eq!(reopened.head_offset(), 0);
        assert!(reopened.reader().scan_from(0, 10).unwrap().is_empty());
        reopened.shutdown().await.unwrap();
    }
}

// Records written to the OS but never synced sit past the durable end. After a
// failed sync their pages may be gone while a later sync or read still
// succeeds, so the next open must cut back to the last successful sync instead
// of trusting what the recovery scan finds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reopen_after_failed_fsync_cuts_back_to_the_durable_end() {
    for fault in [Fault::Fsync, Fault::FsyncAfterSuccess] {
        let dir = test_dir!("latched_fsync_cut");
        let log = Keratin::open(&dir.root, config())
            .await
            .unwrap();
        log.append(message(vec![1; 64]), Some(KDurability::AfterFsync))
            .await
            .unwrap();
        for payload in [2u8, 3] {
            log.append(message(vec![payload; 64]), Some(KDurability::AfterFlush))
                .await
                .unwrap();
        }
        assert_eq!(log.reader().scan_written_with_byte_budget(0, 10, usize::MAX).unwrap().len(), 3);
        let armed = arm(&dir.root.join("segments/00000000000000000000.log"), fault).unwrap();
        assert!(
            log.append(message(vec![4; 64]), Some(KDurability::AfterFsync))
                .await
                .is_err()
        );
        drop(armed);
        // The cut records stop being readable as soon as the log latches.
        assert_eq!(log.written_next(), 1, "{fault:?}");
        assert_eq!(log.reader().scan_written_with_byte_budget(0, 10, usize::MAX).unwrap().len(), 1);
        log.shutdown().await.unwrap();
        drop(log);
        assert!(
            dir.root.join("latched-cut.pending").exists(),
            "{fault:?}: the latch must record the cut"
        );

        let reopened = Keratin::open(&dir.root, config())
            .await
            .unwrap();
        assert!(!dir.root.join("latched-cut.pending").exists());
        assert!(!dir.root.join("suffix-repair.pending").exists());
        let kept = reopened.reader().scan_from(0, 10).unwrap();
        assert_eq!(kept.len(), 1, "{fault:?}: only the synced record survives");
        assert_eq!(kept[0].payload, vec![1; 64]);
        let next = reopened
            .append(message(vec![5; 64]), Some(KDurability::AfterFsync))
            .await
            .unwrap();
        assert_eq!(next.base_offset, 1, "{fault:?}");
        reopened.shutdown().await.unwrap();
    }
}

// A log bound to a replicated history gets the same cut. Everything it
// confirmed, reported or sealed lies at or below its last successful sync, so
// reopening with its history guard succeeds and only unsynced records go.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bound_history_reopen_after_failed_fsync_cuts_back_to_the_durable_end() {
    let dir = test_dir!("latched_fsync_cut_bound");
    let created = Keratin::open(&dir.root, config())
        .await
        .unwrap();
    created.shutdown().await.unwrap();
    drop(created);

    let log = Keratin::open_preserving_history(&dir.root, config())
        .await
        .unwrap();
    log.append(message(vec![1; 64]), Some(KDurability::AfterFsync))
        .await
        .unwrap();
    log.append(message(vec![2; 64]), Some(KDurability::AfterFlush))
        .await
        .unwrap();
    let armed = arm(&dir.root.join("segments/00000000000000000000.log"), Fault::Fsync).unwrap();
    assert!(
        log.append(message(vec![3; 64]), Some(KDurability::AfterFsync))
            .await
            .is_err()
    );
    drop(armed);
    log.shutdown().await.unwrap();
    drop(log);

    let reopened = Keratin::open_preserving_history(&dir.root, config())
        .await
        .expect("a latched cut must not strand a bound history");
    let kept = reopened.reader().scan_from(0, 10).unwrap();
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].payload, vec![1; 64]);
    reopened.shutdown().await.unwrap();
}

// The power loss simulation drops what no sync covered, through the same cut
// a failed sync leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simulated_power_loss_drops_unsynced_records() {
    let dir = test_dir!("simulated_power_loss");
    let log = Keratin::open(&dir.root, config()).await.unwrap();
    log.append(message(vec![1; 64]), Some(KDurability::AfterFsync))
        .await
        .unwrap();
    for payload in [2u8, 3] {
        log.append(message(vec![payload; 64]), Some(KDurability::AfterFlush))
            .await
            .unwrap();
    }
    let durable = log.durable_next();
    assert_eq!(durable, 1);
    // A killed process leaves its manifest behind the records in the files.
    log.force_close().await.unwrap();
    keratin_log::io_fault_injection::lose_unsynced_on_next_open(&dir.root, durable).unwrap();
    let reopened = Keratin::open(&dir.root, config()).await.unwrap();
    assert_eq!(reopened.next_offset(), 1);
    assert_eq!(reopened.reader().scan_from(0, 10).unwrap().len(), 1);
    reopened.shutdown().await.unwrap();
}
