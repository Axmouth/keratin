use keratin_log::*;
use std::time::Duration;

fn message(value: u8) -> Message {
    Message {
        flags: 0,
        headers: vec![],
        payload: vec![value; 128],
    }
}
fn config() -> KeratinConfig {
    KeratinConfig {
        tail_cache_bytes: 1024 * 1024,
        default_durability: KDurability::AfterFlush,
        ..KeratinConfig::test_default()
    }
}
fn active(root: &std::path::Path) -> std::path::PathBuf {
    root.join("segments/00000000000000000000.log")
}

#[tokio::test]
async fn small_idle_flush_writes_without_publishing_durability() {
    for cache in [0, 1024 * 1024] {
        let dir = test_dir!("after_flush_idle");
        let log = Keratin::open(
            &dir.root,
            KeratinConfig {
                tail_cache_bytes: cache,
                ..config()
            },
        )
        .await
        .unwrap();
        let before = std::fs::metadata(active(&dir.root)).unwrap().len();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            log.append_batch(vec![message(37)], None),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result.base_offset, 0);
        assert!(std::fs::metadata(active(&dir.root)).unwrap().len() > before);
        assert!(
            std::fs::read(active(&dir.root))
                .unwrap()
                .windows(128)
                .any(|w| w == [37; 128])
        );
        assert_eq!(
            log.reader().fetch(0).unwrap().unwrap().payload,
            vec![37; 128]
        );
        assert_eq!(log.durable_offset(), 0);
        assert!(log.reader().scan_from(0, 1).unwrap().is_empty());
        #[cfg(feature = "early-replication-experiment")]
        if cache > 0 {
            let records = log.reader().scan_for_replication(0, 1).unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].payload, vec![37; 128]);
        }
        log.sync().await.unwrap();
        assert_eq!(
            log.reader().scan_from(0, 1).unwrap()[0].payload,
            vec![37; 128]
        );
        log.shutdown().await.unwrap();
        drop(log);
        let reopened = Keratin::open(&dir.root, config()).await.unwrap();
        assert_eq!(
            reopened.reader().scan_from(0, 1).unwrap()[0].payload,
            vec![37; 128]
        );
        reopened.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn cancelled_flush_waiter_does_not_cancel_other_appends() {
    let dir = test_dir!("after_flush_cancel");
    let log = Keratin::open(&dir.root, config()).await.unwrap();
    drop(
        log.append_batch_enqueue_receiver(vec![message(1)], None)
            .unwrap(),
    );
    let result = log.append_batch(vec![message(2)], None).await.unwrap();
    assert_eq!(result.base_offset, 1);
    assert_eq!(log.durable_offset(), 0);
    log.sync().await.unwrap();
    let records = log.reader().scan_from(0, 2).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].payload, vec![1; 128]);
    assert_eq!(records[1].payload, vec![2; 128]);
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn follower_flush_and_duplicate_do_not_advance_durable_frontier() {
    let dir = test_dir!("after_flush_follower");
    let log = Keratin::open(&dir.root, config()).await.unwrap();
    log.become_follower();
    let epoch = log.current_epoch();
    for _ in 0..2 {
        log.append_replicated_batch(epoch, 0, vec![message(19)], Some(KDurability::AfterFlush))
            .await
            .unwrap();
        assert_eq!(log.durable_offset(), 0);
        assert!(log.reader().scan_from(0, 1).unwrap().is_empty());
    }
    log.sync().await.unwrap();
    let records = log.reader().scan_from(0, 5).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload, vec![19; 128]);
    log.shutdown().await.unwrap();
}

#[cfg(feature = "io-fault-injection")]
#[tokio::test]
async fn flush_completion_does_not_wait_for_mixed_batch_fsync() {
    use keratin_log::io_fault_injection::{Fault, arm_gated};
    let dir = test_dir!("after_flush_mixed");
    let log = Keratin::open(&dir.root, config()).await.unwrap();
    let gate = arm_gated(&active(&dir.root), Fault::FsyncPause).unwrap();
    let mut durable = log
        .append_batch_enqueue_receiver(vec![message(1)], Some(KDurability::AfterFsync))
        .unwrap();
    let flushed = log
        .append_batch_enqueue_receiver(vec![message(2)], Some(KDurability::AfterFlush))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), flushed)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while gate.hits() == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        durable.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(log.durable_offset(), 0);
    gate.release();
    durable.await.unwrap().unwrap();
    drop(gate);
    log.sync().await.unwrap();
    assert_eq!(log.reader().scan_from(0, 5).unwrap().len(), 2);
    log.shutdown().await.unwrap();
}

#[cfg(feature = "io-fault-injection")]
#[tokio::test]
async fn flush_failure_fails_waiters_and_latches_the_log() {
    use keratin_log::io_fault_injection::{Fault, arm};
    for fault in [Fault::Write, Fault::PartialWrite] {
        let dir = test_dir!("after_flush_failure");
        let log = Keratin::open(&dir.root, config()).await.unwrap();
        let armed = arm(&active(&dir.root), fault).unwrap();
        let a = log
            .append_batch_enqueue_receiver(vec![message(1)], None)
            .unwrap();
        let b = log
            .append_batch_enqueue_receiver(vec![message(2)], Some(KDurability::AfterFsync))
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(2), a)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        assert!(!matches!(
            tokio::time::timeout(Duration::from_secs(2), b)
                .await
                .unwrap(),
            Ok(Ok(_))
        ));
        assert_eq!(armed.hits(), 1);
        assert_eq!(log.durable_offset(), 0);
        assert!(log.append_batch(vec![message(3)], None).await.is_err());
        assert!(log.sync().await.is_err());
        drop(armed);
        log.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn conflicting_unsynced_prefix_is_rejected() {
    let dir = test_dir!("after_flush_conflict");
    let log = Keratin::open(&dir.root, config()).await.unwrap();
    log.become_follower();
    let epoch = log.current_epoch();
    log.append_replicated_batch(epoch, 0, vec![message(1)], Some(KDurability::AfterFlush))
        .await
        .unwrap();
    assert!(
        log.append_replicated_batch(epoch, 0, vec![message(2)], Some(KDurability::AfterFlush))
            .await
            .is_err()
    );
    assert_eq!(log.durable_offset(), 0);
    log.sync().await.unwrap();
    assert_eq!(
        log.reader().scan_from(0, 1).unwrap()[0].payload,
        vec![1; 128]
    );
    log.shutdown().await.unwrap();
}

#[cfg(feature = "io-fault-injection")]
#[tokio::test]
async fn follower_partial_flush_failure_latches_the_log() {
    use keratin_log::io_fault_injection::{Fault, arm};
    let dir = test_dir!("after_flush_follower_failure");
    let log = Keratin::open(&dir.root, config()).await.unwrap();
    log.become_follower();
    let armed = arm(&active(&dir.root), Fault::PartialWrite).unwrap();
    let epoch = log.current_epoch();
    assert!(
        log.append_replicated_batch(epoch, 0, vec![message(1)], Some(KDurability::AfterFlush))
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(
            Duration::from_secs(2),
            log.append_replicated_batch(epoch, 0, vec![message(1)], Some(KDurability::AfterFlush))
        )
        .await
        .unwrap()
        .is_err()
    );
    assert_eq!(log.durable_offset(), 0);
    drop(armed);
    log.shutdown().await.unwrap();
}
