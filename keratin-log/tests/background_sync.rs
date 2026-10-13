use keratin_log::*;
use std::time::{Duration, Instant};

fn message(value: u8) -> Message {
    Message {
        flags: 0,
        headers: vec![],
        payload: vec![value; 128],
    }
}

fn config(interval: Duration) -> KeratinConfig {
    KeratinConfig {
        background_sync_interval: interval,
        ..KeratinConfig::test_default()
    }
}

/// Read in the order the API documents: durable, then written, then next.
fn frontiers(log: &Keratin) -> (u64, u64, u64) {
    let durable = log.durable_next();
    let written = log.written_next();
    let next = log.next_offset();
    assert!(
        durable <= written && written <= next,
        "frontiers out of order: durable {durable} written {written} next {next}"
    );
    (durable, written, next)
}

async fn wait_until(what: &str, mut check: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !check() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

#[tokio::test]
async fn frontiers_follow_each_durability() {
    let dir = test_dir!("frontier_mix");
    let log = Keratin::open(&dir.root, config(Duration::ZERO))
        .await
        .unwrap();
    let mut watch = log.watch_written_next();
    assert_eq!(*watch.borrow_and_update(), 0);
    assert_eq!(frontiers(&log), (0, 0, 0));

    // A small AfterWrite append can complete while still staged.
    log.append(message(0), Some(KDurability::AfterWrite))
        .await
        .unwrap();
    assert_eq!(frontiers(&log), (0, 0, 1));
    assert!(!watch.has_changed().unwrap());

    // AfterFlush writes the whole staged group, including the AfterWrite
    // record ahead of it, without making anything durable.
    log.append(message(1), Some(KDurability::AfterFlush))
        .await
        .unwrap();
    assert_eq!(frontiers(&log), (0, 2, 2));
    assert!(watch.has_changed().unwrap());
    assert_eq!(*watch.borrow_and_update(), 2);
    assert!(log.reader().scan_from(0, 2).unwrap().is_empty());

    // AfterFsync covers everything written or staged before it.
    log.append(message(2), Some(KDurability::AfterWrite))
        .await
        .unwrap();
    log.append(message(3), Some(KDurability::AfterFsync))
        .await
        .unwrap();
    assert_eq!(frontiers(&log), (4, 4, 4));
    assert_eq!(*watch.borrow_and_update(), 4);
    assert_eq!(log.reader().scan_from(0, 10).unwrap().len(), 4);

    log.append(message(4), Some(KDurability::AfterFlush))
        .await
        .unwrap();
    assert_eq!(frontiers(&log), (4, 5, 5));
    log.sync().await.unwrap();
    assert_eq!(frontiers(&log), (5, 5, 5));
    let status = log.sync_status();
    assert_eq!(status.durable_lag_records, 0);
    assert_eq!(status.background_syncs_started, 0);
    assert_eq!(status.background_sync_interval, Duration::ZERO);
    log.shutdown().await.unwrap();
    drop(log);

    // Reopen starts with every recovered record written and durable.
    let log = Keratin::open(&dir.root, config(Duration::ZERO))
        .await
        .unwrap();
    assert_eq!(frontiers(&log), (5, 5, 5));
    assert_eq!(*log.watch_written_next().borrow(), 5);
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn replicated_appends_publish_the_written_frontier() {
    let dir = test_dir!("frontier_replicated");
    let log = Keratin::open(&dir.root, config(Duration::ZERO))
        .await
        .unwrap();
    log.become_follower();
    let epoch = log.current_epoch();
    // Replicated AfterWrite and AfterFlush appends reach the OS before they
    // reply. Only AfterFsync makes them durable.
    log.append_replicated_batch(epoch, 0, vec![message(0)], Some(KDurability::AfterWrite))
        .await
        .unwrap();
    assert_eq!(frontiers(&log), (0, 1, 1));
    log.append_replicated_batch(epoch, 1, vec![message(1)], Some(KDurability::AfterFlush))
        .await
        .unwrap();
    assert_eq!(frontiers(&log), (0, 2, 2));
    log.append_replicated_batch(epoch, 2, vec![message(2)], Some(KDurability::AfterFsync))
        .await
        .unwrap();
    assert_eq!(frontiers(&log), (3, 3, 3));
    log.shutdown().await.unwrap();
}

// Readers load the frontiers in the documented order while a mix of appends
// runs, with the background sync moving durability on its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frontiers_stay_ordered_under_mixed_load() {
    let dir = test_dir!("frontier_order");
    let log = std::sync::Arc::new(
        Keratin::open(&dir.root, config(Duration::from_millis(2)))
            .await
            .unwrap(),
    );
    // Stops the checker even when an assertion below fails, so a failure
    // does not hang the runtime shutdown.
    struct StopOnDrop(std::sync::Arc<std::sync::atomic::AtomicBool>);
    impl Drop for StopOnDrop {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stopper = StopOnDrop(stop.clone());
    let checker = {
        let log = log.clone();
        let stop = stop.clone();
        tokio::task::spawn_blocking(move || {
            let mut checks = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                frontiers(&log);
                checks += 1;
            }
            checks
        })
    };
    let mut receipts = Vec::new();
    for i in 0..600u32 {
        let durability = match i % 3 {
            0 => KDurability::AfterWrite,
            1 => KDurability::AfterFlush,
            _ => KDurability::AfterFsync,
        };
        receipts.push(
            log.append_enqueue_receiver(message(i as u8), Some(durability))
                .unwrap(),
        );
    }
    for receipt in receipts {
        receipt.await.unwrap().unwrap();
    }
    log.append(message(0), Some(KDurability::AfterFlush))
        .await
        .unwrap();
    wait_until("durable reaches written", || {
        log.durable_next() == log.written_next()
    })
    .await;
    drop(stopper);
    assert!(checker.await.unwrap() > 0);
    assert_eq!(frontiers(&log), (601, 601, 601));
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn background_sync_makes_written_records_durable_after_an_interval() {
    let dir = test_dir!("background_sync_interval");
    let interval = Duration::from_millis(500);
    let opened = Instant::now();
    let log = Keratin::open(&dir.root, config(interval)).await.unwrap();
    assert_eq!(log.sync_status().background_sync_interval, interval);
    log.append(message(1), Some(KDurability::AfterFlush))
        .await
        .unwrap();
    assert_eq!(log.written_next(), 1);
    if opened.elapsed() < interval {
        // Open counts as the last sync, so the first tick is one interval
        // after it.
        assert_eq!(log.durable_next(), 0);
        assert_eq!(log.sync_status().durable_lag_records, 1);
    }
    wait_until("background sync", || log.durable_next() == 1).await;
    assert!(opened.elapsed() >= interval, "synced before the interval");
    let status = log.sync_status();
    assert_eq!(status.durable_lag_records, 0);
    assert_eq!(status.background_syncs_started, 1);
    assert!(status.since_last_sync < Duration::from_secs(5));
    assert!(status.storage_failure.is_none());
    // Durable reads see the record now.
    assert_eq!(log.reader().scan_from(0, 1).unwrap().len(), 1);
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn idle_and_synced_logs_never_background_sync() {
    let dir = test_dir!("background_sync_idle");
    let log = Keratin::open(&dir.root, config(Duration::from_millis(10)))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(log.sync_status().background_syncs_started, 0);

    // AfterFsync appends leave nothing unsynced.
    for value in 0..5 {
        log.append(message(value), Some(KDurability::AfterFsync))
            .await
            .unwrap();
    }
    // AfterWrite appends small enough to stay staged are not written yet.
    log.append(message(5), Some(KDurability::AfterWrite))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(log.sync_status().background_syncs_started, 0);
    assert_eq!(frontiers(&log), (5, 5, 6));

    // A written record gets one sync, which also covers the staged one, and
    // then the log is idle again.
    log.append(message(6), Some(KDurability::AfterFlush))
        .await
        .unwrap();
    wait_until("background sync", || log.durable_next() == 7).await;
    let started = log.sync_status().background_syncs_started;
    assert_eq!(started, 1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(log.sync_status().background_syncs_started, started);
    assert!(!log.sync_status().background_sync_pending);
    log.shutdown().await.unwrap();
}

// Commits here would wait a minute, so the AfterFsync append completes only
// because the background sync covers it.
#[tokio::test]
async fn background_sync_completes_a_waiting_after_fsync_append() {
    for interval in [Duration::from_millis(20), Duration::ZERO] {
        let dir = test_dir!("background_sync_merge");
        let log = Keratin::open(
            &dir.root,
            KeratinConfig {
                fsync_interval_ms: 60_000,
                min_fsync_interval_ms: 60_000,
                ..config(interval)
            },
        )
        .await
        .unwrap();
        let mut waiting = log
            .append_enqueue_receiver(message(0), Some(KDurability::AfterFsync))
            .unwrap();
        // Writes the waiting record to the OS along with this one.
        log.append(message(1), Some(KDurability::AfterFlush))
            .await
            .unwrap();
        if interval.is_zero() {
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert!(waiting.try_recv().is_err(), "nothing else syncs this log");
            assert_eq!(frontiers(&log), (0, 2, 2));
            // Shutdown commits it.
            log.shutdown().await.unwrap();
            assert_eq!(waiting.await.unwrap().unwrap().base_offset, 0);
            continue;
        }
        let result = tokio::time::timeout(Duration::from_secs(10), waiting)
            .await
            .expect("the background sync must complete the waiting append")
            .unwrap()
            .unwrap();
        assert_eq!(result.base_offset, 0);
        assert_eq!(frontiers(&log), (2, 2, 2));
        assert!(log.sync_status().background_syncs_started >= 1);
        log.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn background_sync_interval_changes_live() {
    let dir = test_dir!("background_sync_live");
    let log = Keratin::open(&dir.root, config(Duration::ZERO))
        .await
        .unwrap();
    log.append(message(0), Some(KDurability::AfterFlush))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(frontiers(&log), (0, 1, 1));
    assert_eq!(log.sync_status().background_syncs_started, 0);

    let applied = log
        .update_writer_settings(|s| s.background_sync_interval = Duration::from_millis(10))
        .await
        .unwrap();
    assert_eq!(applied.revision, 1);
    assert_eq!(
        applied.settings.background_sync_interval,
        Duration::from_millis(10)
    );
    assert_eq!(
        log.sync_status().background_sync_interval,
        Duration::from_millis(10)
    );
    // The settings barrier syncs what was written before the change.
    assert_eq!(frontiers(&log), (1, 1, 1));
    log.append(message(1), Some(KDurability::AfterFlush))
        .await
        .unwrap();
    wait_until("enabled background sync", || log.durable_next() == 2).await;
    assert_eq!(log.sync_status().background_syncs_started, 1);

    // Zero turns it off again.
    log.update_writer_settings(|s| s.background_sync_interval = Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(log.sync_status().background_sync_interval, Duration::ZERO);
    log.append(message(2), Some(KDurability::AfterFlush))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(frontiers(&log), (2, 3, 3));
    assert_eq!(log.sync_status().background_syncs_started, 1);

    // And back on.
    log.update_writer_settings(|s| s.background_sync_interval = Duration::from_millis(10))
        .await
        .unwrap();
    log.append(message(3), Some(KDurability::AfterFlush))
        .await
        .unwrap();
    wait_until("re-enabled background sync", || log.durable_next() == 4).await;
    assert_eq!(log.sync_status().background_syncs_started, 2);

    // An interval past the clock range is refused and changes nothing.
    let refused = log
        .update_writer_settings(|s| s.background_sync_interval = Duration::MAX)
        .await
        .unwrap_err();
    assert_eq!(refused.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(log.writer_settings().await.unwrap().revision, 3);
    log.shutdown().await.unwrap();
    drop(log);

    // Live settings reset on reopen.
    let log = Keratin::open(&dir.root, config(Duration::from_millis(250)))
        .await
        .unwrap();
    assert_eq!(
        log.writer_settings()
            .await
            .unwrap()
            .settings
            .background_sync_interval,
        Duration::from_millis(250)
    );
    log.shutdown().await.unwrap();
}

// Rollover syncs the outgoing segment inline. The background sync still
// brings the published durable frontier up to the written one.
#[tokio::test]
async fn background_sync_covers_records_across_rollover() {
    let dir = test_dir!("background_sync_rollover");
    let cfg = KeratinConfig {
        segment_max_bytes: 1024,
        ..config(Duration::from_millis(10))
    };
    let log = Keratin::open(&dir.root, cfg).await.unwrap();
    for value in 0..40 {
        log.append(message(value), Some(KDurability::AfterFlush))
            .await
            .unwrap();
        frontiers(&log);
    }
    assert!(log.segment_infos().unwrap().len() > 1);
    wait_until("durable reaches written", || log.durable_next() == 40).await;
    assert_eq!(log.reader().scan_from(0, 64).unwrap().len(), 40);
    // Retention still trims only whole sealed segments below the request.
    let head = log.truncate_before(20).await.unwrap();
    assert!(head <= 20);
    assert_eq!(frontiers(&log), (40, 40, 40));
    log.shutdown().await.unwrap();
}

#[cfg(feature = "io-fault-injection")]
mod faults {
    use super::*;
    use keratin_log::io_fault_injection::{Fault, arm, arm_gated};

    fn active(root: &std::path::Path) -> std::path::PathBuf {
        root.join("segments/00000000000000000000.log")
    }

    // While the only background sync is held, later written records stay
    // non-durable and no further sync starts, however many ticks pass.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn held_background_sync_is_never_doubled_or_overcounted() {
        let dir = test_dir!("background_sync_held");
        let log = Keratin::open(&dir.root, config(Duration::from_millis(10)))
            .await
            .unwrap();
        let gate = arm_gated(&active(&dir.root), Fault::FsyncPause).unwrap();
        log.append(message(0), Some(KDurability::AfterFlush))
            .await
            .unwrap();
        wait_until("held sync", || gate.hits() == 1).await;
        assert!(log.sync_status().background_sync_pending);
        for value in 1..3 {
            log.append(message(value), Some(KDurability::AfterFlush))
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        let status = log.sync_status();
        assert_eq!(status.background_syncs_started, 1);
        assert_eq!((status.durable_next, status.written_next), (0, 3));
        gate.release();
        wait_until("catch up", || log.durable_next() == 3).await;
        // One sync for the held range and one for the rest, not one per
        // missed tick.
        assert_eq!(log.sync_status().background_syncs_started, 2);
        drop(gate);
        log.shutdown().await.unwrap();
    }

    // A failed background sync stops the log like any failed sync, and the
    // next open cuts back to the last successful one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_background_sync_latches_and_reopen_cuts_back() {
        for fault in [Fault::Fsync, Fault::FsyncAfterSuccess] {
            let dir = test_dir!("background_sync_failure");
            let log = Keratin::open(&dir.root, config(Duration::from_millis(10)))
                .await
                .unwrap();
            log.append(message(0), Some(KDurability::AfterFsync))
                .await
                .unwrap();
            let armed = arm(&active(&dir.root), fault).unwrap();
            for value in 1..3 {
                log.append(message(value), Some(KDurability::AfterFlush))
                    .await
                    .unwrap();
            }
            wait_until("latched background sync", || {
                log.sync_status().storage_failure.is_some()
            })
            .await;
            assert_eq!(armed.hits(), 1);
            drop(armed);
            let status = log.sync_status();
            assert_eq!(status.durable_next, 1, "{fault:?}");
            assert!(!status.background_sync_pending);
            assert!(
                log.append(message(3), Some(KDurability::AfterFlush))
                    .await
                    .is_err()
            );
            assert!(log.sync().await.is_err());
            log.shutdown().await.unwrap();
            drop(log);
            assert!(dir.root.join("latched-cut.pending").exists(), "{fault:?}");

            let reopened = Keratin::open(&dir.root, config(Duration::from_millis(10)))
                .await
                .unwrap();
            assert!(!dir.root.join("latched-cut.pending").exists());
            let kept = reopened.reader().scan_from(0, 10).unwrap();
            assert_eq!(kept.len(), 1, "{fault:?}: only the synced record survives");
            assert_eq!(frontiers(&reopened), (1, 1, 1));
            let next = reopened
                .append(message(4), Some(KDurability::AfterFsync))
                .await
                .unwrap();
            assert_eq!(next.base_offset, 1, "{fault:?}");
            reopened.shutdown().await.unwrap();
        }
    }
}
