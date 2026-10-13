#![cfg(feature = "io-fault-injection")]
//! A dirty reopen must sync the recovered tail before it publishes the durable
//! frontier. Records written but not synced before a crash live only in the
//! page cache until then.
use keratin_log::io_fault_injection::{Fault, arm, arm_gated};
use keratin_log::*;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const CHILD_ROOT: &str = "KERATIN_DURABLE_REOPEN_CHILD_ROOT";

fn messages(range: std::ops::Range<u8>) -> Vec<Message> {
    range
        .map(|value| Message {
            payload: vec![value; 64],
            flags: 0,
            headers: vec![],
        })
        .collect()
}

fn segment(root: &Path) -> PathBuf {
    root.join("segments/00000000000000000000.log")
}

fn index(root: &Path) -> PathBuf {
    root.join("segments/00000000000000000000.idx")
}

async fn open(root: &Path, preserve_history: bool) -> std::io::Result<Keratin> {
    if preserve_history {
        Keratin::open_preserving_history(root, KeratinConfig::test_default()).await
    } else {
        Keratin::open(root, KeratinConfig::test_default()).await
    }
}

// Child half of `crash_with_unsynced_tail`. Leaves one synced record and three
// written but unsynced ones, then exits without a clean shutdown.
#[tokio::test]
async fn crash_child_writes_unsynced_records() {
    let Some(root) = std::env::var_os(CHILD_ROOT) else {
        return;
    };
    // The parent checks that a dirty reopen syncs these records itself.
    let cfg = KeratinConfig {
        background_sync_interval: std::time::Duration::ZERO,
        ..KeratinConfig::test_default()
    };
    let log = Keratin::open(&root, cfg).await.unwrap();
    log.become_follower();
    let epoch = log.current_epoch();
    log.append_replicated_batch(epoch, 0, messages(0..1), Some(KDurability::AfterFsync))
        .await
        .unwrap();
    // Replicated AfterWrite appends are written to the OS before they reply.
    log.append_replicated_batch(epoch, 1, messages(1..4), Some(KDurability::AfterWrite))
        .await
        .unwrap();
    assert_eq!(log.durable_next(), 1);
    std::process::exit(0);
}

fn crash_with_unsynced_tail(root: &Path) {
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_child_writes_unsynced_records", "--nocapture"])
        .env(CHILD_ROOT, root)
        .status()
        .unwrap();
    assert!(status.success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dirty_reopen_publishes_frontier_only_after_sync() {
    for preserve_history in [false, true] {
        let dir = test_dir!("dirty_reopen_syncs_tail");
        crash_with_unsynced_tail(&dir.root);

        let gate = arm_gated(&segment(&dir.root), Fault::FsyncPause).unwrap();
        let root = dir.root.clone();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let opener = std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async {
                let log = open(&root, preserve_history).await.unwrap();
                done_tx.send(log.durable_next()).unwrap();
                log.shutdown().await.unwrap();
            });
        });

        let deadline = Instant::now() + Duration::from_secs(5);
        while gate.hits() == 0 {
            assert!(
                done_rx.try_recv().is_err(),
                "preserve_history={preserve_history}: frontier published before any sync"
            );
            assert!(
                Instant::now() < deadline,
                "preserve_history={preserve_history}: open never synced the recovered tail"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            done_rx.try_recv().is_err(),
            "open must not finish while its tail sync is held"
        );
        gate.release();
        let durable = done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(durable, 4, "preserve_history={preserve_history}");
        opener.join().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_reopen_sync_fails_the_open() {
    for preserve_history in [false, true] {
        for target in [segment, index] {
            let dir = test_dir!("dirty_reopen_sync_fails");
            crash_with_unsynced_tail(&dir.root);

            let armed = arm(&target(&dir.root), Fault::Fsync).unwrap();
            let Err(error) = open(&dir.root, preserve_history).await else {
                panic!("open must fail when its tail sync fails");
            };
            assert!(error.to_string().contains("injected"), "{error}");
            assert_eq!(armed.hits(), 1);
            drop(armed);

            // The failed open removed nothing, so a later open recovers it all.
            let log = open(&dir.root, preserve_history).await.unwrap();
            assert_eq!(log.durable_next(), 4);
            assert_eq!(log.reader().scan_from(0, 10).unwrap().len(), 4);
            log.shutdown().await.unwrap();
        }
    }
}

// A clean shutdown synced everything before marking the manifest clean, so a
// clean reopen trusts it without another sync.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clean_reopen_needs_no_sync() {
    let dir = test_dir!("clean_reopen_no_sync");
    let log = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    log.append_batch(messages(0..3), Some(KDurability::AfterWrite))
        .await
        .unwrap();
    log.shutdown().await.unwrap();
    drop(log);

    let armed = arm(&segment(&dir.root), Fault::Fsync).unwrap();
    let log = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    assert_eq!(armed.hits(), 0);
    assert_eq!(log.durable_next(), 3);
    drop(armed);
    log.shutdown().await.unwrap();
}
