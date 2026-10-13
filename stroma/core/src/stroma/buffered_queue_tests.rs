use super::*;
use std::time::Duration;

// No background sync, so buffered records stay unsynced until something
// syncs them on purpose.
async fn open(root: &Path, tail_cache_bytes: usize) -> Stroma {
    let cfg = KeratinConfig {
        background_sync_interval: Duration::ZERO,
        tail_cache_bytes,
        ..KeratinConfig::test_default()
    };
    Stroma::open_with_options(
        root,
        StromaKeratinConfig::from_message_log(cfg),
        SnapshotConfig {
            every_events: u64::MAX,
        },
        StromaOptions::default(),
    )
    .await
    .unwrap()
}

fn headers() -> MessageHeaders {
    MessageHeaders {
        published: 0,
        publish_received: 0,
        content_type: None,
        extra: Default::default(),
    }
}

async fn publish(st: &Stroma, tp: &str, values: std::ops::Range<u8>) -> Vec<u64> {
    let mut items = Vec::new();
    let mut receipts = Vec::new();
    for value in values {
        let (completion, rx) = KeratinAppendCompletion::pair();
        items.push(PublishItem {
            headers: headers(),
            payload: vec![value; 512],
            completion,
            not_before: None,
            expire_at: None,
        });
        receipts.push(rx);
    }
    st.append_message_batch(tp, 0, None, items).await.unwrap();
    let mut offsets = Vec::new();
    for rx in receipts {
        offsets.push(rx.await.unwrap().unwrap().base_offset);
    }
    offsets
}

async fn settle(st: &Stroma, off: u64, how: u8) {
    let (completion, rx) = KeratinAppendCompletion::pair();
    match how {
        0 => st.ack_enqueue("q", 0, None, off, completion).await.unwrap(),
        1 => st
            .release_inflight_many("q", 0, None, vec![AckEventMeta { off }], completion)
            .await
            .unwrap(),
        _ => st
            .nack_enqueue("q", 0, None, off, how == 2, completion)
            .await
            .unwrap(),
    }
    rx.await.unwrap().unwrap();
}

async fn event_log(st: &Stroma, tp: &str) -> Arc<Keratin> {
    st.queue_handle(tp, 0, None)
        .await
        .unwrap()
        .resolve()
        .unwrap()
        .event_log()
}

async fn ready(st: &Stroma, tp: &str) -> Vec<(u64, Vec<u8>)> {
    st.poll_ready(tp, 0, None, 100, u64::MAX, u64::MAX)
        .await
        .unwrap()
        .into_iter()
        .map(|(off, _, payload, _)| (off, payload))
        .collect()
}

// A buffered queue confirms, delivers and settles without any fsync, from the
// tail cache or from the file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_queue_confirms_delivers_and_settles_before_sync() {
    for cache in [64 * 1024 * 1024, 0] {
        let dir = keratin_log::test_dir!("buffered_queue_unsynced");
        let st = open(&dir.root, cache).await;
        st.ensure_queue("q", 0, None).await.unwrap();
        st.set_queue_buffered("q", 0, None, true).await.unwrap();
        let log = event_log(&st, "q").await;
        let synced = log.durable_next();
        assert!(!log.sync_status().every_write_sync_expected);

        let offsets = publish(&st, "q", 0..8).await;
        assert_eq!(log.durable_next(), synced, "cache {cache}: a publish synced");
        let delivered = ready(&st, "q").await;
        assert_eq!(delivered.len(), 8);
        for ((off, payload), (expected, value)) in delivered.iter().zip(offsets.iter().zip(0u8..)) {
            assert_eq!(off, expected);
            assert_eq!(payload, &vec![value; 512]);
        }

        // Ack, release, requeue and a terminal nack, which discards without a
        // dead-letter target.
        for (i, off) in offsets.iter().enumerate() {
            settle(&st, *off, (i % 4) as u8).await;
        }
        assert_eq!(log.durable_next(), synced, "cache {cache}: a settle synced");
        assert!(log.written_next() > synced);
        let again: Vec<u64> = ready(&st, "q").await.into_iter().map(|(off, _)| off).collect();
        let expected: Vec<u64> = offsets
            .iter()
            .enumerate()
            .filter(|(i, _)| matches!(i % 4, 1 | 2))
            .map(|(_, off)| *off)
            .collect();
        assert_eq!(again, expected, "cache {cache}");
        assert_eq!(log.sync_status().unexpected_background_syncs, 0);
        st.shutdown().await.unwrap();
    }
}

// A durable queue next to a buffered one keeps syncing every write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_the_declared_queue_is_buffered() {
    let dir = keratin_log::test_dir!("buffered_queue_scope");
    let st = open(&dir.root, 0).await;
    st.ensure_queue("q", 0, None).await.unwrap();
    st.ensure_queue("d", 0, None).await.unwrap();
    st.set_queue_buffered("q", 0, None, true).await.unwrap();
    publish(&st, "d", 0..3).await;
    let log = event_log(&st, "d").await;
    assert_eq!(log.durable_next(), log.next_offset());
    assert!(log.sync_status().every_write_sync_expected);
    st.shutdown().await.unwrap();
}

// Turning the mode off syncs what was written, and later writes sync again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turning_buffered_off_syncs_first() {
    let dir = keratin_log::test_dir!("buffered_queue_off");
    let st = open(&dir.root, 0).await;
    st.ensure_queue("q", 0, None).await.unwrap();
    st.set_queue_buffered("q", 0, None, true).await.unwrap();
    publish(&st, "q", 0..4).await;
    let log = event_log(&st, "q").await;
    assert!(log.durable_next() < log.next_offset());

    st.set_queue_buffered("q", 0, None, false).await.unwrap();
    assert_eq!(log.durable_next(), log.next_offset());
    assert!(log.sync_status().every_write_sync_expected);
    assert_eq!(ready(&st, "q").await.len(), 4);
    publish(&st, "q", 4..6).await;
    assert_eq!(log.durable_next(), log.next_offset());
    assert_eq!(ready(&st, "q").await.len(), 2);
    st.shutdown().await.unwrap();
}

// The declaration covers a log opened after it, and is refused for streams.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declaration_survives_eviction_and_refuses_streams() {
    let dir = keratin_log::test_dir!("buffered_queue_evict");
    let st = open(&dir.root, 0).await;
    st.ensure_queue("q", 0, None).await.unwrap();
    st.set_queue_buffered("q", 0, None, true).await.unwrap();
    st.evict("q", 0, None).await.unwrap();
    publish(&st, "q", 0..2).await;
    let log = event_log(&st, "q").await;
    assert!(log.durable_next() < log.next_offset());
    assert!(!log.sync_status().every_write_sync_expected);
    assert_eq!(ready(&st, "q").await.len(), 2);

    st.create_stream("s", 0, None).await.unwrap();
    assert!(matches!(
        st.set_queue_buffered("s", 0, None, true).await,
        Err(StromaError::WrongPartitionKind { .. })
    ));
    st.shutdown().await.unwrap();
}

// A checkpoint is recovery authority, so it syncs the records it covers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_syncs_the_records_it_covers() {
    let dir = keratin_log::test_dir!("buffered_queue_checkpoint");
    let st = open(&dir.root, 0).await;
    st.ensure_queue("q", 0, None).await.unwrap();
    st.set_queue_buffered("q", 0, None, true).await.unwrap();
    publish(&st, "q", 0..4).await;
    let log = event_log(&st, "q").await;
    assert!(log.durable_next() < log.next_offset());
    st.snapshot_partition("q", 0, None).await.unwrap();
    assert_eq!(log.durable_next(), log.next_offset());
    st.shutdown().await.unwrap();
}

// A dead-letter copy reads the source payload before it is synced.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dead_letter_copy_reads_unsynced_payloads() {
    let dir = keratin_log::test_dir!("buffered_queue_dlq");
    let st = open(&dir.root, 0).await;
    st.ensure_queue("dlq", 0, None).await.unwrap();
    st.declare(
        "q",
        0,
        None,
        DeclareMeta {
            dlq_policy: Some(crate::event::DLQDiscardPolicyWire::CustomDQL {
                tp: "dlq".into(),
                part: 0,
                group: None,
            }),
            dlq_max_retries: None,
            default_message_ttl_ms: None,
        },
    )
    .await
    .unwrap();
    st.set_queue_buffered("q", 0, None, true).await.unwrap();
    let offsets = publish(&st, "q", 7..8).await;
    assert_eq!(ready(&st, "q").await.len(), 1);
    assert!(event_log(&st, "q").await.durable_next() <= offsets[0]);
    settle(&st, offsets[0], 3).await;

    let copied = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let got = ready(&st, "dlq").await;
            if !got.is_empty() {
                return got;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("dead-letter copy");
    assert_eq!(copied.len(), 1);
    assert_eq!(copied[0].1, vec![7; 512]);
    st.shutdown().await.unwrap();
}

// A follower that wrote replicated records without syncing them reports its
// resume point only after syncing, so the answer is durable progress.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_resume_syncs_written_records() {
    let owner_dir = keratin_log::test_dir!("buffered_queue_owner");
    let follower_dir = keratin_log::test_dir!("buffered_queue_follower");
    let owner = open(&owner_dir.root, 0).await;
    let follower = open(&follower_dir.root, 0).await;
    owner.ensure_queue_owner_epoch("q", 0, None, Some(3)).await.unwrap();
    publish(&owner, "q", 0..3).await;
    let OwnerReplicationRead::Batch(batch) = owner
        .read_owner_queue_records("q", 0, None, 0, 10, usize::MAX)
        .await
        .unwrap()
    else {
        panic!("queue log retained")
    };
    follower
        .become_queue_follower_with_epoch("q", 0, None, 3)
        .await
        .unwrap();
    let events = batch.records.into_iter().map(|(_, e)| e).collect();
    follower
        .apply_replicated_queue_batch(
            "q",
            0,
            None,
            ReplicatedEventBatch {
                epoch: 3,
                first_offset: 0,
                events,
                durability: Some(KDurability::AfterFlush),
            },
        )
        .await
        .unwrap();
    let log = event_log(&follower, "q").await;
    assert_eq!(log.next_offset(), 3);
    assert!(log.durable_next() < 3);
    assert_eq!(
        follower
            .queue_replication_next_offset("q", 0, None)
            .await
            .unwrap(),
        3
    );
    assert_eq!(log.durable_next(), 3);
    owner.shutdown().await.unwrap();
    follower.shutdown().await.unwrap();
}

// The declaration is stored with the partition, so it survives a restart,
// and goes away with the partition.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn declaration_survives_restart_and_ends_with_the_partition() {
    let dir = keratin_log::test_dir!("buffered_queue_restart");
    let st = open(&dir.root, 0).await;
    st.ensure_queue("q", 0, None).await.unwrap();
    st.set_queue_buffered("q", 0, None, true).await.unwrap();
    publish(&st, "q", 0..2).await;
    st.shutdown().await.unwrap();
    drop(st);

    let st = open(&dir.root, 0).await;
    let delivered = ready(&st, "q").await;
    assert_eq!(delivered.len(), 2);
    assert!(st.queue_is_buffered("q", 0, None));
    for (off, _) in delivered {
        settle(&st, off, 0).await;
    }
    publish(&st, "q", 2..4).await;
    let log = event_log(&st, "q").await;
    assert!(log.durable_next() < log.next_offset());
    drop(log);

    assert!(matches!(
        st.destroy_partition("q", 0, None).await.unwrap(),
        DestroyOutcome::Destroyed
    ));
    assert!(!st.queue_is_buffered("q", 0, None));
    st.ensure_queue("q", 0, None).await.unwrap();
    publish(&st, "q", 0..1).await;
    let log = event_log(&st, "q").await;
    assert_eq!(log.durable_next(), log.next_offset());
    drop(log);

    // Turning it off removes the marker too.
    st.set_queue_buffered("q", 0, None, true).await.unwrap();
    st.set_queue_buffered("q", 0, None, false).await.unwrap();
    st.shutdown().await.unwrap();
    drop(st);
    let st = open(&dir.root, 0).await;
    event_log(&st, "q").await;
    assert!(!st.queue_is_buffered("q", 0, None));
    st.shutdown().await.unwrap();
}

#[cfg(all(unix, feature = "io-fault-injection"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_power_loss_child() {
    let Some(root) = std::env::var_os("STROMA_BUFFERED_POWER_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let st = open(&root.join("store"), 0).await;
    st.ensure_queue("q", 0, None).await.unwrap();
    st.set_queue_buffered("q", 0, None, true).await.unwrap();
    let first = publish(&st, "q", 0..3).await;
    // Leases that have already run out, so a restart can requeue them.
    let leased = st.poll_ready("q", 0, None, 100, 1, u64::MAX).await.unwrap();
    assert_eq!(leased.len(), 3);
    settle(&st, first[0], 0).await;
    // The checkpoint syncs everything above, so a power loss keeps it.
    st.snapshot_partition("q", 0, None).await.unwrap();
    publish(&st, "q", 3..6).await;
    settle(&st, first[1], 0).await;
    let log = event_log(&st, "q").await;
    assert!(log.durable_next() < log.next_offset());
    let state = format!(
        "{}\n{}\n{}",
        log.durable_next(),
        log.next_offset(),
        st.tp_part_dir("q", 0, None).display()
    );
    std::fs::write(root.join("handoff/child-state"), state).unwrap();
    std::fs::write(root.join("handoff/child-ready"), b"").unwrap();
    std::future::pending::<()>().await;
}

/// A single-node buffered queue loses power after a checkpoint. What the
/// checkpoint covered survives exactly, the unsynced publishes and ack after
/// it are gone, and new publishes reuse the lost offsets without mixing in
/// old bytes.
#[cfg(all(unix, feature = "io-fault-injection"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn power_loss_keeps_what_the_checkpoint_synced() {
    struct KillOnDrop(std::process::Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let dir = keratin_log::test_dir!("buffered_power_loss");
    std::fs::create_dir_all(dir.root.join("handoff")).unwrap();
    let ready_file = dir.root.join("handoff/child-ready");
    let mut child = KillOnDrop(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "stroma::buffered_queue::tests::buffered_power_loss_child",
                "--nocapture",
            ])
            .env("STROMA_BUFFERED_POWER_ROOT", &dir.root)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(30), async {
        while !ready_file.exists() {
            assert!(child.0.try_wait().unwrap().is_none(), "child exited early");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let state = std::fs::read_to_string(dir.root.join("handoff/child-state")).unwrap();
    let mut lines = state.lines();
    let durable: u64 = lines.next().unwrap().parse().unwrap();
    let next: u64 = lines.next().unwrap().parse().unwrap();
    let part_dir = std::path::PathBuf::from(lines.next().unwrap());
    let log_root = find_log_root(&part_dir);
    keratin_log::io_fault_injection::lose_unsynced_on_next_open(&log_root, durable).unwrap();

    let st = open(&dir.root.join("store"), 0).await;
    let log = event_log(&st, "q").await;
    assert_eq!(log.next_offset(), durable, "unsynced records must be gone");
    assert!(durable < next);
    assert!(st.queue_is_buffered("q", 0, None));
    // Offset 0 was acked before the checkpoint. The ack of offset 1 came
    // after it and was lost, so 1 and 2 are delivered again once their
    // leases expire.
    st.requeue_expired(u64::MAX - 1, 100).await.unwrap();
    let again: Vec<(u64, Vec<u8>)> = ready(&st, "q").await;
    assert_eq!(
        again,
        vec![(1, vec![1; 512]), (2, vec![2; 512])],
        "the checkpoint state must come back exactly"
    );
    // New records take the lost offsets (the requeue record first), and
    // new publishes carry their own bytes.
    let fresh = publish(&st, "q", 9..11).await;
    assert!(fresh[0] >= durable && fresh[0] < next, "{fresh:?} {durable} {next}");
    let delivered = ready(&st, "q").await;
    assert_eq!(
        delivered,
        vec![(fresh[0], vec![9; 512]), (fresh[1], vec![10; 512])]
    );
    st.shutdown().await.unwrap();
}

/// The directory holding a log's `segments`, at or below `dir`.
#[cfg(all(unix, feature = "io-fault-injection"))]
fn find_log_root(dir: &Path) -> std::path::PathBuf {
    if dir.join("segments").is_dir() {
        return dir.to_path_buf();
    }
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_dir() {
            let found = find_log_root(&path);
            if found.join("segments").is_dir() {
                return found;
            }
        }
    }
    dir.to_path_buf()
}
