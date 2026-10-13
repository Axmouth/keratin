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
