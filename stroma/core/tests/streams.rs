use std::{sync::Arc, time::Duration};

use keratin_log::{
    CompletionPair, KDurability, KeratinAppendCompletion, KeratinConfig, Message, test_dir,
    util::TempDir,
};
use stroma_core::{
    DeclareMeta, MessageHeaders, Offset, PartitionKind, QueueHandleError, ReplicatedEventBatch,
    ReplicatedMessageBatch, RetentionConfig, SnapshotConfig, Stroma, StromaError, StromaEvent,
    StromaKeratinConfig,
};

async fn open_on(dir: &std::path::Path) -> Arc<Stroma> {
    Arc::new(
        Stroma::open(
            dir,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap(),
    )
}

async fn open_test_stroma() -> (Arc<Stroma>, TempDir) {
    let test_dir = test_dir!("test_data");
    let st = open_on(&test_dir.root).await;
    (st, test_dir)
}

fn headers() -> MessageHeaders {
    MessageHeaders {
        published: Default::default(),
        publish_received: Default::default(),
        content_type: None,
        extra: Default::default(),
    }
}

async fn append(st: &Arc<Stroma>, tp: &str, payload: &[u8]) -> Offset {
    st.append_stream_record(tp, 0, &headers(), payload.to_vec())
        .await
        .unwrap()
}

// Multi-thread runtime: the kind marker write is a synchronous block, so only
// parallel worker threads can actually interleave it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_identical_stream_declares_all_succeed() {
    let (st, _dir) = open_test_stroma().await;
    // Concurrent identical declares must converge idempotently. The kind
    // marker write used a shared temp file whose rename the first writer
    // consumed, failing the others with a spurious io error.
    for round in 0..16 {
        let tp = format!("sensors-{round}");
        let barrier = Arc::new(tokio::sync::Barrier::new(8));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let st = st.clone();
            let tp = tp.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                st.create_stream(&tp, 0, None).await
            }));
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        assert_eq!(st.partition_kind(&tp, 0, None), PartitionKind::Stream);
    }
    assert_eq!(append(&st, "sensors-0", b"a").await, 0);
}

#[tokio::test]
async fn stream_append_assigns_sequential_offsets_and_advances_tail() {
    let (st, _dir) = open_test_stroma().await;
    st.create_stream("sensors", 0, None).await.unwrap();

    assert_eq!(append(&st, "sensors", b"a").await, 0);
    assert_eq!(append(&st, "sensors", b"b").await, 1);
    assert_eq!(append(&st, "sensors", b"c").await, 2);

    let (head, tail) = st.stream_head_tail("sensors", 0).await.unwrap();
    assert_eq!((head, tail), (0, 3));
}

#[tokio::test]
async fn follower_apply_replicated_stream_batch_advances_tail_and_cursor() {
    // A stream follower applies a replicated two-log batch: records to the
    // message log (at the owner's offsets) and a cursor-commit event. The stream
    // tail advances so head/tail and a promoted owner reflect the records, and
    // the replicated cursor is committed so subscribers resume correctly.
    let (st, _dir) = open_test_stroma().await;
    st.create_stream("sensors", 0, None).await.unwrap();
    st.become_stream_follower_with_epoch("sensors", 0, 0)
        .await
        .unwrap();

    st.apply_replicated_stream_batch(
        "sensors",
        0,
        Some(ReplicatedMessageBatch {
            epoch: 0,
            first_offset: 0,
            records: vec![
                Message {
                    flags: 0,
                    headers: headers().encode().unwrap(),
                    payload: b"a".to_vec(),
                },
                Message {
                    flags: 0,
                    headers: headers().encode().unwrap(),
                    payload: b"b".to_vec(),
                },
            ],
            durability: Some(KDurability::AfterFsync),
        }),
        Some(ReplicatedEventBatch {
            epoch: 0,
            first_offset: 0,
            events: vec![StromaEvent::CursorCommit {
                name: "group-a".into(),
                offset: 2,
            }],
            durability: Some(KDurability::AfterFsync),
            encoded: None,
        }),
    )
    .await
    .unwrap();

    let (head, tail) = st.stream_head_tail("sensors", 0).await.unwrap();
    assert_eq!(
        (head, tail),
        (0, 2),
        "tail advanced to reflect applied records"
    );
    assert_eq!(
        st.stream_cursor("sensors", 0, "group-a").await.unwrap(),
        Some(2),
        "replicated cursor commit applied"
    );

    // Records are readable at the owner's offsets, oldest first.
    let records = st.read_stream_records("sensors", 0, 0, 10).await.unwrap();
    let payloads: Vec<&[u8]> = records.iter().map(|(_, p, _)| p.as_slice()).collect();
    assert_eq!(payloads, vec![b"a".as_slice(), b"b".as_slice()]);
}

#[tokio::test]
async fn stream_cursor_commits_and_reads_back() {
    let (st, _dir) = open_test_stroma().await;
    st.create_stream("sensors", 0, None).await.unwrap();
    for _ in 0..5 {
        append(&st, "sensors", b"x").await;
    }

    assert_eq!(
        st.stream_cursor("sensors", 0, "group-a").await.unwrap(),
        None
    );
    st.commit_stream_cursor("sensors", 0, "group-a", 3)
        .await
        .unwrap();
    assert_eq!(
        st.stream_cursor("sensors", 0, "group-a").await.unwrap(),
        Some(3)
    );
}

#[tokio::test]
async fn stream_cursor_batch_commit_applies_and_survives_restart() {
    let (st, dir) = open_test_stroma().await;
    st.create_stream("sensors", 0, None).await.unwrap();
    for _ in 0..6 {
        append(&st, "sensors", b"x").await;
    }

    // One batch commits several cursors at once (one durable record, one apply).
    st.commit_stream_cursors(
        "sensors",
        0,
        vec![
            ("group-a".to_string(), 2),
            ("group-b".to_string(), 4),
            ("group-c".to_string(), 5),
        ],
    )
    .await
    .unwrap();
    assert_eq!(
        st.stream_cursor("sensors", 0, "group-a").await.unwrap(),
        Some(2)
    );
    assert_eq!(
        st.stream_cursor("sensors", 0, "group-b").await.unwrap(),
        Some(4)
    );
    assert_eq!(
        st.stream_cursor("sensors", 0, "group-c").await.unwrap(),
        Some(5)
    );

    // The batch event replays on recovery exactly like single commits.
    st.shutdown().await.unwrap();
    drop(st);
    let st2 = open_on(&dir.root).await;
    assert_eq!(
        st2.stream_cursor("sensors", 0, "group-a").await.unwrap(),
        Some(2)
    );
    assert_eq!(
        st2.stream_cursor("sensors", 0, "group-c").await.unwrap(),
        Some(5)
    );
}

#[tokio::test]
async fn stream_cursor_and_tail_survive_restart_via_event_replay() {
    let (st, dir) = open_test_stroma().await;
    st.create_stream("sensors", 0, None).await.unwrap();
    for _ in 0..4 {
        append(&st, "sensors", b"x").await;
    }
    st.commit_stream_cursor("sensors", 0, "group-a", 2)
        .await
        .unwrap();
    st.commit_stream_cursor("sensors", 0, "group-b", 4)
        .await
        .unwrap();

    // Restart without an explicit snapshot: recovery replays the CursorCommit
    // events and reconciles the tail from the durable message log.
    st.shutdown().await.unwrap();
    drop(st);
    let st2 = open_on(&dir.root).await;

    assert_eq!(
        st2.stream_cursor("sensors", 0, "group-a").await.unwrap(),
        Some(2)
    );
    assert_eq!(
        st2.stream_cursor("sensors", 0, "group-b").await.unwrap(),
        Some(4)
    );
    let (head, tail) = st2.stream_head_tail("sensors", 0).await.unwrap();
    assert_eq!((head, tail), (0, 4));
}

#[tokio::test]
async fn stream_cursor_survives_evict_and_rematerialize_via_snapshot() {
    let (st, _dir) = open_test_stroma().await;
    st.create_stream("sensors", 0, None).await.unwrap();
    for _ in 0..3 {
        append(&st, "sensors", b"x").await;
    }
    st.commit_stream_cursor("sensors", 0, "group-a", 2)
        .await
        .unwrap();

    // Evict snapshots the dirty stream state, then drops the in-memory handle.
    st.unmaterialize("sensors", 0, None).await.unwrap();

    // The next access re-materializes from disk (snapshot load dispatched to the
    // stream engine) and reconciles the tail.
    assert_eq!(
        st.stream_cursor("sensors", 0, "group-a").await.unwrap(),
        Some(2)
    );
    let (head, tail) = st.stream_head_tail("sensors", 0).await.unwrap();
    assert_eq!((head, tail), (0, 3));
}

#[tokio::test]
async fn stream_retention_drops_oldest_segments_and_clamps_lagging_cursors() {
    // Tiny segments so each record rolls into its own segment, making whole sealed
    // segments droppable by retention (truncation is segment-granular).
    let test_dir = test_dir!("test_data");
    let cfg = KeratinConfig {
        segment_max_bytes: 160,
        ..KeratinConfig::test_default()
    };
    let st = Arc::new(
        Stroma::open(
            &test_dir.root,
            StromaKeratinConfig::from_message_log(cfg),
            SnapshotConfig::default(),
        )
        .await
        .unwrap(),
    );

    st.create_stream(
        "sensors",
        0,
        Some(RetentionConfig {
            max_records: Some(3),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    for i in 0..10u32 {
        append(&st, "sensors", format!("m{i}").as_bytes()).await;
    }
    let (head_before, tail) = st.stream_head_tail("sensors", 0).await.unwrap();
    assert_eq!((head_before, tail), (0, 10));

    // A durable cursor that will fall behind retention.
    st.commit_stream_cursor("sensors", 0, "lagger", 1)
        .await
        .unwrap();

    let dropped = st.enforce_stream_retention("sensors", 0).await.unwrap();
    let new_head = dropped.expect("retention should drop oldest segments");
    assert!(new_head > 0, "head should advance");
    assert!(new_head <= 7, "must keep at least the newest ~3 records");

    let (head_after, tail_after) = st.stream_head_tail("sensors", 0).await.unwrap();
    assert_eq!(head_after, new_head);
    assert_eq!(tail_after, 10, "the tail (and active segment) is untouched");

    // The lagging cursor was clamped up to the new head.
    assert_eq!(
        st.stream_cursor("sensors", 0, "lagger").await.unwrap(),
        Some(new_head)
    );

    // Records below the new head are physically gone; the newest are still there.
    let qh = st.queue_handle("sensors", 0, None).await.unwrap();
    let qh = qh.resolve().unwrap();
    assert!(st.scan_messages_from(&qh, new_head, 100).unwrap().len() as u64 >= tail - new_head);
}

#[tokio::test]
async fn queue_command_on_stream_partition_is_rejected() {
    let (st, _dir) = open_test_stroma().await;
    st.create_stream("sensors", 0, None).await.unwrap();

    // A queue-style publish targets the queue engine, which a stream partition
    // does not host, so it must error rather than corrupt state.
    let (c, rx) = KeratinAppendCompletion::pair();
    let res = st
        .append_message("sensors", 0, None, &headers(), b"x".to_vec(), c)
        .await;
    // The append is rejected either at submit or at completion.
    let rejected = res.is_err() || rx.await.map(|r| r.is_err()).unwrap_or(true);
    assert!(rejected, "queue publish on a stream partition should fail");
}

fn queue_meta() -> DeclareMeta {
    DeclareMeta {
        dlq_policy: None,
        dlq_max_retries: None,
        default_message_ttl_ms: None,
    }
}

#[tokio::test]
async fn declaring_a_queue_then_a_stream_is_rejected() {
    let (st, _dir) = open_test_stroma().await;
    st.declare("shared", 0, None, queue_meta()).await.unwrap();
    let err = st.create_stream("shared", 0, None).await.unwrap_err();
    assert!(matches!(err, StromaError::InvalidArgument(_)));
}

#[tokio::test]
async fn declaring_a_stream_then_a_queue_is_rejected() {
    let (st, _dir) = open_test_stroma().await;
    st.create_stream("shared", 0, None).await.unwrap();
    let err = st
        .declare("shared", 0, None, queue_meta())
        .await
        .unwrap_err();
    assert!(matches!(err, StromaError::InvalidArgument(_)));
}

#[tokio::test]
async fn redeclaring_same_kind_is_idempotent() {
    let (st, _dir) = open_test_stroma().await;
    st.create_stream("s", 0, None).await.unwrap();
    st.create_stream("s", 0, None).await.unwrap();
    st.declare("q", 0, None, queue_meta()).await.unwrap();
    st.declare("q", 0, None, queue_meta()).await.unwrap();
}

#[tokio::test]
async fn handle_projects_only_to_its_own_kind() {
    let (st, _dir) = open_test_stroma().await;

    // A work queue projects to the work-queue surface and refuses the stream one.
    st.declare("jobs", 0, None, queue_meta()).await.unwrap();
    let q = st.queue_handle("jobs", 0, None).await.unwrap();
    let q = q.resolve().unwrap();
    assert!(q.as_work_queue().is_some());
    assert!(q.as_stream().is_none());
    assert!(matches!(
        q.stream(),
        Err(QueueHandleError::WrongKind {
            expected: PartitionKind::Stream,
            actual: PartitionKind::Queue,
        })
    ));

    // A stream projects the other way.
    st.create_stream("sensors", 0, None).await.unwrap();
    let s = st.queue_handle("sensors", 0, None).await.unwrap();
    let s = s.resolve().unwrap();
    assert!(s.as_stream().is_some());
    assert!(s.as_work_queue().is_none());
    assert!(matches!(
        s.work_queue(),
        Err(QueueHandleError::WrongKind {
            expected: PartitionKind::Queue,
            actual: PartitionKind::Stream,
        })
    ));
}

/// The `messages` log root of a stream partition somewhere below `dir`.
#[cfg(feature = "io-fault-injection")]
fn find_message_log_root(dir: &std::path::Path) -> Option<std::path::PathBuf> {
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if path.file_name().is_some_and(|n| n == "messages") && path.join("segments").is_dir() {
            return Some(path);
        }
        if let Some(found) = find_message_log_root(&path) {
            return Some(found);
        }
    }
    None
}

/// A snapshot taken while records were staged but not yet synced must not
/// bring the stream back with its tail or cursors past the records that
/// survived a power loss. New records after the restart reuse the lost
/// offsets, so a cursor left at the old tail would skip them.
#[cfg(feature = "io-fault-injection")]
#[tokio::test]
async fn stream_snapshot_past_lost_records_reopens_at_the_log_end() {
    let (st, dir) = open_test_stroma().await;
    st.create_stream("sensors", 0, None).await.unwrap();
    for _ in 0..3 {
        append(&st, "sensors", b"kept").await;
    }
    st.sync_stream("sensors", 0).await.unwrap();
    let durable = st.stream_durable_next("sensors", 0).await.unwrap();
    assert_eq!(durable, 3);
    // An event at offset zero alone makes an ambiguous snapshot boundary that
    // recovery ignores, so log one first.
    st.commit_stream_cursor("sensors", 0, "early", 1)
        .await
        .unwrap();

    // These two are lost below. The cursor and the snapshot both see them.
    append(&st, "sensors", b"lost").await;
    append(&st, "sensors", b"lost").await;
    st.commit_stream_cursor("sensors", 0, "in-snapshot", 5)
        .await
        .unwrap();
    st.snapshot_partition("sensors", 0, None).await.unwrap();
    // Committed after the snapshot, so recovery replays it from the event log.
    st.commit_stream_cursor("sensors", 0, "replayed", 5)
        .await
        .unwrap();
    assert_eq!(st.stream_head_tail("sensors", 0).await.unwrap(), (0, 5));

    st.shutdown().await.unwrap();
    drop(st);
    let log_root = find_message_log_root(&dir.root).expect("stream message log");
    keratin_log::io_fault_injection::lose_unsynced_on_next_open(&log_root, durable).unwrap();

    let st = open_on(&dir.root).await;
    assert_eq!(
        st.stream_head_tail("sensors", 0).await.unwrap(),
        (0, durable),
        "the tail must match the surviving log"
    );
    for name in ["in-snapshot", "replayed"] {
        let cursor = st.stream_cursor("sensors", 0, name).await.unwrap();
        assert_eq!(
            cursor,
            Some(durable),
            "cursor {name} must not pass the tail"
        );
    }

    let first = append(&st, "sensors", b"new-1").await;
    let second = append(&st, "sensors", b"new-2").await;
    assert_eq!((first, second), (durable, durable + 1));
    assert_eq!(
        st.stream_head_tail("sensors", 0).await.unwrap(),
        (0, durable + 2)
    );

    for name in ["in-snapshot", "replayed"] {
        let from = st.stream_cursor("sensors", 0, name).await.unwrap().unwrap();
        let resumed: Vec<(Offset, Vec<u8>)> = st
            .read_stream_records("sensors", 0, from, 10)
            .await
            .unwrap()
            .into_iter()
            .map(|(off, payload, _)| (off, payload))
            .collect();
        assert_eq!(
            resumed,
            vec![(first, b"new-1".to_vec()), (second, b"new-2".to_vec())],
            "a consumer resuming from {name} must see the new records"
        );
    }
    st.shutdown().await.unwrap();
}

async fn open_with_sync_interval(dir: &std::path::Path, interval: Duration) -> Arc<Stroma> {
    Arc::new(
        Stroma::open(
            dir,
            StromaKeratinConfig::from_message_log(KeratinConfig {
                background_sync_interval: interval,
                ..KeratinConfig::test_default()
            }),
            SnapshotConfig::default(),
        )
        .await
        .unwrap(),
    )
}

async fn stream_event_log(st: &Arc<Stroma>, tp: &str) -> Arc<keratin_log::Keratin> {
    let qh = st.queue_handle(tp, 0, None).await.unwrap();
    qh.resolve().unwrap().event_log()
}

async fn stream_message_log(st: &Arc<Stroma>, tp: &str) -> Arc<keratin_log::Keratin> {
    let qh = st.queue_handle(tp, 0, None).await.unwrap();
    qh.resolve().unwrap().msg_log().unwrap()
}

fn record(payload: &[u8]) -> Message {
    Message {
        flags: 0,
        headers: headers().encode().unwrap(),
        payload: payload.to_vec(),
    }
}

async fn wait_for(what: &str, mut check: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !check() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

/// Cursor commits are written to the OS without an fsync each and made
/// durable by the event log's background sync. The commit is visible at once.
#[tokio::test]
async fn stream_cursor_commits_are_synced_in_the_background() {
    let dir = test_dir!("cursor_commit_background_sync");
    let interval = Duration::from_millis(300);
    let opened = std::time::Instant::now();
    let st = open_with_sync_interval(&dir.root, interval).await;
    st.create_stream("sensors", 0, None).await.unwrap();
    for _ in 0..4 {
        append(&st, "sensors", b"x").await;
    }
    st.commit_stream_cursor("sensors", 0, "single", 1)
        .await
        .unwrap();
    st.commit_stream_cursors("sensors", 0, vec![("batch".to_string(), 3)])
        .await
        .unwrap();
    assert_eq!(
        st.stream_cursor("sensors", 0, "batch").await.unwrap(),
        Some(3)
    );
    let events = stream_event_log(&st, "sensors").await;
    assert_eq!(events.written_next(), 2);
    if opened.elapsed() < interval {
        // Open counts as the last sync, so nothing has synced them yet.
        assert_eq!(events.durable_next(), 0);
    }
    wait_for("background sync", || events.durable_next() == 2).await;
    assert!(events.sync_status().background_syncs_started >= 1);
    // A stream event log is not expected to sync every write, so the
    // background sync covering commits is not reported.
    assert_eq!(events.sync_status().unexpected_background_syncs, 0);

    // A snapshot syncs the event log through its boundary first.
    st.commit_stream_cursor("sensors", 0, "single", 2)
        .await
        .unwrap();
    st.snapshot_partition("sensors", 0, None).await.unwrap();
    assert_eq!(events.durable_next(), 3);
    st.shutdown().await.unwrap();
}

/// A follower writes a cursor-only event batch without an fsync, so the batch
/// completes once the records are durable. A batch with any other event keeps
/// the durability it asked for.
#[tokio::test]
async fn follower_cursor_events_do_not_wait_for_an_fsync() {
    let dir = test_dir!("follower_cursor_no_fsync");
    let st = open_with_sync_interval(&dir.root, Duration::from_secs(3600)).await;
    st.create_stream("sensors", 0, None).await.unwrap();
    st.become_stream_follower_with_epoch("sensors", 0, 0)
        .await
        .unwrap();

    let outcome = st
        .apply_replicated_stream_batch(
            "sensors",
            0,
            Some(ReplicatedMessageBatch {
                epoch: 0,
                first_offset: 0,
                records: vec![record(b"a"), record(b"b")],
                durability: Some(KDurability::AfterFsync),
            }),
            Some(ReplicatedEventBatch {
                epoch: 0,
                first_offset: 0,
                events: vec![
                    StromaEvent::CursorCommit {
                        name: "group-a".into(),
                        offset: 1,
                    },
                    StromaEvent::CursorCommitBatch {
                        commits: vec![("group-b".into(), 2)],
                    },
                ],
                durability: Some(KDurability::AfterFsync),
                encoded: None,
            }),
        )
        .await
        .unwrap();
    assert!(outcome.message_log.is_some() && outcome.event_log.is_some());

    let records = stream_message_log(&st, "sensors").await;
    let events = stream_event_log(&st, "sensors").await;
    assert_eq!(records.durable_next(), 2, "records are synced");
    assert_eq!(events.written_next(), 2, "cursor events are written");
    assert_eq!(events.durable_next(), 0, "cursor events wait for a sync");
    assert_eq!(st.stream_head_tail("sensors", 0).await.unwrap(), (0, 2));
    assert_eq!(
        st.stream_cursor("sensors", 0, "group-b").await.unwrap(),
        Some(2)
    );

    // A truncate in the batch keeps the fsync, which covers the earlier
    // cursor events too.
    st.apply_replicated_stream_batch(
        "sensors",
        0,
        None,
        Some(ReplicatedEventBatch {
            epoch: 0,
            first_offset: 2,
            events: vec![
                StromaEvent::CursorCommit {
                    name: "group-a".into(),
                    offset: 2,
                },
                StromaEvent::StreamTruncate { before: 0 },
            ],
            durability: Some(KDurability::AfterFsync),
            encoded: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(events.durable_next(), 4);
    st.shutdown().await.unwrap();
}

#[cfg(feature = "io-fault-injection")]
fn find_event_log_root(dir: &std::path::Path, topic: &str) -> Option<std::path::PathBuf> {
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if path.file_name().is_some_and(|n| n == "log")
            && path.join("segments").is_dir()
            && path.to_string_lossy().contains(topic)
        {
            return Some(path);
        }
        if let Some(found) = find_event_log_root(&path, topic) {
            return Some(found);
        }
    }
    None
}

/// A power cut drops cursor commits no sync covered. The stream reopens
/// cleanly with cursors from the snapshot and the surviving events, new
/// commits take the freed offsets, and they survive another restart.
#[cfg(feature = "io-fault-injection")]
#[tokio::test]
async fn unsynced_cursor_commits_lost_in_a_power_cut_reopen_cleanly() {
    let dir = test_dir!("cursor_commit_power_cut");
    let st = open_with_sync_interval(&dir.root, Duration::from_secs(3600)).await;
    st.create_stream("sensors", 0, None).await.unwrap();
    for _ in 0..6 {
        append(&st, "sensors", b"x").await;
    }
    st.sync_stream("sensors", 0).await.unwrap();
    // Two events, so the snapshot boundary is not the ambiguous zero.
    st.commit_stream_cursor("sensors", 0, "r", 1).await.unwrap();
    st.commit_stream_cursor("sensors", 0, "r", 2).await.unwrap();
    st.snapshot_partition("sensors", 0, None).await.unwrap();
    let events = stream_event_log(&st, "sensors").await;
    let durable = events.durable_next();
    assert_eq!(durable, 2, "the snapshot synced its boundary");

    // Neither of these is synced before the cut.
    st.commit_stream_cursor("sensors", 0, "r", 4).await.unwrap();
    st.commit_stream_cursors(
        "sensors",
        0,
        vec![("r".to_string(), 5), ("s".to_string(), 3)],
    )
    .await
    .unwrap();
    assert_eq!(events.written_next(), 4);
    assert_eq!(events.durable_next(), durable);
    drop(events);

    st.shutdown().await.unwrap();
    drop(st);
    let root = find_event_log_root(&dir.root, "sensors").expect("stream event log");
    keratin_log::io_fault_injection::lose_unsynced_on_next_open(&root, durable).unwrap();

    let st = open_with_sync_interval(&dir.root, Duration::from_secs(3600)).await;
    assert_eq!(st.stream_head_tail("sensors", 0).await.unwrap(), (0, 6));
    assert_eq!(st.stream_cursor("sensors", 0, "r").await.unwrap(), Some(2));
    assert_eq!(st.stream_cursor("sensors", 0, "s").await.unwrap(), None);
    assert!(st.quarantined_partitions().is_empty());
    assert_eq!(
        stream_event_log(&st, "sensors").await.next_offset(),
        durable
    );

    // New commits reuse the lost offsets.
    st.commit_stream_cursor("sensors", 0, "r", 3).await.unwrap();
    st.commit_stream_cursors("sensors", 0, vec![("s".to_string(), 4)])
        .await
        .unwrap();
    assert_eq!(stream_event_log(&st, "sensors").await.next_offset(), 4);
    st.shutdown().await.unwrap();
    drop(st);

    // A clean restart replays them past the old snapshot boundary.
    let st = open_with_sync_interval(&dir.root, Duration::from_secs(3600)).await;
    assert_eq!(st.stream_cursor("sensors", 0, "r").await.unwrap(), Some(3));
    assert_eq!(st.stream_cursor("sensors", 0, "s").await.unwrap(), Some(4));
    st.shutdown().await.unwrap();
}
