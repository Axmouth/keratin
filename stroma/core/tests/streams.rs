use std::sync::Arc;

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
