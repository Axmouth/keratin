use std::sync::Arc;

use keratin_log::{
    CompletionPair, KDurability, KeratinAppendCompletion, KeratinConfig, Message, test_dir,
    util::TempDir,
};
use stroma_core::{
    DeclareMeta, MessageHeaders, Offset, ReplicatedEventBatch, ReplicatedMessageBatch,
    RetentionConfig, SnapshotConfig, Stroma, StromaError, StromaEvent, StromaKeratinConfig,
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
    assert_eq!((head, tail), (0, 2), "tail advanced to reflect applied records");
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
    let err = st.declare("shared", 0, None, queue_meta()).await.unwrap_err();
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
