use std::sync::Arc;

use keratin_log::{
    CompletionPair, KeratinAppendCompletion, KeratinConfig, test_dir, util::TempDir,
};
use stroma_core::{
    MessageHeaders, Offset, RetentionConfig, SnapshotConfig, Stroma, StromaKeratinConfig,
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
