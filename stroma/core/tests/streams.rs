use std::sync::Arc;

use keratin_log::{
    CompletionPair, KeratinAppendCompletion, KeratinConfig, test_dir, util::TempDir,
};
use stroma_core::{MessageHeaders, Offset, SnapshotConfig, Stroma, StromaKeratinConfig};

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
