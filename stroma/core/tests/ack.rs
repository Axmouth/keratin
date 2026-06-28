use std::sync::Arc;

use keratin_log::{
    CompletionPair, KeratinAppendCompletion, KeratinConfig, test_dir, util::TempDir,
};
use stroma_core::{MessageHeaders, Offset, SnapshotConfig, Stroma, StromaKeratinConfig};

async fn open_test_stroma() -> (Arc<Stroma>, TempDir) {
    let test_dir = test_dir!("test_data");
    let res = Arc::new(
        Stroma::open(
            &test_dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap(),
    );
    (res, test_dir)
}

pub async fn append_one(
    st: &Arc<Stroma>,
    tp: &str,
    part: u32,
    group: Option<&str>,
    payload: &[u8],
) -> Offset {
    let (c, rx) = KeratinAppendCompletion::pair();
    let headers = MessageHeaders {
        published: Default::default(),
        publish_received: Default::default(),
        content_type: None,
        extra: Default::default(),
    };
    st.append_message(tp, part, group, &headers, payload.to_vec(), c)
        .await
        .unwrap();
    rx.await.unwrap().unwrap().base_offset
}

#[tokio::test]
async fn out_of_order_acks_never_skip_frontier() {
    let (st, _test_dir) = open_test_stroma().await;
    let q = st.queue_handle("test", 0, None).await.unwrap();
    let q = q.resolve().unwrap();
    let q = q.work_queue().unwrap();

    for i in 0..1000 {
        q.mark_inflight(i, 0).await.unwrap();
    }

    for i in (0..1000).rev() {
        q.ack(i).await.unwrap();
    }

    assert_eq!(q.settled_until().await, 1000);
}

#[tokio::test]
async fn acked_offsets_never_resurrect() {
    let (st, _test_dir) = open_test_stroma().await;

    // ACK offset 5 before it exists
    st.ack_one("t", 0, None, 5).await.unwrap();
    let headers = MessageHeaders {
        published: Default::default(),
        publish_received: Default::default(),
        content_type: None,
        extra: Default::default(),
    };

    // Append messages until offsets advance past 5
    loop {
        let (c, rx) = KeratinAppendCompletion::pair();
        st.append_message("t", 0, None, &headers, b"x".to_vec(), c)
            .await
            .unwrap();
        let offset = rx.await.unwrap().unwrap().base_offset;
        println!("{offset} appended");

        if offset >= 5 {
            assert_eq!(offset, 5, "offsets must be contiguous in this test");
            break;
        }
    }

    assert!(st.is_settled("t", 0, None, 5).await.unwrap());
    assert!(!st.is_ready("t", 0, None, 5).await.unwrap());
}

#[tokio::test]
async fn expiry_never_resurrects_acked_offsets_after_restart() {
    let (st, dir) = open_test_stroma().await;

    let off = append_one(&st, "t", 0, None, b"x").await;
    st.mark_inflight_one("t", 0, None, off, 10).await.unwrap();
    st.ack_one("t", 0, None, off).await.unwrap();

    st.requeue_expired(20, 10).await.unwrap();
    st.shutdown().await.unwrap();
    drop(st);

    let st2 = Stroma::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();

    assert!(st2.is_settled("t", 0, None, off).await.unwrap());
    assert!(!st2.is_ready("t", 0, None, off).await.unwrap());
}

#[tokio::test]
async fn expiry_respects_max_retries() {
    let (st, _dir) = open_test_stroma().await;

    let off = append_one(&st, "t", 0, None, b"x").await;

    for _ in 0..5 {
        st.mark_inflight_one("t", 0, None, off, 10).await.unwrap();
        st.requeue_expired(10, 10).await.unwrap();
    }

    // One more expiry should DLQ / ack terminally
    st.mark_inflight_one("t", 0, None, off, 10).await.unwrap();
    st.requeue_expired(10, 10).await.unwrap();

    assert!(st.is_settled("t", 0, None, off).await.unwrap());
    assert!(!st.is_ready("t", 0, None, off).await.unwrap());

    let qh = st.queue_handle("t", 0, None).await.unwrap();
    let qh = qh.resolve().unwrap();
    let qh = qh.work_queue().unwrap();

    qh.dead_letter_commit(vec![off]).await.unwrap();

    assert!(st.is_settled("t", 0, None, off).await.unwrap());
    assert!(!st.is_ready("t", 0, None, off).await.unwrap());
}
