use std::sync::Arc;

use keratin_log::{CompletionPair, KeratinAppendCompletion, KeratinConfig, util::{TempDir, test_dir}};
use stroma_core::{Offset, QueueState, SnapshotConfig, Stroma};

async fn open_test_stroma() -> (Arc<Stroma>, TempDir) {
    let test_dir = test_dir("test_data");
    (Arc::new(
        Stroma::open(
            &test_dir.root,
            KeratinConfig::test_default(),
            SnapshotConfig::default(),
        )
        .await
        .unwrap(),
    ), test_dir)
}

pub async fn append_one(
    st: &Arc<Stroma>,
    tp: &str,
    part: u32,
    group: Option<&str>,
    payload: &[u8],
) -> Offset {
    let (c, rx) = KeratinAppendCompletion::pair();
    st.append_message(tp, part, group, payload, c)
        .await
        .unwrap();
    rx.await.unwrap().unwrap().base_offset
}

#[test]
fn expired_messages_are_redelivered_and_never_lost() {
    let mut g = QueueState::new("test".into(), 0);

    for i in 0..100 {
        g.enqueue(i, 0);
        g.mark_inflight(i, 1000);
    }

    let expired = g.collect_expired(2000, 1000);

    for off in expired {
        g.enqueue(off, 1);
        g.mark_inflight(off, 3000);
    }

    assert_eq!(g.inflight_len(), 100);
}

#[tokio::test]
async fn mark_inflight_after_enqueue_is_applied() {
    let (st, _td) = open_test_stroma().await;

    let (completion, rx) = KeratinAppendCompletion::pair();
    st.append_message("t", 0, None, b"x", completion).await.unwrap();

    // Wait until enqueue event is durable + applied
    let ar = rx.await.unwrap().unwrap();
    dbg!(ar);
    let off = ar.base_offset;

    // Now mark inflight is allowed
    st.mark_inflight_one("t", 0, None, off, 100).await.unwrap();

    assert!(st.is_inflight_or_acked("t", 0, None, off).unwrap());
}

#[tokio::test]
async fn published_messages_become_deliverable_eventually() {
    let (st, _td) = open_test_stroma().await;

    let mut rxlist = vec![];
    for i in 0..100 {
        let (completion, rx) = KeratinAppendCompletion::pair();
        rxlist.push(rx);
        st.append_message("t", 0, None, format!("m{i}").as_bytes(), completion)
            .await
            .unwrap();
    }

    for rx in rxlist {
        let _ = rx.await.unwrap().unwrap();
    }

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let d = st.next_deliverable("t", 0,  None,0, 100).unwrap();
            if d < 100 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn enqueue_happens_before_mark_inflight_visibility() {
    let (st, _td) = open_test_stroma().await;

    let (completion, rx) = KeratinAppendCompletion::pair();
    st.append_message("t", 0,  None,b"x", completion).await.unwrap();

    // Wait until enqueue is DURABLE
    rx.await.unwrap().unwrap();

    // Now mark inflight must work
    st.mark_inflight_one("t", 0,  None,0, 100).await.unwrap();

    assert!(st.is_inflight_or_acked("t", 0, None, 0).unwrap());
}

#[tokio::test]
async fn inflight_before_enqueue_is_ignored() {
    let (st, _td) = open_test_stroma().await;

    st.mark_inflight_one("t", 0,  None,0, 100).await.unwrap();
    assert!(!st.is_inflight_or_acked("t", 0, None, 0).unwrap());

    let (completion, rx) = KeratinAppendCompletion::pair();
    st.append_message("t", 0,  None,"s".as_bytes(), completion)
        .await
        .unwrap();
    let ar = rx.await.unwrap().unwrap();
    let offset = ar.base_offset;

    // Must still not be inflight unless re-issued
    assert!(!st.is_inflight_or_acked("t", 0, None, offset).unwrap());
}

#[tokio::test]
async fn append_completion_implies_enqueued() {
    let (st, _td) = open_test_stroma().await;

    let (c, rx) = KeratinAppendCompletion::pair();
    st.append_message("t", 0,  None,b"x", c).await.unwrap();

    let ar = rx.await.unwrap().unwrap();

    assert!(st.is_ready("t", 0, None, ar.base_offset).unwrap());
}

#[tokio::test]
async fn append_completions_may_arrive_out_of_order() {
    let (st, _td) = open_test_stroma().await;

    let mut rxs = vec![];
    for _ in 0..50 {
        let (c, rx) = KeratinAppendCompletion::pair();
        st.append_message("t", 0, None, b"x", c).await.unwrap();
        rxs.push(rx);
    }

    for rx in rxs {
        let ar = rx.await.unwrap().unwrap();
        assert!(st.is_ready("t", 0, None, ar.base_offset).unwrap());
    }
}

#[tokio::test]
async fn poll_ready_delivers_and_marks_inflight() {
    let (st, _td) = open_test_stroma().await;

    // append 3 messages
    for _ in 0..3 {
        let (c, rx) = KeratinAppendCompletion::pair();
        st.append_message("t", 0, None, b"x", c).await.unwrap();
        rx.await.unwrap().unwrap();
    }

    let now = 1000;
    let msgs = st.poll_ready("t", 0, None, 10, now + 100).await.unwrap();

    assert_eq!(msgs.len(), 3);

    for (off, _) in msgs {
        assert!(st.is_inflight_or_acked("t", 0, None, off).unwrap());
    }
}

#[tokio::test]
async fn expired_messages_are_redelivered_via_poll_ready() {
    let (st, _td) = open_test_stroma().await;

    let (c, rx) = KeratinAppendCompletion::pair();
    st.append_message("t", 0, None, b"x", c).await.unwrap();
    let off = rx.await.unwrap().unwrap().base_offset;

    let now = 1000;
    let _ = st.poll_ready("t", 0, None, 1, now + 10).await.unwrap();

    // expire
    let expired = st.list_expired(now + 20, 10).unwrap();
    assert_eq!(expired.len(), 1);

    let msgs2 = st.poll_ready("t", 0, None, 1, now + 30).await.unwrap();
    assert_eq!(msgs2[0].0, off);
}

#[tokio::test]
async fn expired_message_is_redelivered() {
    let (st, _dir) = open_test_stroma().await;

    let off = append_one(&st, "t", 0, None, b"x").await;
    st.mark_inflight_one("t", 0, None, off, 10).await.unwrap();

    st.requeue_expired(10, 10).await.unwrap();

    assert!(st.is_ready("t", 0, None, off).unwrap());
    assert!(!st.is_inflight_or_acked("t", 0, None, off).unwrap());
}

#[tokio::test]
async fn ack_before_expiry_prevents_redelivery() {
    let (st, _dir) = open_test_stroma().await;

    let off = append_one(&st, "t", 0, None, b"x").await;
    st.mark_inflight_one("t", 0, None, off, 10).await.unwrap();
    st.ack_one("t", 0, None, off).await.unwrap();

    st.requeue_expired(20, 10).await.unwrap();

    assert!(st.is_acked("t", 0, None, off).unwrap());
    assert!(!st.is_ready("t", 0, None, off).unwrap());
}

#[tokio::test]
async fn expiry_is_idempotent() {
    let (st, _dir) = open_test_stroma().await;

    let off = append_one(&st, "t", 0, None, b"x").await;
    st.mark_inflight_one("t", 0, None, off, 10).await.unwrap();

    st.requeue_expired(10, 10).await.unwrap();
    st.requeue_expired(10, 10).await.unwrap();

    assert!(st.is_ready("t", 0, None, off).unwrap());
}

#[tokio::test]
async fn expiry_processes_multiple_partitions() {
    let (st, _dir) = open_test_stroma().await;

    let o1 = append_one(&st, "a", 0, None, b"x").await;
    let o2 = append_one(&st, "b", 0, None, b"x").await;

    st.mark_inflight_one("a", 0, None, o1, 10).await.unwrap();
    st.mark_inflight_one("b", 0, None, o2, 10).await.unwrap();

    st.requeue_expired(10, 10).await.unwrap();

    assert!(st.is_ready("a", 0, None, o1).unwrap());
    assert!(st.is_ready("b", 0, None, o2).unwrap());
}

#[tokio::test]
async fn expiry_respects_batch_limit() {
    let (st, _dir) = open_test_stroma().await;

    for i in 0..10 {
        let off = append_one(&st, "t", 0, None, b"x").await;
        st.mark_inflight_one("t", 0, None, off, i + 10).await.unwrap();
    }

    let n = st.requeue_expired(100, 3).await.unwrap().len();
    assert_eq!(n, 3);
}
