use std::sync::Arc;

use keratin_log::{CompletionPair, KeratinAppendCompletion, KeratinConfig, util::test_dir};
use stroma_core::{QueueState, SnapshotConfig, Stroma};

async fn open_test_stroma() -> Arc<Stroma> {
    let test_dir = test_dir("test_data");
    Arc::new(Stroma::open(
        &test_dir.root,
        KeratinConfig::test_default(),
        SnapshotConfig::default(),
    )
    .await
    .unwrap())
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
    let st = open_test_stroma().await;

    let (completion, rx) = KeratinAppendCompletion::pair();
    st.append_message("t", 0, b"x", completion).await.unwrap();

    // Wait until enqueue event is durable + applied
    let ar = rx.await.unwrap().unwrap();
    dbg!(ar);
    let off = ar.base_offset;

    // Now mark inflight is allowed
    st.mark_inflight_one("t", 0, off, 100).await.unwrap();

    assert!(st.is_inflight_or_acked("t", 0, off).unwrap());
}

#[tokio::test]
async fn published_messages_become_deliverable_eventually() {
    let st = open_test_stroma().await;

    let mut rxlist = vec![];
    for i in 0..100 {
        let (completion, rx) = KeratinAppendCompletion::pair();
        rxlist.push(rx);
        st.append_message("t", 0, format!("m{i}").as_bytes(), completion)
            .await
            .unwrap();
    }

    for rx in rxlist {
        let _ = rx.await.unwrap().unwrap();
    }

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let d = st.next_deliverable("t", 0, 0, 100).unwrap();
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
    let st = open_test_stroma().await;

    let (completion, rx) = KeratinAppendCompletion::pair();
    st.append_message("t", 0, b"x", completion).await.unwrap();

    // Wait until enqueue is DURABLE
    rx.await.unwrap().unwrap();

    // Now mark inflight must work
    st.mark_inflight_one("t", 0, 0, 100).await.unwrap();

    assert!(st.is_inflight_or_acked("t", 0, 0).unwrap());
}

#[tokio::test]
async fn inflight_before_enqueue_is_ignored() {
    let st = open_test_stroma().await;

    st.mark_inflight_one("t", 0, 0, 100).await.unwrap();
    assert!(!st.is_inflight_or_acked("t", 0, 0).unwrap());

    let (completion, rx) = KeratinAppendCompletion::pair();
    st.append_message("t", 0, "s".as_bytes(), completion).await.unwrap();
    let ar = rx.await.unwrap().unwrap();
    let offset = ar.base_offset;

    // Must still not be inflight unless re-issued
    assert!(!st.is_inflight_or_acked("t", 0, offset).unwrap());
}

#[tokio::test]
async fn append_completion_implies_enqueued() {
    let st = open_test_stroma().await;

    let (c, rx) = KeratinAppendCompletion::pair();
    st.append_message("t", 0, b"x", c).await.unwrap();

    let ar = rx.await.unwrap().unwrap();

    assert!(st.is_enqueued("t", 0, ar.base_offset).unwrap());
}

#[tokio::test]
async fn append_completions_may_arrive_out_of_order() {
    let st = open_test_stroma().await;

    let mut rxs = vec![];
    for _ in 0..50 {
        let (c, rx) = KeratinAppendCompletion::pair();
        st.append_message("t", 0, b"x", c).await.unwrap();
        rxs.push(rx);
    }

    for rx in rxs {
        let ar = rx.await.unwrap().unwrap();
        assert!(st.is_enqueued("t", 0, ar.base_offset).unwrap());
    }
}
