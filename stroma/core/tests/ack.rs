use std::sync::Arc;

use keratin_log::{CompletionPair, KeratinAppendCompletion, KeratinConfig, util::test_dir};
use stroma_core::{QueueState, SnapshotConfig, Stroma};

async fn open_test_stroma() -> Arc<Stroma> {
    let test_dir = test_dir("test_data");
    Arc::new(
        Stroma::open(
            &test_dir.root,
            KeratinConfig::test_default(),
            SnapshotConfig::default(),
        )
        .await
        .unwrap(),
    )
}

#[test]
fn out_of_order_acks_never_skip_frontier() {
    let mut g = QueueState::new("test".into(), 0);

    for i in 0..1000 {
        g.mark_inflight(i, 0);
    }

    for i in (0..1000).rev() {
        g.ack(i);
    }

    assert_eq!(g.settled_until(), 1000);
}

#[tokio::test]
async fn acked_offsets_never_resurrect() {
    let st = open_test_stroma().await;

    // ACK offset 5 before it exists
    st.ack_one("t", 0, 5).await.unwrap();

    // Append messages until offsets advance past 5
    loop {
        let (c, rx) = KeratinAppendCompletion::pair();
        st.append_message("t", 0, b"x", c).await.unwrap();
        let offset = rx.await.unwrap().unwrap().base_offset;

        if offset >= 5 {
            assert_eq!(offset, 5, "offsets must be contiguous in this test");
            break;
        }
    }

    assert!(st.is_acked("t", 0, 5).unwrap());
    assert!(!st.is_enqueued("t", 0, 5).unwrap());
}
