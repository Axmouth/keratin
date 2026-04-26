use std::sync::Arc;

use keratin_log::{KeratinConfig, test_dir};
use similar_asserts::assert_eq;
use stroma_core::*;

#[tokio::test]
async fn snapshot_delta_replay_is_deterministic() {
    let dir = test_dir!("stroma_replay");
    let kcfg = KeratinConfig::test_default();
    let scfg = SnapshotConfig::default();

    let st = Stroma::open(&dir.root, kcfg, scfg).await.unwrap();

    for i in 0..500 {
        st.mark_inflight_one("t", 0, None, i, 1000000000 + i)
            .await
            .unwrap();
        if i.is_multiple_of(3) {
            st.ack_one("t", 0, None, i).await.unwrap();
        }
    }

    // snapshot logical state BEFORE drop
    let before = st.debug_dump_queue("t", 0, None).await.unwrap();

    // force persistence so restart is deterministic
    st.snapshot_partition("t", 0, None).await.unwrap();
    st.shutdown().await.unwrap();

    drop(st);

    let st2 = Stroma::open(&dir.root, kcfg, scfg).await.unwrap();

    let after = st2.debug_dump_queue("t", 0, None).await.unwrap();

    assert_eq!(before, after);
}

#[tokio::test]
async fn expired_messages_survive_restart() {
    let dir = test_dir!("expiry_restart");
    let st = Arc::new(
        Stroma::open(
            &dir.root,
            KeratinConfig::test_default(),
            SnapshotConfig::default(),
        )
        .await
        .unwrap(),
    );

    let (c, rx) = KeratinAppendCompletion::pair();
    let headers = MessageHeaders {
        published: Default::default(),
        publish_received: Default::default(),
        extra: Default::default(),
    };
    st.append_message("t", 0, None, &headers, b"x", c).await.unwrap();
    st.mark_inflight_one("t", 0, None, 0, 10).await.unwrap();
    let offset = rx.await.unwrap().unwrap().base_offset;

    st.list_expired(100, 10).await.unwrap();
    st.shutdown().await.unwrap();
    drop(st);

    let st2 = Stroma::open(
        &dir.root,
        KeratinConfig::test_default(),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    assert!(st2.is_ready("t", 0, None, offset).await.unwrap());
}

#[tokio::test]
async fn discover_partitions_handles_encoded_names() {
    // create dirs like:
    // events/group%2Fa/topic%2Fb/0000000001
    let dir = test_dir!("discover_partitions_handles_encoded_names");
    let st = Arc::new(
        Stroma::open(
            &dir.root,
            KeratinConfig::test_default(),
            SnapshotConfig::default(),
        )
        .await
        .unwrap(),
    );

    st.queue_handle("a", 1, Some("b")).await.unwrap();
    st.queue_handle("c", 2, Some("d")).await.unwrap();
    st.queue_handle("topic+\\/i", 3, Some("group+\\/j")).await.unwrap();
    st.queue_handle("topic/e", 4, Some("group/f")).await.unwrap();
    st.queue_handle("topic g", 5, Some("group h")).await.unwrap();

    let mut parts = st.discover_partitions().unwrap();
    parts.sort();

    assert_eq!(
        parts,
        vec![
            (Some("b".to_string()), "a".to_string(), 1),
            (Some("d".to_string()), "c".to_string(), 2),
            (Some("group h".to_string()), "topic g".to_string(), 5),
            (Some("group+\\/j".to_string()), "topic+\\/i".to_string(), 3),
            (Some("group/f".to_string()), "topic/e".to_string(), 4),
        ]
    );
}
