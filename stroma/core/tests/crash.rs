use std::sync::Arc;

use keratin_log::{CompletionPair, KeratinAppendCompletion, KeratinConfig, util::test_dir};
use stroma_core::{SnapshotConfig, Stroma};

#[tokio::test]
async fn truncated_delta_does_not_corrupt_state() {
    let dir = test_dir("test_data");
    let kcfg = KeratinConfig::test_default();
    let scfg = SnapshotConfig::default();

    let st = Stroma::open(&dir.root, kcfg, scfg).await.unwrap();

    for i in 0..1000 {
        st.mark_inflight_one("t", 0, i, 1000).await.unwrap();
        if i % 4 == 0 {
            st.ack_one("t", 0, i).await.unwrap();
        }
    }

    st.truncate_partition_log("t", 0, 123).await.unwrap();
    drop(st);

    let st2 = Stroma::open(&dir.root, kcfg, scfg).await.unwrap();
    st2.validate().unwrap();
}

#[tokio::test]
async fn enqueue_is_durable_and_replayed() {
    let dir = test_dir("enqueue_replay");
    let st = Arc::new(
        Stroma::open(
            &dir.root,
            KeratinConfig::test_default(),
            SnapshotConfig::default(),
        )
        .await
        .unwrap(),
    );

    let (completion, rx) = KeratinAppendCompletion::pair();
    st.append_message("t", 0, b"hello", completion)
        .await
        .unwrap();
    let _append_result = rx.await.unwrap().unwrap();

    // Force snapshot & restart
    st.snapshot_partition("t", 0).await.unwrap();
    drop(st);

    let st2 = Stroma::open(
        &dir.root,
        KeratinConfig::test_default(),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();

    assert!(st2.is_enqueued("t", 0, 0).unwrap());
}

#[tokio::test]
async fn crash_between_message_and_enqueue_event_is_safe() {
    let test_dir = test_dir("test_data");
    let st = Arc::new(
        Stroma::open(
            &test_dir.root,
            KeratinConfig::test_default(),
            SnapshotConfig::default(),
        )
        .await
        .unwrap(),
    );

    let (c, rx) = KeratinAppendCompletion::pair();
    st.append_message("t", 0, b"x", c).await.unwrap();
    let offset = rx.await.unwrap().unwrap().base_offset;

    drop(st);

    let st2 = Stroma::open(
        &test_dir.root,
        KeratinConfig::test_default(),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    st2.validate().unwrap();

    // message may or may not exist, but must not be inflight/enqueued
    assert!(!st2.is_enqueued("t", 0, offset).unwrap());
}
