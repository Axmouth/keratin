use std::sync::Arc;

use keratin_log::{KeratinConfig, util::test_dir};
use similar_asserts::assert_eq;
use stroma_core::*;

#[tokio::test]
async fn snapshot_delta_replay_is_deterministic() {
    let dir = test_dir("stroma_replay");
    let kcfg = KeratinConfig::test_default();
    let scfg = SnapshotConfig::default();

    let st = Stroma::open(&dir.root, kcfg, scfg).await.unwrap();

    for i in 0..500 {
        st.mark_inflight_one("t", 0,  None, i, 1000000000 + i)
            .await
            .unwrap();
        if i.is_multiple_of(3) {
            st.ack_one("t", 0,  None, i).await.unwrap();
        }
    }

    // snapshot logical state BEFORE drop
    let before = st.debug_dump_queue("t", 0,  None,);

    // force persistence so restart is deterministic
    st.snapshot_partition("t", 0,  None).await.unwrap();

    drop(st);

    let st2 = Stroma::open(&dir.root, kcfg, scfg).await.unwrap();

    let after = st2.debug_dump_queue("t", 0,  None);

    assert_eq!(before, after);
}

#[tokio::test]
async fn expired_messages_survive_restart() {
    let dir = test_dir("expiry_restart");
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
    st.append_message("t", 0,  None, b"x", c).await.unwrap();
    st.mark_inflight_one("t", 0,  None, 0, 10).await.unwrap();
    let offset = rx.await.unwrap().unwrap().base_offset;

    st.list_expired(100, 10).unwrap();
    drop(st);

    let st2 = Stroma::open(
        &dir.root,
        KeratinConfig::test_default(),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    assert!(st2.is_ready("t", 0,  None, offset).unwrap());
}
