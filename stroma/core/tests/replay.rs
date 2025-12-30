use keratin_log::{KeratinConfig, util::test_dir};
use similar_asserts::assert_eq;
use stroma_core::*;

#[tokio::test]
async fn snapshot_delta_replay_is_deterministic() {
    let dir = test_dir("stroma_replay");
    let kcfg = KeratinConfig::test_default();
    let scfg = SnapshotConfig::default();

    let st = Stroma::open(&dir.root, kcfg.clone(), scfg.clone())
        .await
        .unwrap();

    for i in 0..500 {
        st.mark_inflight_one("t", 0, "g", i, 1000000000 + i)
            .await
            .unwrap();
        if i.is_multiple_of(3) {
            st.ack_one("t", 0, "g", i).await.unwrap();
        }
    }

    // snapshot logical state BEFORE drop
    let before = st.debug_dump_group("t", 0, "g");

    // force persistence so restart is deterministic
    st.snapshot_partition("t", 0).await.unwrap();

    drop(st);

    let st2 = Stroma::open(&dir.root, kcfg, scfg).await.unwrap();

    let after = st2.debug_dump_group("t", 0, "g");

    assert_eq!(before, after);
}
