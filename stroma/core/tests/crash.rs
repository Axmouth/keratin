use keratin_log::{KeratinConfig, util::test_dir};
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
