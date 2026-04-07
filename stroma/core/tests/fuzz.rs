use std::sync::Arc;

use keratin_log::KeratinConfig;
use stroma_core::{SnapshotConfig, Stroma, TempDir, test_dir};

async fn open_test_stroma() -> (Arc<Stroma>, TempDir) {
    let test_dir = test_dir("test_data");
    let res = Arc::new(
        Stroma::open(
            &test_dir.root,
            KeratinConfig::test_default(),
            SnapshotConfig::default(),
        )
        .await
        .unwrap(),
    );
    (res, test_dir)
}

#[tokio::test]
async fn random_operations_never_break_invariants() {
    fastrand::seed(0xC0FFEE);

    let (st, _test_dir) = open_test_stroma().await;
    let q = st.queue_handle("test", 0, None).await.unwrap();

    for _ in 0..50_000 {
        let o = fastrand::u64(0..2000);
        match fastrand::u8(0..3) {
            0 => q.mark_inflight(o, fastrand::u64(0..100_000)).await,
            1 => q.ack(o).await,
            _ => q.clear_inflight(o).await,
        }

        // Since we never mention offsets >= 2000, frontier must never exceed 2000.
        assert!(q.settled_until().await <= 2000);
    }
}
