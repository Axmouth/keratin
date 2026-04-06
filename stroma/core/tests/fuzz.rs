use stroma_core::QueueHandle;

#[tokio::test]
async fn random_operations_never_break_invariants() {
    fastrand::seed(0xC0FFEE);

    let q = QueueHandle::init("test".into(), 0);

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
