use stroma_core::QueueState;

#[test]
fn random_operations_never_break_invariants() {
    fastrand::seed(0xC0FFEE);

    let mut g = QueueState::new("test".into(), 0);

    for _ in 0..50_000 {
        let o = fastrand::u64(0..2000);
        match fastrand::u8(0..3) {
            0 => g.mark_inflight(o, fastrand::u64(0..100_000)),
            1 => g.ack(o),
            _ => g.clear_inflight(o),
        }

        // Since we never mention offsets >= 2000, frontier must never exceed 2000.
        assert!(g.acked_until() <= 2000);
    }
}
