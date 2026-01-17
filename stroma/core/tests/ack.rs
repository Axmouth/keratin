use stroma_core::QueueState;

#[test]
fn out_of_order_acks_never_skip_frontier() {
    let mut g = QueueState::new("test".into(), 0);

    for i in 0..1000 {
        g.mark_inflight(i, 0);
    }

    for i in (0..1000).rev() {
        g.ack(i);
    }

    assert_eq!(g.acked_until(), 1000);
}
