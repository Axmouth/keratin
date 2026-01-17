use stroma_core::QueueState;

#[test]
fn expired_messages_are_redelivered_and_never_lost() {
    let mut g = QueueState::new("test".into(), 0);

    for i in 0..100 {
        g.mark_inflight(i, 1000);
    }

    let expired = g.pop_expired(2000, 1000);

    for off in expired {
        g.mark_inflight(off, 3000);
    }

    assert_eq!(g.inflight_len(), 100);
}
