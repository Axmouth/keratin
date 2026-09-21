//! Reachable storage interleavings and state-level ordering constraints.
//! State-level permutations alone do not establish broker reachability.

use super::*;
use crate::state::QueueInternalState;

fn state() -> QueueInternalState {
    QueueInternalState::new("ordering".into(), 0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn older_message_ack_obeys_selected_application_order() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let dir = keratin_log::test_dir!("older_ack_new_publish");
        let st = Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        async fn publish(st: &Stroma, id: u8) -> tokio::sync::oneshot::Receiver<std::result::Result<AppendResult, IoError>> {
            let (completion, rx) = KeratinAppendCompletion::pair();
            st.append_message_batch(
                "ordering", 0, None,
                vec![PublishItem {
                    headers: MessageHeaders {
                        published: Default::default(),
                        publish_received: Default::default(),
                        content_type: None,
                        extra: Default::default(),
                    },
                    payload: vec![id], completion, not_before: None, expire_at: None,
                }],
            ).await.unwrap();
            rx
        }
        // A real publish completes, then the ordinary delivery path leases it.
        publish(&st, 0).await.await.unwrap().unwrap();
        let delivered = st.poll_ready("ordering", 0, None, 1, unix_millis() + 60_000, u64::MAX).await.unwrap();
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].0, 0);

        let pause = Arc::new(PublishApplyPause {
            topic: "ordering", base: 1,
            entered: Notify::new(), release: Notify::new(),
        });
        *st.publish_apply_pause.lock().unwrap() = Some(pause.clone());
        let mut new_confirm = publish(&st, 1).await;
        pause.entered.notified().await;
        let qh = st.queue_handle("ordering", 0, None).await.unwrap();
        let h = qh.resolve().unwrap();
        while h.msg_log().durable_offset() < 1 || h.event_log().durable_offset() < 1 {
            tokio::task::yield_now().await;
        }

        // This is the fast ACK storage API used by the broker, for the message
        // actually delivered above. It does not ACK the paused new publish.
        let (completion, mut ack_done) = KeratinAppendCompletion::pair();
        st.ack_enqueue_many("ordering", 0, None, vec![AckEventMeta { off: 0 }], completion).await.unwrap();
        let wq = h.work_queue().unwrap();
        if cfg!(feature = "ordered-queue-apply") {
            while h.event_log().durable_offset() < 2 {
                tokio::task::yield_now().await;
            }
            assert!(matches!(ack_done.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty)));
            assert!(!wq.is_settled(0).await);
            assert!(!wq.is_ready(1).await);
            assert_eq!(h.applied_upto().load(Ordering::Acquire), 0);
            pause.release.notify_one();
            new_confirm.await.unwrap().unwrap();
            ack_done.await.unwrap().unwrap();
            assert!(wq.is_settled(0).await);
            assert!(wq.is_ready(1).await);
            assert_eq!(h.applied_upto().load(Ordering::Acquire), 2);
            st.shutdown().await.unwrap();
            return;
        }
        let ack_record = ack_done.await.unwrap().unwrap();
        // The callback reports submission, so also wait for actor-visible ACK.
        while !wq.is_settled(0).await {
            tokio::task::yield_now().await;
        }
        assert!(!wq.is_ready(1).await);
        assert!(matches!(new_confirm.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty)));
        eprintln!("ordering audit: older ACK event={}, sampled applied_upto={}, new message 1 still unapplied",
            ack_record.base_offset, h.applied_upto().load(Ordering::Acquire));

        pause.release.notify_one();
        new_confirm.await.unwrap().unwrap();
        assert!(wq.is_settled(0).await);
        assert!(wq.is_ready(1).await);
        st.shutdown().await.unwrap();
    }).await.expect("storage ordering scenario timed out");
}

#[test]
fn independent_enqueue_and_older_ack_have_equal_final_state() {
    let mut a = state();
    a.enqueue(0, 0, None);
    a.mark_inflight(0, 100);
    let mut b = a.clone();
    a.enqueue(1, 0, None);
    a.ack(0);
    b.ack(0);
    b.enqueue(1, 0, None);
    assert_eq!(a.recovery_state_digest(), b.recovery_state_digest());
}

#[test]
fn immediate_enqueue_and_ack_same_offset_converge() {
    let mut a = state();
    let mut b = state();
    a.enqueue(7, 2, Some(500));
    a.ack(7);
    b.ack(7);
    b.enqueue(7, 2, Some(500));
    assert_eq!(a.recovery_state_digest(), b.recovery_state_digest());
    assert!(b.is_settled(7));
    assert!(!b.is_ready(7));
}

#[test]
fn dependent_state_transitions_require_their_predecessors() {
    // These algebraic examples are constraints, not evidence that the broker
    // permits a consumer to settle or lease a never-delivered message.
    let mut ordered = state();
    ordered.enqueue(7, 0, None);
    ordered.mark_inflight(7, 100);
    let mut reversed = state();
    reversed.mark_inflight(7, 100);
    reversed.enqueue(7, 0, None);
    assert!(ordered.is_inflight(7));
    assert!(reversed.is_ready(7));

    let mut ordered = state();
    ordered.enqueue(7, 0, None);
    ordered.nack(7, true);
    let mut reversed = state();
    reversed.nack(7, true);
    reversed.enqueue(7, 0, None);
    assert_eq!(ordered.get_retries(7), 1);
    assert_eq!(reversed.get_retries(7), 0);

    let mut ordered = state();
    ordered.enqueue(7, 0, None);
    ordered.cancel_enqueue_many(&[7]);
    let mut reversed = state();
    reversed.cancel_enqueue_many(&[7]);
    reversed.enqueue(7, 0, None);
    assert!(!ordered.is_ready(7));
    assert!(reversed.is_ready(7));

    let mut ordered = state();
    ordered.mark_pending_dlq_many(&[7]);
    ordered.commit_dlq(7);
    let mut reversed = state();
    reversed.commit_dlq(7);
    reversed.mark_pending_dlq_many(&[7]);
    assert!(ordered.is_settled(7));
    assert!(reversed.is_pending_dlq(7));
}

#[test]
fn replaying_nack_is_not_generally_idempotent() {
    let mut once = state();
    once.enqueue(7, 0, None);
    once.mark_inflight(7, 100);
    once.nack(7, true);
    let mut twice = once.clone();
    twice.nack(7, true);
    assert_eq!(once.get_retries(7), 1);
    assert_eq!(twice.get_retries(7), 2);
}

#[test]
fn retry_policy_change_and_nack_are_order_sensitive() {
    let mut a = state();
    a.enqueue(7, 0, None);
    let mut b = a.clone();
    let policy = DeclareMeta {
        dlq_policy: None,
        dlq_max_retries: Some(0),
        default_message_ttl_ms: None,
    };
    a.apply_declare(&policy);
    a.nack(7, true);
    b.nack(7, true);
    b.apply_declare(&policy);
    assert!(a.is_pending_dlq(7));
    assert!(b.is_ready(7));
    assert_eq!(b.get_retries(7), 1);
}
