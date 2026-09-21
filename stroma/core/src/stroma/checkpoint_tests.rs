use super::*;

async fn open(root: &Path) -> Stroma {
    Stroma::open(
        root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap()
}

async fn publish(
    st: &Stroma,
) -> tokio::sync::oneshot::Receiver<std::result::Result<AppendResult, IoError>> {
    let (completion, rx) = KeratinAppendCompletion::pair();
    st.append_message_batch(
        "q",
        0,
        None,
        vec![PublishItem {
            headers: MessageHeaders {
                published: 0,
                publish_received: 0,
                content_type: None,
                extra: Default::default(),
            },
            payload: vec![42],
            completion,
            not_before: None,
            expire_at: None,
        }],
    )
    .await
    .unwrap();
    rx
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_capture_excludes_durable_unapplied_zero_and_restart_replays_nack_once() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let dir = keratin_log::test_dir!("exact_capture_unapplied_zero");
        let st = open(&dir.root).await;
        let pause = Arc::new(PublishApplyPause {
            topic: "q",
            base: 0,
            entered: Notify::new(),
            release: Notify::new(),
        });
        *st.publish_apply_pause.lock().unwrap() = Some(pause.clone());
        let confirm = publish(&st).await;
        pause.entered.notified().await;
        let ticket = st.queue_handle("q", 0, None).await.unwrap();
        {
            let h = ticket.resolve().unwrap();
            assert_eq!(h.event_log().next_offset(), 1);
            let snapshot = h.capture_exact_checkpoint(false).await.unwrap();
            assert_eq!(snapshot.event_next, 0);
            assert!(!snapshot.state.is_ready(0));
        }
        st.snapshot_partition("q", 0, None).await.unwrap();
        let (boundary, _) = st
            .read_queue_snapshot(&st.snap_file("q", 0, None))
            .unwrap()
            .unwrap();
        assert_eq!(boundary.event_next(), 0);
        assert!(!boundary.ambiguous_zero());
        pause.release.notify_one();
        confirm.await.unwrap().unwrap();
        // Keep this zero-boundary checkpoint. The first enqueue must be replayed.
        st.shutdown().await.unwrap();
        drop(st);
        let st = open(&dir.root).await;
        let ticket = st.queue_handle("q", 0, None).await.unwrap();
        assert!(
            ticket
                .resolve()
                .unwrap()
                .work_queue()
                .unwrap()
                .is_ready(0)
                .await
        );
        let (done, rx) = KeratinAppendCompletion::pair();
        st.nack_enqueue("q", 0, None, 0, true, done).await.unwrap();
        rx.await.unwrap().unwrap();
        st.snapshot_partition("q", 0, None).await.unwrap();
        let (boundary, _) = st
            .read_queue_snapshot(&st.snap_file("q", 0, None))
            .unwrap()
            .unwrap();
        assert_eq!(boundary.event_next(), 2);
        assert!(ticket.resolve().unwrap().last_snapshot_timestamp() > 0);
        // A later enqueue is outside the checkpoint and must replay; the NACK
        // inside it must not increment retries for a second time.
        publish(&st).await.await.unwrap().unwrap();
        st.shutdown().await.unwrap();
        drop(st);
        let st = open(&dir.root).await;
        let ticket = st.queue_handle("q", 0, None).await.unwrap();
        {
            let h = ticket.resolve().unwrap();
            let wq = h.work_queue().unwrap();
            assert_eq!(wq.retries(0).await, 1);
            assert!(wq.is_ready(1).await);
            assert_eq!(h.ordered_applied_next().unwrap(), Some(3));
        }
        st.shutdown().await.unwrap();
    })
    .await
    .expect("checkpoint/restart scenario timed out");
}

#[tokio::test]
async fn cancelled_receiver_keeps_capture_fenced_until_actor_processes_command() {
    let dir = keratin_log::test_dir!("cancelled_capture_command");
    let st = open(&dir.root).await;
    let ticket = st.queue_handle("q", 0, None).await.unwrap();
    let h = ticket.resolve().unwrap();
    let order = crate::ordered_apply::OrderedApply::new(0);
    let permit = order.checkpoint().await.unwrap();
    let (response, receiver) = tokio::sync::oneshot::channel();
    let command = crate::state::QueueCommand::CaptureExactCheckpoint {
        permit,
        mark_clean: false,
        response,
    };
    drop(receiver);
    let scope = order.scope();
    let mut waiting = Box::pin(scope.enter(0, 1));
    assert!(futures::poll!(&mut waiting).is_pending());
    h.command_enqueue(command).await.unwrap();
    let turn = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap();
    crate::ordered_apply::finish(Some(turn), Some(scope)).unwrap();
    drop(h);
    st.shutdown().await.unwrap();
}

#[tokio::test]
async fn interrupted_application_survives_role_changes_and_rejects_capture() {
    let dir = keratin_log::test_dir!("interrupted_apply_roles");
    let st = open(&dir.root).await;
    let ticket = st.queue_handle("q", 0, None).await.unwrap();
    let h = ticket.resolve().unwrap();
    drop(h.ordered_apply_scope());
    h.become_follower();
    h.become_owner();
    assert!(h.ensure_owner().is_err());
    assert!(h.capture_exact_checkpoint(false).await.is_err());
    drop(h);
    st.shutdown().await.unwrap();
}

#[tokio::test]
async fn partial_application_cannot_checkpoint_and_restart_replays_from_last_good_boundary() {
    let dir = keratin_log::test_dir!("partial_apply_checkpoint_restart");
    let st = open(&dir.root).await;
    publish(&st).await.await.unwrap().unwrap();
    publish(&st).await.await.unwrap().unwrap();
    st.snapshot_partition("q", 0, None).await.unwrap();
    {
        let ticket = st.queue_handle("q", 0, None).await.unwrap();
        let h = ticket.resolve().unwrap();
        let a = StromaEvent::Nack {
            off: 0,
            requeue: true,
        };
        let b = StromaEvent::Nack {
            off: 1,
            requeue: true,
        };
        h.event_log()
            .append_batch(
                vec![event_msg(&a).unwrap(), event_msg(&b).unwrap()],
                Some(KDurability::AfterFsync),
            )
            .await
            .unwrap();
        let scope = h.ordered_apply_scope().unwrap();
        let turn = scope.enter(2, 2).await.unwrap();
        st.apply_event_inmem(a, &h).await.unwrap();
        drop(turn); // cancellation/failure after the first of two applications
        drop(scope);
        assert_eq!(h.work_queue().unwrap().retries(0).await, 1);
        assert_eq!(h.work_queue().unwrap().retries(1).await, 0);
        assert!(st.snapshot_partition("q", 0, None).await.is_err());
        h.become_follower();
        assert!(st.become_queue_owner("q", 0, None).await.is_err());
    }
    st.shutdown().await.unwrap();
    drop(st);
    let st = open(&dir.root).await;
    {
        let ticket = st.queue_handle("q", 0, None).await.unwrap();
        let h = ticket.resolve().unwrap();
        assert_eq!(h.work_queue().unwrap().retries(0).await, 1);
        assert_eq!(h.work_queue().unwrap().retries(1).await, 1);
        assert_eq!(h.ordered_applied_next().unwrap(), Some(4));
    }
    publish(&st).await.await.unwrap().unwrap();
    st.shutdown().await.unwrap();
}

#[tokio::test]
async fn event_zero_in_log_without_application_cannot_promote_or_checkpoint() {
    let dir = keratin_log::test_dir!("unapplied_zero_promotion");
    let st = open(&dir.root).await;
    let ticket = st.queue_handle("q", 0, None).await.unwrap();
    {
        let h = ticket.resolve().unwrap();
        let declare = StromaEvent::Declare(DeclareMeta {
            dlq_policy: None,
            dlq_max_retries: Some(3),
            default_message_ttl_ms: None,
        });
        h.event_log()
            .append_batch(
                vec![event_msg(&declare).unwrap()],
                Some(KDurability::AfterFsync),
            )
            .await
            .unwrap();
        assert_eq!(h.applied_upto().load(Ordering::Acquire), 0);
        assert_eq!(h.ordered_applied_next().unwrap(), Some(0));
    }
    st.become_queue_follower("q", 0, None).await.unwrap();
    assert!(matches!(
        st.promote_queue_follower_if_caught_up("q", 0, None, 0, 1)
            .await
            .unwrap(),
        QueuePromotionOutcome::EventsNotApplied { .. }
    ));
    assert!(st.become_queue_owner("q", 0, None).await.is_err());
    assert!(st.snapshot_partition("q", 0, None).await.is_err());
    st.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_snapshot_write_preserves_dirty_state_and_retry_succeeds() {
    let dir = keratin_log::test_dir!("exact_capture_write_failure");
    let st = open(&dir.root).await;
    publish(&st).await.await.unwrap().unwrap();
    let path = st.snap_file("q", 0, None);
    fs::create_dir_all(&path).unwrap(); // force rename-to-file to fail
    assert!(st.snapshot_partition("q", 0, None).await.is_err());
    let ticket = st.queue_handle("q", 0, None).await.unwrap();
    assert!(ticket.resolve().unwrap().dirty_snapshot());
    fs::remove_dir(&path).unwrap();
    st.snapshot_partition("q", 0, None).await.unwrap();
    assert!(!ticket.resolve().unwrap().dirty_snapshot());
    st.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_snapshot_caller_does_not_abandon_admitted_persistence() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let dir = keratin_log::test_dir!("cancelled_snapshot_persistence");
        let st = open(&dir.root).await;
        publish(&st).await.await.unwrap().unwrap();
        let ticket = st.queue_handle("q", 0, None).await.unwrap();
        let h = ticket.resolve().unwrap();
        let gate = h.recovery_gate.clone();
        // Hold only disk publication. The actor must still capture and release
        // application so this reaches the cancellation boundary deterministically.
        let disk = gate.snapshot_io.lock();
        let writer = st.clone();
        let caller = tokio::spawn(async move { writer.snapshot_partition("q", 0, None).await });
        while h.dirty_snapshot() {
            tokio::task::yield_now().await;
        }
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        drop(disk);
        // Taking apply admission joins the owned writer through persistence.
        let apply = h.follower_apply_state().await;
        let (boundary, _) = st
            .read_queue_snapshot(&st.snap_file("q", 0, None))
            .unwrap()
            .unwrap();
        assert_eq!(boundary.event_next(), 1);
        assert_eq!(h.ordered_applied_next().unwrap(), Some(1));
        drop(apply);
        drop(h);
        st.shutdown().await.unwrap();
    })
    .await
    .expect("cancelled snapshot persistence timed out");
}

#[tokio::test]
async fn repeated_follower_batch_skips_nack_and_rejected_overlap_does_not_advance() {
    let dir = keratin_log::test_dir!("exact_follower_overlap");
    let st = open(&dir.root).await;
    publish(&st).await.await.unwrap().unwrap();
    let (done, rx) = KeratinAppendCompletion::pair();
    st.nack_enqueue("q", 0, None, 0, true, done).await.unwrap();
    rx.await.unwrap().unwrap();
    let OwnerReplicationRead::Batch(batch) = st
        .read_owner_event_records("q", 0, None, 0, 10)
        .await
        .unwrap()
    else {
        panic!("batch")
    };
    st.become_queue_follower("q", 0, None).await.unwrap();
    let replay = batch
        .records
        .into_iter()
        .map(|(_, event)| event)
        .collect::<Vec<_>>();
    st.apply_replicated_queue_batch(
        "q",
        0,
        None,
        None,
        Some(ReplicatedEventBatch {
            epoch: batch.epoch,
            first_offset: 0,
            events: replay.clone(),
            durability: None,
        }),
    )
    .await
    .unwrap();
    let ticket = st.queue_handle("q", 0, None).await.unwrap();
    assert_eq!(
        ticket
            .resolve()
            .unwrap()
            .work_queue()
            .unwrap()
            .retries(0)
            .await,
        1
    );
    let mut suffix = replay;
    suffix.push(StromaEvent::Ack { off: 0 });
    let rejected = st
        .apply_replicated_queue_batch(
            "q",
            0,
            None,
            None,
            Some(ReplicatedEventBatch {
                epoch: batch.epoch,
                first_offset: 0,
                events: suffix,
                durability: None,
            }),
        )
        .await
        .unwrap();
    assert!(matches!(
        rejected.event_log,
        Some(ReplicatedAppendOutcome::Overlap { .. })
    ));
    assert_eq!(
        ticket.resolve().unwrap().ordered_applied_next().unwrap(),
        Some(2)
    );
    st.apply_replicated_queue_batch(
        "q",
        0,
        None,
        None,
        Some(ReplicatedEventBatch {
            epoch: batch.epoch,
            first_offset: 2,
            events: vec![StromaEvent::Ack { off: 0 }],
            durability: None,
        }),
    )
    .await
    .unwrap();
    {
        let h = ticket.resolve().unwrap();
        assert_eq!(h.ordered_applied_next().unwrap(), Some(3));
        assert!(h.work_queue().unwrap().is_settled(0).await);
    }
    st.snapshot_partition("q", 0, None).await.unwrap();
    st.become_queue_owner("q", 0, None).await.unwrap();
    publish(&st).await.await.unwrap().unwrap();
    st.shutdown().await.unwrap();
}
