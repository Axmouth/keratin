// Uses the same sealed-page helpers as the ordinary recovery inspector tests.
fn combined_seal(head: u64, records: &[RecoveryRecord]) -> SealedReplicaFrontiers {
    let mut source = seal(0, &[], head, records);
    source.history.log_layout = QueueLogLayout::Combined;
    source.history.version = 2;
    source.history.storage_history = Some(crate::PreparedStorageHistory {
        topic: "q".into(), partition: 0, group: None, stream: false,
        binding: crate::StorageHistoryBinding {
            resource_incarnation: [1; 16], accepted_history: [2; 16], writer_session: [3; 16],
        },
        storage_instance: [4; 16],
    });
    source.history.id = [0; 32];
    let mut hash = blake3::Hasher::new();
    hash.update(b"fibril-retained-history-v1\0");
    hash.update(&rmp_serde::to_vec_named(&("q", 0u32, None::<&str>, false, &source.history)).unwrap());
    source.history.id = *hash.finalize().as_bytes();
    source
}
fn payload_event(not_before: Option<u64>) -> StromaEvent {
    StromaEvent::PayloadEnqueue {
        not_before, expire_at: Some(1000), headers: vec![], payload: vec![23; 64],
    }
}
fn combined_inspector(records: &[RecoveryRecord], limits: RecoveryInspectionLimits) -> RecoveryPairInspector {
    let source = combined_seal(0, records);
    inspector(source.clone(), source, limits)
        .with_queue_checkpoint_replay(records.len() as u64, Default::default(), 4096).unwrap()
}

#[test]
fn combined_recovery_replays_sparse_payloads_timers_and_settlement() {
    let ev = events(vec![
        payload_event(None),
        payload_event(Some(100)),
        StromaEvent::MarkInflight { off: 0, deadline: 300 },
        StromaEvent::NackMany { reqs: vec![crate::NackEventMeta { off: 0, requeue: true, not_before: Some(500) }] },
        StromaEvent::Enqueue { off: 1, retries: 0, expire_at: None },
        StromaEvent::Ack { off: 1 },
        payload_event(None),
        StromaEvent::CancelEnqueueMany { offs: vec![6] },
        StromaEvent::Enqueue { off: 0, retries: 1, expire_at: Some(1000) },
    ]);
    let mut expected = crate::QueueInternalState::new("q".into(), 0);
    expected.enqueue(0, 0, Some(1000));
    expected.enqueue_delayed(1, 100);
    expected.mark_inflight(0, 300);
    expected.nack_at(0, true, Some(500));
    expected.enqueue(1, 0, None);
    expected.ack(1);
    expected.enqueue(6, 0, Some(1000));
    expected.cancel_enqueue_many(&[6]);
    expected.enqueue(0, 1, Some(1000));
    for off in [2, 3, 4, 5, 7, 8] { expected.ack(off); }
    let check = combined_inspector(&ev, RecoveryInspectionLimits { page_records: 2, ..Default::default() });
    let done = drive_checkpoints(check, [[&[], &ev], [&[], &ev]], [&[], &[]]).unwrap();
    let (report, [a, b]) = done.finish_with_queue_artifacts(4096).unwrap();
    assert_eq!(a, b);
    assert_eq!(report.messages.left_range, (0, 0));
    assert_eq!(report.records, 4 * ev.len() as u64); // two bounded passes per replica
    assert_eq!(a.evidence().log_layout, QueueLogLayout::Combined);
    assert_eq!(a.evidence().message_next, 0); // physical compatibility lane
    assert_eq!(a.evidence().payload_log.source, RecoveryReadSource::Events);
    assert_eq!(a.evidence().payload_log.next, ev.len() as u64);
    assert_eq!(a.evidence().state_digest, expected.recovery_state_digest());
    let mut hash = blake3::Hasher::new();
    hash.update(b"fibril-recovery-live-payloads-v1\0");
    hash_record(&mut hash, &ev[0]);
    assert_eq!(a.evidence().live_payload_digest, Some(*hash.finalize().as_bytes()));
}

#[test]
fn combined_recovery_keeps_old_live_payload_across_compacted_checkpoints() {
    let ev = events(vec![
        payload_event(None), StromaEvent::Ack { off: 0 }, payload_event(None),
        StromaEvent::MarkInflight { off: 2, deadline: 300 },
        StromaEvent::Nack { off: 2, requeue: true }, payload_event(None), StromaEvent::Ack { off: 5 },
    ]);
    let mut a = crate::QueueInternalState::new("q".into(), 0);
    a.enqueue(0, 0, Some(1000)); a.ack(0); a.ack(1);
    let mut b = a.clone(); b.enqueue(2, 0, Some(1000)); b.mark_inflight(2, 300); b.ack(3);
    b.nack_at(2, true, None); b.ack(4);
    let snapshots = [checkpoint_envelope(&a, 2), checkpoint_envelope(&b, 5)];
    let sources = [checkpoint_seal(combined_seal(0, &ev), &snapshots[0]), checkpoint_seal(combined_seal(2, &ev[2..]), &snapshots[1])];
    let check = inspector(sources[0].clone(), sources[1].clone(), Default::default())
        .with_queue_checkpoint_replay(7, Default::default(), 4096).unwrap();
    let report = run_checkpoints(check, [[&[], &ev], [&[], &ev[2..]]], [&snapshots[0], &snapshots[1]]).unwrap();
    let [left, right] = report.queue_replay.unwrap();
    assert_eq!(left.state_digest, right.state_digest);
    assert_eq!(left.live_payload_digest, right.live_payload_digest);
    assert_eq!(left.payload_log.head, 0); assert_eq!(right.payload_log.head, 2);
    b.enqueue(5, 0, Some(1000)); b.ack(5); b.ack(6);
    assert_eq!(left.state_digest, b.recovery_state_digest());
}

#[test]
fn combined_recovery_rejects_forward_references_and_nonpayload_live_positions() {
    let ev = events(vec![payload_event(None), StromaEvent::Ack { off: 2 }, payload_event(None)]);
    let error = drive_checkpoints(combined_inspector(&ev, Default::default()), [[&[], &ev], [&[], &ev]], [&[], &[]]).err().unwrap();
    assert!(error.contains("forward payload reference"));
    let ev = events(vec![payload_event(None), StromaEvent::Ack { off: 0 }]);
    let mut bad = crate::QueueInternalState::new("q".into(), 0);
    bad.ack(0); bad.enqueue(1, 0, None);
    let snap = checkpoint_envelope(&bad, 2);
    let source = checkpoint_seal(combined_seal(0, &ev), &snap);
    let check = inspector(source.clone(), source, Default::default()).with_queue_checkpoint_replay(2, Default::default(), 4096).unwrap();
    let error = drive_checkpoints(check, [[&[], &ev], [&[], &ev]], [&snap, &snap]).err().unwrap();
    assert!(error.contains("non-payload combined record"));
}

#[test]
fn combined_recovery_payload_pass_is_budgeted_and_digest_checked() {
    let ev = events(vec![payload_event(None)]);
    let check = combined_inspector(&ev, RecoveryInspectionLimits { total_records: 2, ..Default::default() });
    assert!(drive_checkpoints(check, [[&[], &ev], [&[], &ev]], [&[], &[]]).is_err());
    let mut check = combined_inspector(&ev, Default::default());
    let mut corrupted = false;
    while let Some((side, request)) = check.next_read().unwrap() {
        assert_eq!(request.source, RecoveryReadSource::Events);
        let mut reply = page(&request, &ev, 1);
        if check.logs.iter().all(|l| l.next_side().is_none()) && !corrupted {
            let mut changed = payload_event(None);
            if let StromaEvent::PayloadEnqueue { payload, .. } = &mut changed { payload[0] ^= 1; }
            reply.records[0].payload = changed.encode().unwrap(); corrupted = true;
            assert!(check.accept_page(side, reply).is_err());
            break;
        }
        check.accept_page(side, reply).unwrap();
    }
    assert!(corrupted);
    assert!(check.finish_with_queue_artifacts(4096).is_err());
}
