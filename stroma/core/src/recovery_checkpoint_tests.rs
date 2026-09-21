fn checkpoint_envelope(state: &crate::QueueInternalState, next: u64) -> Vec<u8> {
    let blob = state.encode_snapshot(next.saturating_sub(1));
    let mut out = b"SSNAP\0\0\0".to_vec();
    out.extend_from_slice(&[0, 2, 0, 0]);
    out.extend_from_slice(&next.to_be_bytes());
    out.extend_from_slice(&(blob.len() as u32).to_be_bytes());
    out.extend(blob);
    out.extend_from_slice(&crc32c::crc32c(&out[8..]).to_be_bytes());
    out
}

#[test]
fn delayed_timer_history_replays_from_different_checkpoints_without_local_clock() {
    let msg = messages(0, 1);
    let ev = events(vec![
        StromaEvent::EnqueueDelayed {
            off: 0,
            not_before: 100,
        },
        StromaEvent::ActivateDelayed { now: 100, max: 10 },
        StromaEvent::NackMany {
            reqs: vec![crate::NackEventMeta {
                off: 0,
                requeue: true,
                not_before: Some(500),
            }],
        },
        StromaEvent::ActivateDelayed { now: 500, max: 10 },
    ]);
    let mut a = crate::QueueInternalState::new("q".into(), 0);
    a.enqueue_delayed(0, 100);
    let mut b = a.clone();
    b.activate_delayed(100, 10);
    b.mark_inflight(0, 300); // owner-local delivery
    b.nack_at(0, true, Some(500));
    let snapshots = [checkpoint_envelope(&a, 1), checkpoint_envelope(&b, 3)];
    for target in [3, 4] {
        let seals = [
            checkpoint_seal(seal(0, &msg, 1, &ev[1..]), &snapshots[0]),
            checkpoint_seal(seal(0, &msg, 3, &ev[3..]), &snapshots[1]),
        ];
        let pair = inspector(seals[0].clone(), seals[1].clone(), Default::default())
            .with_queue_checkpoint_replay(target, Default::default(), 4096)
            .unwrap();
        let report = run_checkpoints(
            pair,
            [[&msg, &ev[1..]], [&msg, &ev[3..]]],
            [&snapshots[0], &snapshots[1]],
        )
        .unwrap();
        let [left, right] = report.queue_replay.unwrap();
        assert_eq!(left.state_digest, right.state_digest);
        assert_eq!(left.live_payload_digest, right.live_payload_digest);
        let mut expected = b.clone();
        if target == 4 {
            expected.activate_delayed(500, 10);
        }
        assert_eq!(left.state_digest, expected.recovery_state_digest());
        assert_eq!(expected.get_retries(0), 1);
        assert_eq!(expected.is_ready(0), target == 4);
    }
}


fn checkpoint_seal(mut seal: SealedReplicaFrontiers, snapshot: &[u8]) -> SealedReplicaFrontiers {
    seal.history.snapshot_digest = Some(*blake3::hash(snapshot).as_bytes());
    seal.history.id = [0; 32];
    let mut hash = blake3::Hasher::new();
    hash.update(b"fibril-retained-history-v1\0");
    hash.update(
        &rmp_serde::to_vec_named(&("q", 0u32, None::<&str>, false, &seal.history)).unwrap(),
    );
    seal.history.id = *hash.finalize().as_bytes();
    seal
}

fn drive_checkpoints(
    mut inspector: RecoveryPairInspector,
    data: [[&[RecoveryRecord]; 2]; 2],
    snapshots: [&[u8]; 2],
) -> Result<RecoveryPairInspector, String> {
    while let Some((side, request)) = inspector.next_read()? {
        let i = side.index();
        let response = if request.source == RecoveryReadSource::Snapshot {
            let bytes = snapshots[i];
            let end = bytes.len() as u64;
            let next = end.min(request.from + request.max_bytes as u64);
            RecoveryReadPage {
                history_id: request.history_id,
                source: request.source,
                from: request.from,
                next,
                end,
                records: vec![],
                snapshot_bytes: bytes[request.from as usize..next as usize].to_vec(),
            }
        } else {
            let log = usize::from(request.source == RecoveryReadSource::Events);
            page(&request, data[i][log], inspector.logs[log].cursors[i].end)
        };
        inspector.accept_page(side, response)?;
    }
    Ok(inspector)
}

fn run_checkpoints(
    inspector: RecoveryPairInspector,
    data: [[&[RecoveryRecord]; 2]; 2],
    snapshots: [&[u8]; 2],
) -> Result<RecoveryPairInspection, String> {
    drive_checkpoints(inspector, data, snapshots)?.finish()
}

#[test]
fn verified_queue_artifact_preserves_state_and_is_bound_to_complete_payload_evidence() {
    let mut state = crate::QueueInternalState::new("q".into(), 0);
    state.enqueue(0, 2, Some(900));
    state.mark_inflight(0, 300);
    state.enqueue_delayed(1, 500);
    let snapshot = checkpoint_envelope(&state, 4);
    let msg = messages(0, 2);
    let source = checkpoint_seal(seal(0, &msg, 4, &[]), &snapshot);
    let build = || inspector(source.clone(), source.clone(), Default::default())
        .with_queue_checkpoint_replay(4, Default::default(), 4096).unwrap();
    assert!(build().finish_with_queue_artifacts(4096).is_err()); // incomplete reads
    let transferred = || drive_checkpoints(build(), [[&msg, &[]], [&msg, &[]]], [&snapshot, &snapshot]).unwrap();
    assert!(transferred().finish_with_queue_artifacts(0).is_err());
    assert!(transferred().finish_with_queue_artifacts(1).is_err());
    let (report, [left, right]) = transferred().finish_with_queue_artifacts(4096).unwrap();
    assert_eq!(left, right);
    assert_eq!(left.evidence().history_id, source.history.id);
    assert_eq!(left.evidence().event_next, 4);
    assert_eq!(left.evidence().live_payload_digest, report.queue_replay.unwrap()[0].live_payload_digest);
    assert_eq!(left.snapshot_digest(), *blake3::hash(left.state_snapshot()).as_bytes());
    let mut restored = crate::QueueInternalState::new("q".into(), 0);
    restored.load_snapshot(left.state_snapshot()).unwrap();
    assert!(restored.dump_inflight().is_empty());
    assert!(restored.is_ready(0));
    assert!(!restored.is_ready(1));
    assert_eq!(restored.get_retries(0), 2);
    assert_eq!(restored.collect_ttl_expired(900, 10), vec![0]);
    assert_eq!(restored.recovery_state_digest(), state.recovery_lease_normalized_digest());
    assert_eq!(transferred().finish_with_queue_artifacts(4096).unwrap().1[0], left);
    assert!(report.remaining_proofs.contains(&RecoveryProofRequirement::CommonOriginAndInstalledLineage));
}

#[test]
fn artifacts_require_live_payload_verification_even_when_plain_replay_succeeds() {
    let build = || inspector(seal(0, &[], 0, &[]), seal(0, &[], 0, &[]), Default::default());
    let unchecked = build().with_queue_replay(0, Default::default()).unwrap();
    let unchecked = drive_checkpoints(unchecked, [[&[], &[]], [&[], &[]]], [&[], &[]]).unwrap();
    assert!(unchecked.finish_with_queue_artifacts(4096).is_err());
    let checked = build().with_queue_checkpoint_replay(0, Default::default(), 4096).unwrap();
    let checked = drive_checkpoints(checked, [[&[], &[]], [&[], &[]]], [&[], &[]]).unwrap();
    let (_, [artifact, _]) = checked.finish_with_queue_artifacts(4096).unwrap();
    assert_eq!(artifact.evidence().event_next, 0);
    assert_eq!(artifact.evidence().required_message_next, 0);
}

#[test]
fn unequal_checkpoints_preserve_retry_count_and_compare_live_payloads_and_lease_projection() {
    let msg = messages(0, 3);
    let events = events(vec![
        StromaEvent::Enqueue {
            off: 0,
            retries: 0,
            expire_at: None,
        },
        StromaEvent::Enqueue {
            off: 1,
            retries: 0,
            expire_at: Some(900),
        },
        StromaEvent::Ack { off: 0 },
        StromaEvent::Nack {
            off: 1,
            requeue: true,
        },
        StromaEvent::Enqueue {
            off: 2,
            retries: 0,
            expire_at: None,
        },
    ]);
    let mut a = crate::QueueInternalState::new("q".into(), 0);
    a.enqueue(0, 0, None);
    a.enqueue(1, 0, Some(900));
    a.ack(0);
    a.mark_inflight(1, 500);
    let mut b = a.clone();
    b.nack(1, true);
    b.enqueue(2, 0, None);
    b.mark_inflight(2, 700);
    let snapshots = [checkpoint_envelope(&a, 3), checkpoint_envelope(&b, 5)];
    let seals = [
        checkpoint_seal(seal(0, &msg, 3, &events[3..]), &snapshots[0]),
        checkpoint_seal(seal(1, &msg[1..], 5, &[]), &snapshots[1]),
    ];
    let pair = inspector(
        seals[0].clone(),
        seals[1].clone(),
        RecoveryInspectionLimits {
            page_records: 1,
            page_bytes: 45,
            ..Default::default()
        },
    )
    .with_queue_checkpoint_replay(5, Default::default(), 4096)
    .unwrap();
    let report = run_checkpoints(
        pair,
        [[&msg, &events[3..]], [&msg[1..], &[]]],
        [&snapshots[0], &snapshots[1]],
    )
    .unwrap();
    assert!(
        report
            .remaining_proofs
            .contains(&RecoveryProofRequirement::CommonOriginAndInstalledLineage)
    );
    let [left, right] = report.queue_replay.unwrap();
    assert_eq!(left.checkpoint_event_next, Some(3));
    assert_eq!(right.checkpoint_event_next, Some(5));
    assert_ne!(left.state_digest, right.state_digest); // owner-local lease
    assert_eq!(
        left.lease_normalized_state_digest,
        right.lease_normalized_state_digest
    );
    assert_eq!(left.live_payload_digest, right.live_payload_digest);
    assert_ne!(left.message_digest, right.message_digest); // obsolete payload zero
    assert_ne!(left.event_prefix_digest, right.event_prefix_digest); // different starts
}

#[test]
fn same_checkpoint_state_does_not_hide_changed_live_payload_or_missing_payload() {
    let mut state = crate::QueueInternalState::new("q".into(), 0);
    state.enqueue(1, 0, None);
    let snapshot = checkpoint_envelope(&state, 1);
    let msg = messages(1, 2);
    let mut changed = msg.clone();
    changed[0].payload.push(9);
    for right in [&changed[..], &[][..]] {
        let right_head = if right.is_empty() { 2 } else { 1 };
        let a = checkpoint_seal(seal(1, &msg, 1, &[]), &snapshot);
        let b = checkpoint_seal(seal(right_head, right, 1, &[]), &snapshot);
        let pair = inspector(a, b, Default::default())
            .with_queue_checkpoint_replay(1, Default::default(), 4096)
            .unwrap();
        let report = run_checkpoints(pair, [[&msg, &[]], [right, &[]]], [&snapshot, &snapshot]);
        if right.is_empty() {
            assert!(report.unwrap_err().contains("coverage"));
        } else {
            let [a, b] = report.unwrap().queue_replay.unwrap();
            assert_eq!(
                a.lease_normalized_state_digest,
                b.lease_normalized_state_digest
            );
            assert_ne!(a.live_payload_digest, b.live_payload_digest);
        }
    }
}

#[test]
fn checkpoint_replay_rejects_legacy_boundary_tampering_missing_suffix_and_budgets() {
    let state = crate::QueueInternalState::new("q".into(), 0);
    let original = checkpoint_envelope(&state, 0);
    for case in 0..6 {
        let mut snapshot = original.clone();
        let mut limits = RecoveryInspectionLimits::default();
        let mut replay_limits = RecoveryReplayLimits::default();
        let mut max_bytes = 4096;
        let mut target = 0;
        if case == 0 {
            snapshot[9] = 1;
        } // valid hash/CRC but ambiguous old format
        if case == 1 {
            snapshot[19] = 2;
            target = 2;
        } // body says event zero
        if case <= 1 {
            let len = snapshot.len();
            let crc = crc32c::crc32c(&snapshot[8..len - 4]);
            snapshot[len - 4..].copy_from_slice(&crc.to_be_bytes());
        }
        if case == 2 {
            limits.total_bytes = 10;
        }
        if case == 3 {
            replay_limits.operations_per_replica = 1;
        }
        if case == 4 {
            max_bytes = 28;
        }
        let sealed = checkpoint_seal(seal(0, &[], target, &[]), &snapshot);
        if case == 5 {
            snapshot[24] ^= 1;
        } // tampered bytes after sealing
        let pair = inspector(sealed.clone(), sealed, limits)
            .with_queue_checkpoint_replay(target, replay_limits, max_bytes)
            .unwrap();
        assert!(
            run_checkpoints(pair, [[&[], &[]], [&[], &[]]], [&snapshot, &snapshot]).is_err(),
            "case {case}"
        );
    }
}

#[test]
fn lease_projection_preserves_retry_delay_ttl_and_dlq_differences() {
    let mut base = crate::QueueInternalState::new("q".into(), 0);
    base.enqueue(0, 0, Some(500));
    let mut leased = base.clone();
    leased.mark_inflight(0, 200);
    assert_eq!(
        base.recovery_lease_normalized_digest(),
        leased.recovery_lease_normalized_digest()
    );
    let mut retry = base.clone();
    retry.nack(0, true);
    let mut ttl = crate::QueueInternalState::new("q".into(), 0);
    ttl.enqueue(0, 0, Some(501));
    let mut dlq = base.clone();
    dlq.mark_pending_dlq_many(&[0]);
    for changed in [retry, ttl, dlq] {
        assert_ne!(
            base.recovery_lease_normalized_digest(),
            changed.recovery_lease_normalized_digest()
        );
    }
}
