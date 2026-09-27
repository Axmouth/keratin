use super::*;

async fn open(root: &Path) -> Stroma {
    let mut cfg = KeratinConfig::test_default();
    cfg.segment_max_bytes = 4096;
    cfg.tail_cache_bytes = 0;
    Stroma::open_with_options(
        root,
        StromaKeratinConfig::from_message_log(cfg),
        SnapshotConfig {
            every_events: u64::MAX,
        },
        StromaOptions {
            combined_queue_log: true,
            ..Default::default()
        },
    )
    .await
    .unwrap()
}
fn headers() -> MessageHeaders {
    MessageHeaders {
        published: 0,
        publish_received: 0,
        content_type: None,
        extra: Default::default(),
    }
}
async fn publish(st: &Stroma, delayed: bool) -> u64 {
    let (completion, rx) = KeratinAppendCompletion::pair();
    st.append_message_batch(
        "q",
        0,
        None,
        vec![PublishItem {
            headers: headers(),
            payload: vec![42; 2048],
            completion,
            not_before: delayed.then_some(u64::MAX - 1),
            expire_at: None,
        }],
    )
    .await
    .unwrap();
    rx.await.unwrap().unwrap().base_offset
}
async fn ack(st: &Stroma, off: u64) {
    let (completion, rx) = KeratinAppendCompletion::pair();
    st.ack_enqueue("q", 0, None, off, completion).await.unwrap();
    rx.await.unwrap().unwrap();
}

#[test]
fn payload_codec_is_bounded_and_offset_is_implicit() {
    for delayed in [None, Some(12345)] {
        let event = StromaEvent::PayloadEnqueue {
            not_before: delayed,
            expire_at: Some(23456),
            headers: vec![1, 2, 3],
            payload: vec![7; 65536],
        };
        let bytes = event.encode().unwrap();
        assert_eq!(StromaEvent::decode(&bytes).unwrap(), event);
        for len in [0, 8, 12, 13, 20, 30, bytes.len() - 1] {
            assert!(StromaEvent::decode(&bytes[..len]).is_err());
        }
        let state = event.into_record_state(0);
        assert_eq!(state.max_referenced_msg_offset(), Some(0));
    }
    let event = StromaEvent::PayloadEnqueue {
        not_before: None,
        expire_at: None,
        headers: vec![],
        payload: vec![],
    };
    let mut bytes = event.encode().unwrap();
    assert_eq!(StromaEvent::decode(&bytes).unwrap(), event);
    // Both absent options occupy one byte, followed by the header byte length.
    bytes[14..18].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(StromaEvent::decode(&bytes).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sparse_payloads_and_metadata_frontier_survive_replay() {
    let dir = keratin_log::test_dir!("combined_sparse_replay");
    let st = open(&dir.root).await;
    assert_eq!(publish(&st, false).await, 0);
    assert_eq!(publish(&st, true).await, 1);
    ack(&st, 0).await; // State record 2, no payload.
    assert_eq!(publish(&st, false).await, 3);
    assert_eq!(publish(&st, false).await, 4);
    let q = st.queue_handle("q", 0, None).await.unwrap();
    {
        let h = q.resolve().unwrap();
        assert!(h.optional_message_log().is_none());
        assert_eq!(h.work_queue().unwrap().settled_until().await, 1);
        for budget in [1, 1024, 1024 * 1024, 8 * 1024 * 1024] {
            // Cross chunk and segment boundaries, skip a delayed payload and
            // metadata, and preserve exact selection and retry identity.
            let selected = read_selected_with_budget(&h, &[(0, 0), (3, 1)], budget).unwrap();
            assert_eq!(selected.iter().map(|m| m.0).collect::<Vec<_>>(), [0, 3]);
            assert_eq!(selected[1].2, vec![42; 2048]);
            assert_eq!(selected[1].3, 1);
            assert!(read_selected_with_budget(&h, &[(2, 0)], budget).is_err());
            assert!(read_selected_with_budget(&h, &[(5, 0)], budget).is_err());
        }
        assert!(read_selected(&h, &[(2, 0)]).is_err());
        assert!(fetch_payload(&h, 2).unwrap().is_none());
        assert_eq!(scan_payloads(&h, 0, 10).unwrap().len(), 4);
    }
    st.shutdown().await.unwrap();
    drop(q);
    drop(st);
    let st = open(&dir.root).await;
    let q = st.queue_handle("q", 0, None).await.unwrap();
    let h = q.resolve().unwrap();
    assert_eq!(h.work_queue().unwrap().settled_until().await, 1);
    assert_eq!(h.work_queue().unwrap().lowest_not_settled_offset().await, 1);
    assert_eq!(
        st.poll_ready("q", 0, None, 10, u64::MAX - 1, u64::MAX)
            .await
            .unwrap()
            .iter()
            .map(|m| m.0)
            .collect::<Vec<_>>(),
        [3, 4]
    );
    drop(h);
    st.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_payload_pins_retention_below_snapshot_replay_boundary() {
    let dir = keratin_log::test_dir!("combined_retention");
    let st = open(&dir.root).await;
    assert_eq!(publish(&st, true).await, 0);
    for _ in 0..12 {
        let off = publish(&st, false).await;
        ack(&st, off).await;
    }
    let q = st.queue_handle("q", 0, None).await.unwrap();
    st.write_exact_queue_checkpoint(q.clone(), true)
        .await
        .unwrap();
    let (boundary, _) = st
        .read_queue_snapshot(&st.snap_file("q", 0, None))
        .unwrap()
        .unwrap();
    let snapshot_next = boundary.event_next();
    assert!(snapshot_next > 12);
    assert_eq!(
        st.truncate_messages_before("q", 0, None, u64::MAX)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        st.truncate_partition_log(q.clone(), u64::MAX)
            .await
            .unwrap(),
        0
    );
    assert!(fetch_payload(&q.resolve().unwrap(), 0).unwrap().is_some());
    assert!(q.resolve().unwrap().optional_message_log().is_none());
    assert_eq!(q.resolve().unwrap().physical_logs().count(), 1);
    st.shutdown().await.unwrap();
    drop(q);
    drop(st);
    let st = open(&dir.root).await;
    let q = st.queue_handle("q", 0, None).await.unwrap();
    assert!(
        st.recovery_event_scan_starts
            .lock()
            .unwrap()
            .contains(&snapshot_next)
    );
    assert!(fetch_payload(&q.resolve().unwrap(), 0).unwrap().is_some());
    assert_eq!(
        q.resolve()
            .unwrap()
            .work_queue()
            .unwrap()
            .lowest_not_settled_offset()
            .await,
        0
    );
    ack(&st, 0).await;
    // A new snapshot makes every old record reclaimable. Reopen still works
    // with a nonzero head and the next publish keeps its physical identity.
    st.write_exact_queue_checkpoint(q.clone(), true)
        .await
        .unwrap();
    assert!(q.resolve().unwrap().event_log().head_offset() > 0);
    let next = q.resolve().unwrap().event_log().next_offset();
    st.shutdown().await.unwrap();
    drop(q);
    drop(st);
    let st = open(&dir.root).await;
    assert_eq!(publish(&st, false).await, next);
    assert_eq!(
        st.poll_ready("q", 0, None, 1, u64::MAX - 1, u64::MAX)
            .await
            .unwrap()[0]
            .0,
        next
    );
    st.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_only_follower_keeps_payloads_and_overlap_does_not_repeat_nack() {
    let dir = keratin_log::test_dir!("combined_follower");
    let st = open(&dir.root).await;
    st.become_queue_follower_with_epoch("q", 0, None, 1)
        .await
        .unwrap();
    let events = vec![
        StromaEvent::PayloadEnqueue {
            not_before: None,
            expire_at: None,
            headers: headers().encode().unwrap(),
            payload: vec![42],
        },
        StromaEvent::Nack {
            off: 0,
            requeue: true,
        },
    ];
    for _ in 0..2 {
        st.apply_replicated_queue_batch(
            "q",
            0,
            None,
            None,
            Some(ReplicatedEventBatch {
                epoch: 1,
                first_offset: 0,
                events: events.clone(),
                durability: Some(KDurability::AfterFsync),
            }),
        )
        .await
        .unwrap();
    }
    let q = st.queue_handle("q", 0, None).await.unwrap();
    let h = q.resolve().unwrap();
    assert_eq!(h.work_queue().unwrap().retries(0).await, 1);
    assert_eq!(fetch_payload(&h, 0).unwrap().unwrap().payload, [42]);
    assert_eq!(
        st.queue_replication_next_offsets("q", 0, None)
            .await
            .unwrap(),
        (0, 2)
    );
    drop(h);
    st.shutdown().await.unwrap();
}

#[test]
fn format_marker_requires_explicit_mode_and_fresh_storage() {
    let dir = keratin_log::test_dir!("combined_marker");
    prepare_root(&dir.root, true).unwrap();
    assert!(prepare_root(&dir.root, false).is_err());
    prepare_root(&dir.root, true).unwrap();
    let other = dir.root.join("legacy");
    std::fs::create_dir(&other).unwrap();
    std::fs::write(other.join("existing"), b"data").unwrap();
    assert!(prepare_root(&other, true).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn activation_uses_original_payload_and_cancelled_receiver_keeps_admitted_work() {
    let dir = keratin_log::test_dir!("combined_activation_cancel");
    let st = open(&dir.root).await;
    let off = publish(&st, true).await;
    st.append_events_durable(
        "q",
        0,
        None,
        vec![StromaEvent::ActivateDelayed {
            now: u64::MAX - 1,
            max: 1,
        }],
        KDurability::AfterFsync,
    )
    .await
    .unwrap();
    let delivered = st
        .poll_ready("q", 0, None, 1, u64::MAX - 1, u64::MAX)
        .await
        .unwrap();
    assert_eq!(delivered[0].0, off);
    assert_eq!(delivered[0].2, vec![42; 2048]);
    let (completion, rx) = KeratinAppendCompletion::pair();
    drop(rx);
    st.append_message_batch(
        "q",
        0,
        None,
        vec![PublishItem {
            headers: headers(),
            payload: vec![8],
            completion,
            not_before: None,
            expire_at: None,
        }],
    )
    .await
    .unwrap();
    st.shutdown().await.unwrap();
    drop(st);
    let st = open(&dir.root).await;
    let q = st.queue_handle("q", 0, None).await.unwrap();
    let h = q.resolve().unwrap();
    assert_eq!(
        scan_payloads(&h, 0, 10)
            .unwrap()
            .iter()
            .map(|r| r.0)
            .collect::<Vec<_>>(),
        [0, 2]
    );
    assert!(h.work_queue().unwrap().is_ready(2).await);
    drop(h);
    st.shutdown().await.unwrap();
}

#[cfg(feature = "io-fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_combined_fsync_never_confirms_or_applies_payload() {
    use keratin_log::io_fault_injection::{Fault, arm};
    for fault in [Fault::Fsync, Fault::FsyncAfterSuccess] {
        let dir = keratin_log::test_dir!("combined_fsync_failure");
        let st = open(&dir.root).await;
        let q = st.queue_handle("q", 0, None).await.unwrap();
        let path = st
            .tp_part_dir("q", 0, None)
            .join("segments/00000000000000000000.log");
        let armed = arm(&path, fault).unwrap();
        let (completion, rx) = KeratinAppendCompletion::pair();
        st.append_message_batch(
            "q",
            0,
            None,
            vec![PublishItem {
                headers: headers(),
                payload: vec![7],
                completion,
                not_before: None,
                expire_at: None,
            }],
        )
        .await
        .unwrap();
        assert!(rx.await.unwrap().is_err());
        assert_eq!(armed.hits(), 1);
        assert!(!q.resolve().unwrap().work_queue().unwrap().is_ready(0).await);
        drop(armed);
        let _ = st.shutdown().await;
    }
}

#[tokio::test]
async fn combined_writer_uses_payload_budgets_and_read_cap_allows_one_oversized_record() {
    let dir = keratin_log::test_dir!("combined_payload_budgets");
    let mut message = KeratinConfig::test_default();
    message.tail_cache_bytes = 8 * 1024 * 1024;
    message.segment_max_bytes = 32 * 1024 * 1024;
    let mut config = StromaKeratinConfig::from_message_log(message);
    config.event_log.tail_cache_bytes = 0;
    config.event_log.default_durability = KDurability::AfterFsync;
    config.message_log.default_durability = KDurability::AfterWrite;
    let st = Stroma::open_with_options(
        &dir.root,
        config,
        SnapshotConfig::default(),
        StromaOptions {
            combined_queue_log: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let effective = st.queue_event_log_config();
    assert_eq!(effective.segment_max_bytes, message.segment_max_bytes);
    assert_eq!(effective.max_batch_bytes, message.max_batch_bytes);
    assert_eq!(effective.flush_target_bytes, message.flush_target_bytes);
    assert_eq!(effective.index_stride_bytes, message.index_stride_bytes);
    assert_eq!(effective.tail_cache_bytes, message.tail_cache_bytes);
    assert_eq!(effective.max_batch_records, message.max_batch_records);
    assert_eq!(effective.default_durability, KDurability::AfterFsync);
    // The small metadata profile is still retained independently.
    assert_eq!(st.keratin_cfg_event.tail_cache_bytes, 0);
    assert_eq!(
        st.keratin_cfg_event.max_batch_bytes,
        message.max_batch_bytes / 8
    );
    publish(&st, false).await;
    publish(&st, false).await;
    let read = st
        .read_owner_event_records_with_byte_budget("q", 0, None, 0, 10, 1024)
        .await
        .unwrap();
    let crate::OwnerReplicationRead::Batch(batch) = read else {
        panic!("unexpected checkpoint")
    };
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.next_offset, 1);
    let read = st
        .read_owner_event_records_with_byte_budget("q", 0, None, 1, 10, 1024)
        .await
        .unwrap();
    let crate::OwnerReplicationRead::Batch(batch) = read else {
        panic!("unexpected checkpoint")
    };
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].0, 1);
    assert_eq!(batch.next_offset, 2);
    st.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_log_lifecycle_and_empty_replication_lane_are_explicit() {
    use crate::OwnerReplicationRead;
    let dir = keratin_log::test_dir!("combined_single_log_lifecycle");
    let st = open(&dir.root).await;
    let q = st.queue_handle("q", 0, None).await.unwrap();
    let h = q.resolve().unwrap();
    assert!(h.optional_message_log().is_none());
    assert!(h.msg_log().is_err());
    assert_eq!(h.physical_logs().count(), 1);
    assert!(!st.msg_tp_part_dir("q", 0, None).exists());
    st.ensure_queue_owner_epoch("q", 0, None, Some(7))
        .await
        .unwrap();
    let OwnerReplicationRead::Batch(lane) = st
        .read_owner_message_records("q", 0, None, 0, 100)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        (lane.epoch, lane.requested_offset, lane.next_offset),
        (7, 0, 0)
    );
    assert!(lane.records.is_empty());
    assert!(
        st.read_owner_message_records("q", 0, None, 1, 100)
            .await
            .is_err()
    );
    publish(&st, false).await;
    st.become_queue_follower("q", 0, None).await.unwrap();
    assert!(
        st.read_owner_message_records("q", 0, None, 0, 100)
            .await
            .is_err()
    );
    assert_eq!(
        st.queue_replication_next_offsets("q", 0, None)
            .await
            .unwrap(),
        (0, 1)
    );
    // A malformed absent-lane frame is refused before poisoning follower apply.
    assert!(
        st.apply_replicated_queue_batch(
            "q",
            0,
            None,
            Some(crate::ReplicatedMessageBatch {
                epoch: 6,
                first_offset: 0,
                records: vec![],
                durability: Some(KDurability::AfterFsync),
            }),
            None
        )
        .await
        .is_err()
    );
    st.become_queue_owner_with_epoch("q", 0, None, 8)
        .await
        .unwrap();
    assert_eq!(h.message_lane_epoch(), 8);
    assert!(st.advance_queue_epoch("q", 0, None, 7).await.is_err());
    drop(h);
    drop(q);
    st.shutdown().await.unwrap();
    drop(st);
    let st = open(&dir.root).await;
    let q = st.queue_handle("q", 0, None).await.unwrap();
    let h = q.resolve().unwrap();
    assert!(h.optional_message_log().is_none());
    assert_eq!(h.message_lane_epoch(), 8);
    assert_eq!(
        fetch_payload(&h, 0).unwrap().unwrap().payload,
        vec![42; 2048]
    );
    drop(h);
    st.shutdown().await.unwrap();
}

#[cfg(all(
    feature = "early-replication-experiment",
    feature = "io-fault-injection"
))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn early_combined_follower_durability_never_substitutes_for_owner_fsync() {
    use crate::{OwnerReplicationRead, ReplicatedEventBatch};
    use keratin_log::io_fault_injection::{Fault, arm_gated};
    for prefix in [0, 1] {
        for fault in [Fault::FsyncPause, Fault::Fsync, Fault::FsyncAfterSuccess] {
            let owner_dir = keratin_log::test_dir!("combined_early_owner");
            let follower_dir = keratin_log::test_dir!("combined_early_follower");
            let mut stores = Vec::new();
            for dir in [&owner_dir.root, &follower_dir.root] {
                let cfg = KeratinConfig {
                    tail_cache_bytes: 8 * 1024 * 1024,
                    flush_target_bytes: 8,
                    ..KeratinConfig::test_default()
                };
                stores.push(
                    Stroma::open_with_options(
                        dir,
                        StromaKeratinConfig::from_message_log(cfg),
                        SnapshotConfig {
                            every_events: u64::MAX,
                        },
                        StromaOptions {
                            combined_queue_log: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap(),
                );
            }
            for st in &stores {
                st.initialize_empty_storage_history(
                    "q",
                    0,
                    None,
                    PartitionKind::Queue,
                    crate::StorageHistoryBinding {
                        resource_incarnation: [1; 16],
                        accepted_history: [2; 16],
                        writer_session: [3; 16],
                    },
                )
                .await
                .unwrap();
                assert!(!st.msg_tp_part_dir("q", 0, None).exists());
            }
            let owner = &stores[0];
            let follower = &stores[1];
            let q = owner.queue_handle("q", 0, None).await.unwrap();
            let h = q.resolve().unwrap();
            if prefix == 1 {
                assert_eq!(publish(owner, false).await, 0);
            }
            follower.become_queue_follower("q", 0, None).await.unwrap();
            let armed = arm_gated(
                &owner
                    .tp_part_dir("q", 0, None)
                    .join("segments/00000000000000000000.log"),
                fault,
            )
            .unwrap();
            let (completion, mut rx) = KeratinAppendCompletion::pair();
            owner
                .append_message_batch(
                    "q",
                    0,
                    None,
                    vec![PublishItem {
                        headers: headers(),
                        payload: vec![73; 4096],
                        completion,
                        not_before: None,
                        expire_at: None,
                    }],
                )
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while armed.hits() == 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
            assert!(matches!(
                rx.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));
            assert!(!h.work_queue().unwrap().is_ready(prefix).await);
            let OwnerReplicationRead::Batch(durable) = owner
                .read_owner_event_records("q", 0, None, 0, 100)
                .await
                .unwrap()
            else {
                panic!()
            };
            assert_eq!(durable.next_offset, prefix);
            let OwnerReplicationRead::Batch(tentative) = owner
                .read_owner_event_records_early("q", 0, None, 0, 100)
                .await
                .unwrap()
            else {
                panic!()
            };
            assert_eq!(tentative.next_offset, prefix + 1);
            follower
                .apply_open_replicated_queue_batch(
                    "q",
                    0,
                    None,
                    Box::pin(futures::stream::iter([Ok((
                        None,
                        Some(ReplicatedEventBatch {
                            epoch: tentative.epoch,
                            first_offset: 0,
                            events: tentative
                                .records
                                .into_iter()
                                .map(|(_, event)| event)
                                .collect(),
                            durability: Some(KDurability::AfterFsync),
                        }),
                    ))])),
                )
                .await
                .unwrap();
            let fq = follower.queue_handle("q", 0, None).await.unwrap();
            let fh = fq.resolve().unwrap();
            assert_eq!(fh.event_log().next_offset(), prefix + 1);
            assert_eq!(
                fetch_payload(&fh, prefix).unwrap().unwrap().payload,
                vec![73; 4096]
            );
            assert!(matches!(
                rx.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));
            assert!(!h.work_queue().unwrap().is_ready(prefix).await);
            armed.release();
            let result = tokio::time::timeout(Duration::from_secs(5), rx)
                .await
                .unwrap()
                .unwrap();
            if matches!(fault, Fault::FsyncPause) {
                assert_eq!(result.unwrap().base_offset, prefix);
                assert!(h.work_queue().unwrap().is_ready(prefix).await);
            } else {
                assert!(result.is_err());
                assert!(!h.work_queue().unwrap().is_ready(prefix).await);
                let (completion, rx) = KeratinAppendCompletion::pair();
                let result = owner
                    .append_message_batch(
                        "q",
                        0,
                        None,
                        vec![PublishItem {
                            headers: headers(),
                            payload: vec![99],
                            completion,
                            not_before: None,
                            expire_at: None,
                        }],
                    )
                    .await;
                if result.is_ok() {
                    assert!(
                        tokio::time::timeout(Duration::from_secs(5), rx)
                            .await
                            .unwrap()
                            .unwrap()
                            .is_err()
                    );
                }
            }
            drop(armed);
            drop(fh);
            drop(fq);
            drop(h);
            drop(q);
            let _ = owner.shutdown().await;
            follower.shutdown().await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_recovery_seal_and_two_pass_reads_survive_cold_reopen() {
    use crate::recovery_inspection::{RecoveryInspectionLimits, RecoveryPairInspector};
    use crate::{RecoveryReadSource, RecoverySealRequest, QueueLogLayout};
    for snapshot in [false, true] {
        let dir = keratin_log::test_dir!("combined_recovery_cold");
        let mut st = open(&dir.root).await;
        st.initialize_empty_storage_history("q", 0, None, PartitionKind::Queue, crate::StorageHistoryBinding {
            resource_incarnation: [1; 16], accepted_history: [2; 16], writer_session: [3; 16],
        }).await.unwrap();
        assert_eq!(publish(&st, false).await, 0);
        assert_eq!(publish(&st, true).await, 1);
        ack(&st, 0).await;
        if snapshot { st.snapshot_partition("q", 0, None).await.unwrap(); }
        assert_eq!(publish(&st, false).await, 3);
        ack(&st, 3).await;
        let seal = RecoverySealRequest { transition: [9; 32], fence_epoch: 8 };
        let sealed = st.seal_replica_for_recovery("q", 0, None, seal.clone()).await.unwrap();
        assert_eq!(sealed.history.log_layout, QueueLogLayout::Combined);
        assert_eq!((sealed.message_next, sealed.event_next), (0, 5));
        assert_eq!(sealed.history.snapshot_digest.is_some(), snapshot);
        for cold in [false, true] {
            if cold {
                st.shutdown().await.unwrap();
                drop(st);
                st = open(&dir.root).await;
            }
            assert!(st.queue_handle("q", 0, None).await.is_err());
            assert!(!st.msg_tp_part_dir("q", 0, None).exists());
            assert_eq!(st.seal_replica_for_recovery("q", 0, None, seal.clone()).await.unwrap(), sealed);
            let mut check = RecoveryPairInspector::new("q", 0, None, false, sealed.clone(), sealed.clone(),
                RecoveryInspectionLimits { page_records: 2, page_bytes: 4096, ..Default::default() }).unwrap()
                .with_queue_checkpoint_replay(5, Default::default(), 4096).unwrap();
            let mut cursors = [None, None];
            let mut repeated = [0, 0];
            while let Some((side, request)) = check.next_read().unwrap() {
                let i = match side { crate::recovery_inspection::RecoverySide::Left => 0, crate::recovery_inspection::RecoverySide::Right => 1 };
                assert_ne!(request.source, RecoveryReadSource::Messages);
                if request.source == RecoveryReadSource::Events && request.from == sealed.event_head { repeated[i] += 1; }
                let strict = st.read_sealed_replica("q", 0, None, PartitionKind::Queue, request.clone()).await.unwrap();
                let (page, cursor) = st.read_sealed_replica_sequential("q", 0, None, PartitionKind::Queue, request, cursors[i].take()).await.unwrap();
                assert_eq!(strict, page);
                cursors[i] = cursor;
                check.accept_page(side, page).unwrap();
            }
            assert_eq!(repeated, [2, 2]);
            let (_, [a, b]) = check.finish_with_queue_artifacts(4096).unwrap();
            assert_eq!(a, b);
            let mut state = crate::QueueInternalState::new("q".into(), 0);
            state.load_snapshot(a.state_snapshot()).unwrap();
            assert!(!state.is_ready(1));
            assert_eq!(state.recovery_live_ranges().iter().cloned().collect::<Vec<_>>(), vec![1..2]);
            assert_eq!(a.evidence().payload_log.next, 5);
            drop(cursors);
        }
        st.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_recovery_stage_install_preserves_payloads_and_retry_progress() {
    use crate::recovery_inspection::{RecoveryInspectionLimits, RecoveryPairInspector};
    use crate::{QueueRecoveryStageSpec, RecoveryReadRequest, RecoveryReadSource, RecoverySealRequest, StorageHistoryBinding};
    for reuse in [false, true] {
        let dir = keratin_log::test_dir!("combined_recovery_install");
        let source = open(&dir.root.join("source")).await;
        source.initialize_empty_storage_history("q", 0, None, PartitionKind::Queue, StorageHistoryBinding {
            resource_incarnation: [1; 16], accepted_history: [2; 16], writer_session: [3; 16],
        }).await.unwrap();
        assert_eq!(publish(&source, false).await, 0);
        assert_eq!(publish(&source, true).await, 1);
        ack(&source, 0).await;
        source.snapshot_partition("q", 0, None).await.unwrap();
        assert_eq!(publish(&source, false).await, 3);
        let seal = RecoverySealRequest { transition: [9; 32], fence_epoch: 8 };
        let sealed = source.seal_replica_for_recovery("q", 0, None, seal.clone()).await.unwrap();
        let mut check = RecoveryPairInspector::new("q", 0, None, false, sealed.clone(), sealed.clone(),
            RecoveryInspectionLimits { page_records: 2, page_bytes: 4096, ..Default::default() }).unwrap()
            .with_queue_checkpoint_replay(4, Default::default(), 4096).unwrap();
        while let Some((side, request)) = check.next_read().unwrap() {
            let page = source.read_sealed_replica("q", 0, None, PartitionKind::Queue, request).await.unwrap();
            check.accept_page(side, page).unwrap();
        }
        let (_, [artifact, _]) = check.finish_with_queue_artifacts(4096).unwrap();
        let spec = QueueRecoveryStageSpec {
            plan: [8; 32], topic: "q".into(), partition: 0, group: None,
            binding: StorageHistoryBinding { resource_incarnation: [1; 16], accepted_history: [4; 16], writer_session: [5; 16] },
            fence_epoch: 8, source_history: sealed.history.id,
            payload_log: sealed.history.payload_log(), message_head: 0, message_next: 0, event_next: 4,
            message_digest: sealed.history.message_digest, snapshot_digest: artifact.snapshot_digest(),
            state_digest: artifact.evidence().lease_normalized_state_digest,
            live_payload_digest: artifact.evidence().live_payload_digest.unwrap(),
        };
        let target = if reuse { source.clone() } else { open(&dir.root.join("target")).await };
        // A snapshot behind the copied suffix must never authorize installation.
        let mut wrong = spec.clone();
        wrong.event_next -= 1;
        assert!(target.open_queue_recovery_stage(wrong, artifact.state_snapshot().to_vec(), Default::default()).await.is_err());
        let mut stage = if reuse {
            target.open_queue_recovery_stage_reusing(spec.clone(), artifact.state_snapshot().to_vec(), Default::default(), seal.clone()).await.unwrap()
        } else {
            target.open_queue_recovery_stage(spec.clone(), artifact.state_snapshot().to_vec(), Default::default()).await.unwrap()
        };
        assert_eq!(stage.next_offset().await, if reuse { 4 } else { 0 });
        if !reuse {
            let mut from = 0;
            while from < spec.payload_log.next {
                let page = source.read_sealed_replica("q", 0, None, PartitionKind::Queue, RecoveryReadRequest {
                    seal: seal.clone(), history_id: sealed.history.id, source: RecoveryReadSource::Events,
                    from, max_records: 2, max_bytes: 4096,
                }).await.unwrap();
                let next = page.next;
                assert!(next > from);
                assert_eq!(stage.append(page.clone()).await.unwrap(), next);
                assert_eq!(stage.append(page).await.unwrap(), next);
                if from == 0 {
                    drop(stage);
                    stage = target.resume_queue_recovery_stage(spec.clone(), Default::default()).await.unwrap();
                    assert_eq!(stage.next_offset().await, next);
                }
                from = next;
            }
        }
        let receipt = stage.finish().await.unwrap();
        assert_eq!((receipt.message_next, receipt.event_next), (0, 4));
        let page = stage.read_completed_messages(0, 10, 16384).await.unwrap();
        assert_eq!((page.source, page.end, page.records.len()), (RecoveryReadSource::Events, 4, 4));
        let installed = target.install_queue_recovery_stage(spec.clone(), seal.clone(), &stage).await.unwrap();
        assert!(target.queue_handle("q", 0, None).await.is_err());
        assert!(!target.msg_tp_part_dir("q", 0, None).exists());
        target.recovery_routes.clear();
        assert_eq!(target.install_queue_recovery_stage(spec.clone(), seal.clone(), &stage).await.unwrap(), installed);
        target.admit_prepared_queue_recovery(installed.clone()).await.unwrap();
        target.ensure_queue_owner_epoch("q", 0, None, Some(8)).await.unwrap();
        let q = target.queue_handle("q", 0, None).await.unwrap();
        let h = q.resolve().unwrap();
        assert_eq!((h.event_log().head_offset(), h.event_log().next_offset()), (0, 4));
        assert_eq!(fetch_payload(&h, 1).unwrap().unwrap().payload, vec![42; 2048]);
        assert_eq!(fetch_payload(&h, 3).unwrap().unwrap().payload, vec![42; 2048]);
        assert!(fetch_payload(&h, 2).unwrap().is_none());
        let mut state = crate::QueueInternalState::new("q".into(), 0);
        state.load_snapshot(&h.force_encode_snapshot(3).await.unwrap()).unwrap();
        assert_eq!(state.recovery_state_digest(), spec.state_digest);
        assert_eq!(publish(&target, false).await, 4);
        assert_eq!(target.install_queue_recovery_stage(spec.clone(), seal.clone(), &stage).await.unwrap(), installed);
        assert_eq!(h.event_log().next_offset(), 5);
        drop(stage);
        drop(h);
        drop(q);
        target.shutdown().await.unwrap();
        source.shutdown().await.unwrap();
        let root = if reuse { dir.root.join("source") } else { dir.root.join("target") };
        drop(target);
        drop(source);
        let reopened = open(&root).await;
        assert!(reopened.admit_prepared_queue_recovery(installed).await.is_err());
        let sealed = reopened.seal_replica_for_recovery("q", 0, None,
            RecoverySealRequest { transition: [10; 32], fence_epoch: 9 }).await.unwrap();
        assert_eq!((sealed.message_next, sealed.event_next), (0, 5));
        assert_eq!(sealed.history.storage_history.unwrap().binding, spec.binding);
        assert!(!reopened.msg_tp_part_dir("q", 0, None).exists());
        reopened.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_agreed_checkpoint_preserves_old_payload_until_replacement() {
    use crate::{QueueLogLayout, RecoverySealRequest, StorageHistoryBinding};
    #[derive(Debug)]
    struct NoLag;
    impl QueueReplicationRetention for NoLag {
        fn retained_from(&self, _: &str, _: u32, _: Option<&str>, _: Option<&StorageHistoryBinding>) -> Result<(u64, u64)> {
            // The absent message lane stays at zero. Only event progress pins a combined queue.
            Ok((0, u64::MAX))
        }
    }
    let dir = keratin_log::test_dir!("combined_agreed_checkpoint");
    let st = open(&dir.root).await;
    st.set_queue_replication_retention(Arc::new(NoLag));
    let storage = st.prepare_empty_storage_history("q", 0, None, PartitionKind::Queue,
        StorageHistoryBinding { resource_incarnation: [1; 16], accepted_history: [2; 16], writer_session: [3; 16] }).await.unwrap();
    st.admit_prepared_storage_history(storage.clone()).await.unwrap();
    assert_eq!(publish(&st, true).await, 0);
    assert_eq!(publish(&st, false).await, 1);
    ack(&st, 1).await;
    let a = st.begin_queue_checkpoint_pin(storage.clone(), [1; 32]).await.unwrap();
    assert_eq!(a.pin.log_layout, QueueLogLayout::Combined);
    assert_eq!(a.pin.event_next, 3);
    assert_eq!(publish(&st, false).await, 3);
    assert_eq!(publish(&st, false).await, 4);
    ack(&st, 3).await;
    let b = st.begin_queue_checkpoint_pin(storage.clone(), [2; 32]).await.unwrap();
    let target = st.queue_checkpoint_target(storage.clone(), b.pin.event_next, b.pin.message_head, b.pin.message_next).await.unwrap();
    assert_eq!((target.message_head, target.message_next, target.event_next), (0, 6, 6));
    let ca = st.build_queue_checkpoint_capsule(a.pin.clone(), target.clone(), Default::default()).await.unwrap();
    let cb = st.build_queue_checkpoint_capsule(b.pin.clone(), target, Default::default()).await.unwrap();
    assert_eq!(ca.contents, cb.contents);
    assert_eq!(ca.snapshot, cb.snapshot);
    let mut bad = ca.contents.target();
    bad.log_layout = QueueLogLayout::Separate;
    assert!(st.build_queue_checkpoint_capsule(a.pin.clone(), bad, Default::default()).await.is_err());
    st.accept_queue_checkpoint(a.pin.clone(), [11; 32], ca.digest().unwrap(), None).await.unwrap();
    let ticket = st.queue_handle("q", 0, None).await.unwrap();
    let h = ticket.resolve().unwrap();
    assert_eq!(h.event_log().head_offset(), 0);
    assert_eq!(fetch_payload(&h, 0).unwrap().unwrap().payload, vec![42; 2048]);
    assert!(h.optional_message_log().is_none());
    assert_eq!(st.queue_checkpoint_activity("q", 0, None).unwrap().message_next, 6);
    st.release_queue_checkpoint_pin(b.pin).await.unwrap();
    ack(&st, 0).await;
    ack(&st, 4).await;
    let c = st.begin_queue_checkpoint_pin(storage.clone(), [3; 32]).await.unwrap();
    let target = st.queue_checkpoint_target(storage, c.pin.event_next, c.pin.message_head, c.pin.message_next).await.unwrap();
    assert_eq!((target.message_head, target.message_next, target.event_next), (8, 8, 8));
    let cc = st.build_queue_checkpoint_capsule(c.pin.clone(), target, Default::default()).await.unwrap();
    st.accept_queue_checkpoint(c.pin, [12; 32], cc.digest().unwrap(), Some([11; 32])).await.unwrap();
    assert_eq!(h.event_log().head_offset(), 8);
    drop(h);
    drop(ticket);
    st.shutdown().await.unwrap();
    drop(st);
    let st = open(&dir.root).await;
    let seal = st.seal_replica_for_recovery("q", 0, None,
        RecoverySealRequest { transition: [9; 32], fence_epoch: 9 }).await.unwrap();
    assert_eq!((seal.event_head, seal.event_next, seal.message_next), (8, 8, 0));
    assert!(seal.history.snapshot_digest.is_some());
    let mut check = crate::recovery_inspection::RecoveryPairInspector::new("q", 0, None, false,
        seal.clone(), seal.clone(), Default::default()).unwrap().with_queue_checkpoint_replay(8, Default::default(), 4096).unwrap();
    while let Some((side, request)) = check.next_read().unwrap() {
        let page = st.read_sealed_replica("q", 0, None, PartitionKind::Queue, request).await.unwrap();
        check.accept_page(side, page).unwrap();
    }
    let (_, [recovered, _]) = check.finish_with_queue_artifacts(4096).unwrap();
    assert_eq!(recovered.state_snapshot(), cc.snapshot);
    assert!(!st.msg_tp_part_dir("q", 0, None).exists());
    st.shutdown().await.unwrap();
}
