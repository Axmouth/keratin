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
async fn cached_selected_reads_match_file_reads_and_give_up_on_a_miss() {
    let dir = keratin_log::test_dir!("combined_cached_selected");
    let open_cached = |root: std::path::PathBuf| async move {
        let mut cfg = KeratinConfig::test_default();
        cfg.tail_cache_bytes = 1024 * 1024;
        Stroma::open_with_options(
            root,
            StromaKeratinConfig::from_message_log(cfg),
            SnapshotConfig {
                every_events: u64::MAX,
            },
            StromaOptions::default(),
        )
        .await
        .unwrap()
    };
    let st = open_cached(dir.root.clone()).await;
    assert_eq!(publish(&st, false).await, 0);
    assert_eq!(publish(&st, true).await, 1);
    ack(&st, 0).await;
    assert_eq!(publish(&st, false).await, 3);
    let key = |m: &(u64, MessageHeaders, Vec<u8>, u32)| (m.0, m.2.clone(), m.3);
    {
        let q = st.queue_handle("q", 0, None).await.unwrap();
        let h = q.resolve().unwrap();
        for written in [false, true] {
            let cached = read_selected_cached(&h, &[(0, 0), (3, 1)], written)
                .unwrap()
                .unwrap();
            let read = read_selected(&h, &[(0, 0), (3, 1)], written).unwrap();
            assert_eq!(
                cached.iter().map(key).collect::<Vec<_>>(),
                read.iter().map(key).collect::<Vec<_>>()
            );
            // Identity checks still apply: offset 2 is an ack, not a payload.
            assert!(read_selected_cached(&h, &[(2, 0)], written).is_err());
        }
    }
    st.shutdown().await.unwrap();
    drop(st);
    // A reopened log starts with an empty tail cache, so the cached read gives
    // up and the file read still finds the record.
    let st = open_cached(dir.root.clone()).await;
    let q = st.queue_handle("q", 0, None).await.unwrap();
    let h = q.resolve().unwrap();
    assert!(
        read_selected_cached(&h, &[(0, 0)], false)
            .unwrap()
            .is_none()
    );
    assert_eq!(read_selected(&h, &[(0, 0)], false).unwrap().len(), 1);
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
            // metadata, and preserve exact selection and retry identity. Gap 0
            // skips every unselected record and u64::MAX scans all of them.
            for gap in [0, 1, SELECTED_SCAN_GAP, u64::MAX] {
                let read = |offsets: &[(u64, u32)]| {
                    read_selected_with_limits(&h, offsets, budget, gap, false)
                };
                let selected = read(&[(0, 0), (3, 1)]).unwrap();
                assert_eq!(selected.iter().map(|m| m.0).collect::<Vec<_>>(), [0, 3]);
                assert_eq!(selected[1].2, vec![42; 2048]);
                assert_eq!(selected[1].3, 1);
                assert_eq!(read(&[(1, 0), (4, 0)]).unwrap().len(), 2);
                assert!(read(&[(2, 0)]).is_err());
                assert!(read(&[(0, 0), (2, 0)]).is_err());
                assert!(read(&[(5, 0)]).is_err());
                assert!(read(&[(0, 0), (5, 0)]).is_err());
            }
        }
        assert!(read_selected(&h, &[(2, 0)], false).is_err());
        // This store has no tail cache, so the cached read always gives up.
        assert!(
            read_selected_cached(&h, &[(0, 0)], false)
                .unwrap()
                .is_none()
        );
        assert!(fetch_payload(&h, 2, false).unwrap().is_none());
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
        st.truncate_queue_before("q", 0, None, u64::MAX)
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
    assert!(
        fetch_payload(&q.resolve().unwrap(), 0, false)
            .unwrap()
            .is_some()
    );
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
    assert!(
        fetch_payload(&q.resolve().unwrap(), 0, false)
            .unwrap()
            .is_some()
    );
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
            ReplicatedEventBatch {
                epoch: 1,
                first_offset: 0,
                events: events.clone(),
                durability: Some(KDurability::AfterFsync),
                encoded: None,
            },
        )
        .await
        .unwrap();
    }
    let q = st.queue_handle("q", 0, None).await.unwrap();
    let h = q.resolve().unwrap();
    assert_eq!(h.work_queue().unwrap().retries(0).await, 1);
    assert_eq!(fetch_payload(&h, 0, false).unwrap().unwrap().payload, [42]);
    assert_eq!(
        st.queue_replication_next_offset("q", 0, None)
            .await
            .unwrap(),
        2
    );
    drop(h);
    st.shutdown().await.unwrap();
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
        .read_owner_queue_records("q", 0, None, 0, 10, 1024)
        .await
        .unwrap();
    let crate::OwnerReplicationRead::Batch(batch) = read else {
        panic!("unexpected checkpoint")
    };
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.next_offset, 1);
    let read = st
        .read_owner_queue_records("q", 0, None, 1, 10, 1024)
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
async fn single_log_lifecycle_and_stream_reads_are_explicit() {
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
    // A queue has no stream record log, so stream reads are refused rather
    // than answered with an empty page.
    assert!(
        st.read_owner_stream_records("q", 0, 0, 100, usize::MAX)
            .await
            .is_err()
    );
    assert!(
        st.read_owner_stream_cursor_events("q", 0, 0, 100, usize::MAX)
            .await
            .is_err()
    );
    publish(&st, false).await;
    st.become_queue_follower("q", 0, None).await.unwrap();
    assert!(
        st.read_owner_queue_records("q", 0, None, 0, 100, usize::MAX)
            .await
            .is_err()
    );
    assert_eq!(
        st.queue_replication_next_offset("q", 0, None)
            .await
            .unwrap(),
        1
    );
    // An empty single-log frame is refused before poisoning follower apply.
    assert!(
        st.apply_replicated_queue_batch(
            "q",
            0,
            None,
            ReplicatedEventBatch {
                epoch: 6,
                first_offset: 1,
                events: vec![],
                durability: Some(KDurability::AfterFsync),
                encoded: None,
            },
        )
        .await
        .is_err()
    );
    st.become_queue_owner_with_epoch("q", 0, None, 8)
        .await
        .unwrap();
    assert_eq!(h.event_log().current_epoch(), 8);
    assert!(st.advance_queue_epoch("q", 0, None, 7).await.is_err());
    drop(h);
    drop(q);
    st.shutdown().await.unwrap();
    drop(st);
    let st = open(&dir.root).await;
    let q = st.queue_handle("q", 0, None).await.unwrap();
    let h = q.resolve().unwrap();
    assert!(h.optional_message_log().is_none());
    assert_eq!(h.event_log().current_epoch(), 8);
    assert_eq!(
        fetch_payload(&h, 0, false).unwrap().unwrap().payload,
        vec![42; 2048]
    );
    drop(h);
    st.shutdown().await.unwrap();
}

#[cfg(feature = "io-fault-injection")]
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
                .read_owner_queue_records("q", 0, None, 0, 100, usize::MAX)
                .await
                .unwrap()
            else {
                panic!()
            };
            assert_eq!(durable.next_offset, prefix);
            let OwnerReplicationRead::Batch(tentative) = owner
                .read_owner_queue_records_early("q", 0, None, 0, 100, usize::MAX)
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
                    Box::pin(futures::stream::iter([Ok(ReplicatedEventBatch {
                        epoch: tentative.epoch,
                        first_offset: 0,
                        events: tentative
                            .records
                            .into_iter()
                            .map(|(_, event)| event)
                            .collect(),
                        durability: Some(KDurability::AfterFsync),
                        encoded: None,
                    })])),
                )
                .await
                .unwrap();
            let fq = follower.queue_handle("q", 0, None).await.unwrap();
            let fh = fq.resolve().unwrap();
            assert_eq!(fh.event_log().next_offset(), prefix + 1);
            assert_eq!(
                fetch_payload(&fh, prefix, false).unwrap().unwrap().payload,
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
    use crate::{RecoveryReadSource, RecoverySealRequest};
    for snapshot in [false, true] {
        let dir = keratin_log::test_dir!("combined_recovery_cold");
        let mut st = open(&dir.root).await;
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
        assert_eq!(publish(&st, false).await, 0);
        assert_eq!(publish(&st, true).await, 1);
        ack(&st, 0).await;
        if snapshot {
            st.snapshot_partition("q", 0, None).await.unwrap();
        }
        assert_eq!(publish(&st, false).await, 3);
        ack(&st, 3).await;
        let seal = RecoverySealRequest {
            transition: [9; 32],
            fence_epoch: 8,
        };
        let sealed = st
            .seal_replica_for_recovery("q", 0, None, seal.clone())
            .await
            .unwrap();
        assert_eq!(sealed.history.queue_log().unwrap().next, 5);
        assert_eq!(sealed.history.snapshot_digest.is_some(), snapshot);
        for cold in [false, true] {
            if cold {
                st.shutdown().await.unwrap();
                drop(st);
                st = open(&dir.root).await;
            }
            assert!(st.queue_handle("q", 0, None).await.is_err());
            assert!(!st.msg_tp_part_dir("q", 0, None).exists());
            assert_eq!(
                st.seal_replica_for_recovery("q", 0, None, seal.clone())
                    .await
                    .unwrap(),
                sealed
            );
            let mut paired = None;
            for single in [false, true] {
                let mut check = RecoveryPairInspector::new(
                    "q",
                    0,
                    None,
                    sealed.clone(),
                    sealed.clone(),
                    RecoveryInspectionLimits {
                        total_records: if single { 10 } else { 20 },
                        page_records: 2,
                        page_bytes: 4096,
                        ..Default::default()
                    },
                )
                .unwrap()
                .with_queue_checkpoint_replay(5, Default::default(), 4096)
                .unwrap();
                if single {
                    check = check.with_single_source().unwrap();
                }
                let mut cursors = [None, None];
                let mut repeated = [0, 0];
                while let Some((side, request)) = check.next_read().unwrap() {
                    let i = match side {
                        crate::recovery_inspection::RecoverySide::Left => 0,
                        crate::recovery_inspection::RecoverySide::Right => 1,
                    };
                    assert_ne!(request.source, RecoveryReadSource::StreamRecords);
                    if request.source == RecoveryReadSource::Log
                        && request.from == sealed.history.queue_log().unwrap().head
                    {
                        repeated[i] += 1;
                    }
                    let strict = st
                        .read_sealed_replica("q", 0, None, PartitionKind::Queue, request.clone())
                        .await
                        .unwrap();
                    let (page, cursor) = st
                        .read_sealed_replica_sequential(
                            "q",
                            0,
                            None,
                            PartitionKind::Queue,
                            request,
                            cursors[i].take(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(strict, page);
                    cursors[i] = cursor;
                    check.accept_page(side, page).unwrap();
                }
                assert_eq!(repeated, if single { [2, 0] } else { [2, 2] });
                let (report, [a, b]) = check.finish_with_queue_artifacts(4096).unwrap();
                assert_eq!(a, b);
                assert_eq!(report.records, if single { 10 } else { 20 });
                if let Some((expected, bytes)) = &paired {
                    assert_eq!(&a, expected);
                    assert_eq!(report.bytes * 2, *bytes);
                } else {
                    paired = Some((a.clone(), report.bytes));
                }
                let mut state = crate::QueueInternalState::new("q".into(), 0);
                state.load_snapshot(a.state_snapshot()).unwrap();
                assert!(!state.is_ready(1));
                assert_eq!(
                    state
                        .recovery_live_ranges()
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>(),
                    vec![1..2]
                );
                assert_eq!(a.evidence().log.next, 5);
                drop(cursors);
            }
        }
        st.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_recovery_stage_install_preserves_payloads_and_retry_progress() {
    use crate::recovery_inspection::{RecoveryInspectionLimits, RecoveryPairInspector};
    use crate::{
        QueueRecoveryStageSpec, RecoveryReadRequest, RecoveryReadSource, RecoverySealRequest,
        StorageHistoryBinding,
    };
    for reuse in [false, true] {
        let dir = keratin_log::test_dir!("combined_recovery_install");
        let source = open(&dir.root.join("source")).await;
        source
            .initialize_empty_storage_history(
                "q",
                0,
                None,
                PartitionKind::Queue,
                StorageHistoryBinding {
                    resource_incarnation: [1; 16],
                    accepted_history: [2; 16],
                    writer_session: [3; 16],
                },
            )
            .await
            .unwrap();
        assert_eq!(publish(&source, false).await, 0);
        assert_eq!(publish(&source, true).await, 1);
        ack(&source, 0).await;
        source.snapshot_partition("q", 0, None).await.unwrap();
        assert_eq!(publish(&source, false).await, 3);
        let seal = RecoverySealRequest {
            transition: [9; 32],
            fence_epoch: 8,
        };
        let sealed = source
            .seal_replica_for_recovery("q", 0, None, seal.clone())
            .await
            .unwrap();
        let mut check = RecoveryPairInspector::new(
            "q",
            0,
            None,
            sealed.clone(),
            sealed.clone(),
            RecoveryInspectionLimits {
                page_records: 2,
                page_bytes: 4096,
                ..Default::default()
            },
        )
        .unwrap()
        .with_queue_checkpoint_replay(4, Default::default(), 4096)
        .unwrap();
        while let Some((side, request)) = check.next_read().unwrap() {
            let page = source
                .read_sealed_replica("q", 0, None, PartitionKind::Queue, request)
                .await
                .unwrap();
            check.accept_page(side, page).unwrap();
        }
        let (_, [artifact, _]) = check.finish_with_queue_artifacts(4096).unwrap();
        let spec = QueueRecoveryStageSpec {
            plan: [8; 32],
            topic: "q".into(),
            partition: 0,
            group: None,
            binding: StorageHistoryBinding {
                resource_incarnation: [1; 16],
                accepted_history: [4; 16],
                writer_session: [5; 16],
            },
            fence_epoch: 8,
            source_history: sealed.history.id,
            log: *sealed.history.queue_log().unwrap(),
            snapshot_digest: artifact.snapshot_digest(),
            snapshot_bytes: artifact.state_snapshot().len() as u64,
            state_digest: artifact.evidence().lease_normalized_state_digest,
            live_payload_digest: artifact.evidence().live_payload_digest.unwrap(),
        };
        let target = if reuse {
            source.clone()
        } else {
            open(&dir.root.join("target")).await
        };
        // A snapshot whose boundary differs from the source log tail must
        // never authorize installation.
        let mut wrong = spec.clone();
        wrong.log.next -= 1;
        assert!(
            target
                .open_queue_recovery_stage(
                    wrong,
                    artifact.state_snapshot().to_vec(),
                    Default::default()
                )
                .await
                .is_err()
        );
        let mut stage = if reuse {
            target
                .open_queue_recovery_stage_reusing(
                    spec.clone(),
                    artifact.state_snapshot().to_vec(),
                    Default::default(),
                    seal.clone(),
                )
                .await
                .unwrap()
        } else {
            target
                .open_queue_recovery_stage(
                    spec.clone(),
                    artifact.state_snapshot().to_vec(),
                    Default::default(),
                )
                .await
                .unwrap()
        };
        assert_eq!(stage.next_offset().await, if reuse { 4 } else { 0 });
        if !reuse {
            let mut from = 0;
            while from < spec.log.next {
                let page = source
                    .read_sealed_replica(
                        "q",
                        0,
                        None,
                        PartitionKind::Queue,
                        RecoveryReadRequest {
                            seal: seal.clone(),
                            history_id: sealed.history.id,
                            source: RecoveryReadSource::Log,
                            from,
                            max_records: 2,
                            max_bytes: 4096,
                        },
                    )
                    .await
                    .unwrap();
                let next = page.next;
                assert!(next > from);
                assert_eq!(stage.append(page.clone()).await.unwrap(), next);
                assert_eq!(stage.append(page).await.unwrap(), next);
                if from == 0 {
                    drop(stage);
                    stage = target
                        .resume_queue_recovery_stage(spec.clone(), Default::default())
                        .await
                        .unwrap();
                    assert_eq!(stage.next_offset().await, next);
                }
                from = next;
            }
        }
        let receipt = stage.finish().await.unwrap();
        assert_eq!(receipt.next, 4);
        let page = stage.read_completed_messages(0, 10, 16384).await.unwrap();
        assert_eq!(
            (page.source, page.end, page.records.len()),
            (RecoveryReadSource::Log, 4, 4)
        );
        let installed = target
            .install_queue_recovery_stage(spec.clone(), seal.clone(), &stage)
            .await
            .unwrap();
        assert!(target.queue_handle("q", 0, None).await.is_err());
        assert!(!target.msg_tp_part_dir("q", 0, None).exists());
        target.generations.clear();
        assert_eq!(
            target
                .install_queue_recovery_stage(spec.clone(), seal.clone(), &stage)
                .await
                .unwrap(),
            installed
        );
        target
            .admit_prepared_queue_recovery(installed.clone())
            .await
            .unwrap();
        target
            .ensure_queue_owner_epoch("q", 0, None, Some(8))
            .await
            .unwrap();
        let q = target.queue_handle("q", 0, None).await.unwrap();
        let h = q.resolve().unwrap();
        assert_eq!(
            (h.event_log().head_offset(), h.event_log().next_offset()),
            (0, 4)
        );
        assert_eq!(
            fetch_payload(&h, 1, false).unwrap().unwrap().payload,
            vec![42; 2048]
        );
        assert_eq!(
            fetch_payload(&h, 3, false).unwrap().unwrap().payload,
            vec![42; 2048]
        );
        assert!(fetch_payload(&h, 2, false).unwrap().is_none());
        let mut state = crate::QueueInternalState::new("q".into(), 0);
        state
            .load_snapshot(&h.force_encode_snapshot(3).await.unwrap())
            .unwrap();
        assert_eq!(state.recovery_state_digest(), spec.state_digest);
        assert_eq!(publish(&target, false).await, 4);
        assert_eq!(
            target
                .install_queue_recovery_stage(spec.clone(), seal.clone(), &stage)
                .await
                .unwrap(),
            installed
        );
        assert_eq!(h.event_log().next_offset(), 5);
        drop(stage);
        drop(h);
        drop(q);
        target.shutdown().await.unwrap();
        source.shutdown().await.unwrap();
        let root = if reuse {
            dir.root.join("source")
        } else {
            dir.root.join("target")
        };
        drop(target);
        drop(source);
        let reopened = open(&root).await;
        assert!(
            reopened
                .admit_prepared_queue_recovery(installed)
                .await
                .is_err()
        );
        let sealed = reopened
            .seal_replica_for_recovery(
                "q",
                0,
                None,
                RecoverySealRequest {
                    transition: [10; 32],
                    fence_epoch: 9,
                },
            )
            .await
            .unwrap();
        assert_eq!(sealed.history.queue_log().unwrap().next, 5);
        assert_eq!(
            sealed.history.storage_history.unwrap().binding,
            spec.binding
        );
        assert!(!reopened.msg_tp_part_dir("q", 0, None).exists());
        reopened.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_agreed_checkpoint_preserves_old_payload_until_replacement() {
    use crate::{RecoverySealRequest, StorageHistoryBinding};
    #[derive(Debug)]
    struct NoLag;
    impl QueueReplicationRetention for NoLag {
        fn retained_next(
            &self,
            _: &str,
            _: u32,
            _: Option<&str>,
            _: Option<&StorageHistoryBinding>,
        ) -> Result<u64> {
            Ok(u64::MAX)
        }
    }
    let dir = keratin_log::test_dir!("combined_agreed_checkpoint");
    let st = open(&dir.root).await;
    st.set_queue_replication_retention(Arc::new(NoLag));
    let storage = st
        .prepare_empty_storage_history(
            "q",
            0,
            None,
            PartitionKind::Queue,
            StorageHistoryBinding {
                resource_incarnation: [1; 16],
                accepted_history: [2; 16],
                writer_session: [3; 16],
            },
        )
        .await
        .unwrap();
    st.admit_prepared_storage_history(storage.clone())
        .await
        .unwrap();
    assert_eq!(publish(&st, true).await, 0);
    assert_eq!(publish(&st, false).await, 1);
    ack(&st, 1).await;
    let a = st
        .begin_queue_checkpoint_pin(storage.clone(), [1; 32])
        .await
        .unwrap();
    assert_eq!(
        (a.pin.applied_next, a.pin.log_next, a.pin.payload_head),
        (3, 3, 0)
    );
    assert_eq!(publish(&st, false).await, 3);
    assert_eq!(publish(&st, false).await, 4);
    ack(&st, 3).await;
    let b = st
        .begin_queue_checkpoint_pin(storage.clone(), [2; 32])
        .await
        .unwrap();
    let target = st
        .queue_checkpoint_target(storage.clone(), b.pin.log_next, b.pin.payload_head)
        .await
        .unwrap();
    assert_eq!((target.payload_head, target.next), (0, 6));
    let ca = st
        .build_queue_checkpoint_capsule(a.pin.clone(), target.clone(), Default::default())
        .await
        .unwrap();
    let cb = st
        .build_queue_checkpoint_capsule(b.pin.clone(), target, Default::default())
        .await
        .unwrap();
    assert_eq!(ca.contents, cb.contents);
    assert_eq!(ca.snapshot, cb.snapshot);
    let mut bad = ca.contents.target();
    bad.payload_head = bad.next + 1;
    assert!(
        st.build_queue_checkpoint_capsule(a.pin.clone(), bad, Default::default())
            .await
            .is_err()
    );
    st.accept_queue_checkpoint(a.pin.clone(), [11; 32], ca.digest().unwrap(), None)
        .await
        .unwrap();
    let ticket = st.queue_handle("q", 0, None).await.unwrap();
    let h = ticket.resolve().unwrap();
    assert_eq!(h.event_log().head_offset(), 0);
    assert_eq!(
        fetch_payload(&h, 0, false).unwrap().unwrap().payload,
        vec![42; 2048]
    );
    assert!(h.optional_message_log().is_none());
    assert_eq!(st.queue_checkpoint_activity("q", 0, None).unwrap().next, 6);
    st.release_queue_checkpoint_pin(b.pin).await.unwrap();
    ack(&st, 0).await;
    ack(&st, 4).await;
    let c = st
        .begin_queue_checkpoint_pin(storage.clone(), [3; 32])
        .await
        .unwrap();
    let target = st
        .queue_checkpoint_target(storage, c.pin.log_next, c.pin.payload_head)
        .await
        .unwrap();
    assert_eq!((target.payload_head, target.next), (8, 8));
    let cc = st
        .build_queue_checkpoint_capsule(c.pin.clone(), target, Default::default())
        .await
        .unwrap();
    st.accept_queue_checkpoint(c.pin, [12; 32], cc.digest().unwrap(), Some([11; 32]))
        .await
        .unwrap();
    assert_eq!(h.event_log().head_offset(), 8);
    drop(h);
    drop(ticket);
    st.shutdown().await.unwrap();
    drop(st);
    let st = open(&dir.root).await;
    let seal = st
        .seal_replica_for_recovery(
            "q",
            0,
            None,
            RecoverySealRequest {
                transition: [9; 32],
                fence_epoch: 9,
            },
        )
        .await
        .unwrap();
    let log = seal.history.queue_log().unwrap();
    assert_eq!((log.head, log.next), (8, 8));
    assert!(seal.history.snapshot_digest.is_some());
    let mut check = crate::recovery_inspection::RecoveryPairInspector::new(
        "q",
        0,
        None,
        seal.clone(),
        seal.clone(),
        Default::default(),
    )
    .unwrap()
    .with_queue_checkpoint_replay(8, Default::default(), 4096)
    .unwrap();
    while let Some((side, request)) = check.next_read().unwrap() {
        let page = st
            .read_sealed_replica("q", 0, None, PartitionKind::Queue, request)
            .await
            .unwrap();
        check.accept_page(side, page).unwrap();
    }
    let (_, [recovered, _]) = check.finish_with_queue_artifacts(4096).unwrap();
    assert_eq!(recovered.state_snapshot(), cc.snapshot);
    assert!(!st.msg_tp_part_dir("q", 0, None).exists());
    st.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_learner_checkpoint_crash_child() {
    let Some(root) = std::env::var_os("STROMA_COMBINED_LEARNER_CRASH_ROOT") else {
        return;
    };
    let root = Path::new(&root);
    let install =
        rmp_serde::from_slice(&fs::read(root.with_extension("fixture")).unwrap()).unwrap();
    let st = open(root).await;
    let binding = crate::StorageHistoryBinding {
        resource_incarnation: [1; 16],
        accepted_history: [2; 16],
        writer_session: [3; 16],
    };
    let receipt = st
        .prepare_queue_learner_storage("q", 0, None, binding, [4; 32])
        .await
        .unwrap();
    st.become_queue_follower_with_epoch("q", 0, None, 7)
        .await
        .unwrap();
    st.install_queue_learner_checkpoint(receipt, install)
        .await
        .unwrap();
    panic!("child did not stop at the requested installation boundary");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_learner_checkpoint_resumes_old_payload_backfill_without_reapplying_nack() {
    use crate::{OwnerReplicationRead, StorageHistoryBinding};
    let dir = keratin_log::test_dir!("combined_learner_checkpoint");
    let owner = open(&dir.root.join("owner")).await;
    let binding = StorageHistoryBinding {
        resource_incarnation: [1; 16],
        accepted_history: [2; 16],
        writer_session: [3; 16],
    };
    owner
        .initialize_empty_storage_history("q", 0, None, PartitionKind::Queue, binding.clone())
        .await
        .unwrap();
    owner
        .ensure_queue_owner_epoch("q", 0, None, Some(7))
        .await
        .unwrap();
    assert_eq!(publish(&owner, true).await, 0);
    assert_eq!(publish(&owner, false).await, 1);
    owner.nack_one("q", 0, None, 1, true).await.unwrap();
    let cut = owner
        .export_queue_state_checkpoint("q", 0, None)
        .await
        .unwrap();
    assert_eq!((cut.epoch, cut.start_offset, cut.next_offset), (7, 0, 3));
    let OwnerReplicationRead::Batch(batch) = owner
        .read_owner_queue_records("q", 0, None, 0, 100, usize::MAX)
        .await
        .unwrap()
    else {
        panic!("retained source")
    };
    let events: Vec<_> = batch.records.into_iter().map(|(_, e)| e).collect();
    assert_eq!(events.len(), 3);
    for hard_kill in [false, true]
        .into_iter()
        .filter(|hard_kill| !hard_kill || cfg!(unix))
    {
        for boundary in [
            "intent",
            "follower",
            "log_reset",
            "state",
            "snapshot",
            "complete",
        ] {
            let root = dir.root.join(format!("{boundary}-{hard_kill}"));
            let mut st = open(&root).await;
            let receipt = st
                .prepare_queue_learner_storage("q", 0, None, binding.clone(), [4; 32])
                .await
                .unwrap();
            st.become_queue_follower_with_epoch("q", 0, None, 7)
                .await
                .unwrap();
            let install = cut.clone();
            let mut wrong = install.clone();
            wrong.start_offset = 1;
            assert!(
                st.install_queue_learner_checkpoint(receipt.clone(), wrong)
                    .await
                    .is_err()
            );
            if hard_kill {
                st.shutdown().await.unwrap();
                drop(st);
                fs::write(
                    root.with_extension("fixture"),
                    rmp_serde::to_vec_named(&install).unwrap(),
                )
                .unwrap();
                #[cfg(unix)]
                {
                    struct KillOnDrop(std::process::Child);
                    impl Drop for KillOnDrop {
                        fn drop(&mut self) {
                            let _ = self.0.kill();
                            let _ = self.0.wait();
                        }
                    }
                    let ready = root.with_extension("ready");
                    let mut child = KillOnDrop(
                        std::process::Command::new(std::env::current_exe().unwrap())
                            .args([
                                "--exact",
                                "stroma::combined::tests::combined_learner_checkpoint_crash_child",
                                "--nocapture",
                            ])
                            .env("STROMA_COMBINED_LEARNER_CRASH_ROOT", &root)
                            .env("STROMA_CHECKPOINT_CRASH_READY", &ready)
                            .env("STROMA_CHECKPOINT_CRASH_BOUNDARY", boundary)
                            .stdout(std::process::Stdio::null())
                            .spawn()
                            .unwrap(),
                    );
                    tokio::time::timeout(std::time::Duration::from_secs(20), async {
                        while !ready.exists() {
                            assert!(
                                child.0.try_wait().unwrap().is_none(),
                                "child exited before {boundary}"
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                        }
                    })
                    .await
                    .unwrap();
                    child.0.kill().unwrap();
                    use std::os::unix::process::ExitStatusExt;
                    assert_eq!(child.0.wait().unwrap().signal(), Some(9));
                }
            } else {
                *st.checkpoint_fault.lock().unwrap() = Some(boundary);
                let err = st
                    .install_queue_learner_checkpoint(receipt, install.clone())
                    .await
                    .unwrap_err();
                assert!(err.to_string().contains(boundary), "{err}");
                st.shutdown().await.unwrap();
                drop(st);
            }
            st = open(&root).await;
            assert!(st.queue_handle("q", 0, None).await.is_err());
            let mut receipt = st
                .prepare_queue_learner_storage("q", 0, None, binding.clone(), [5; 32])
                .await
                .unwrap();
            for (offset, event) in events.iter().enumerate() {
                assert!(
                    st.verify_queue_learner_caught_up("q", 0, None, 7, 3)
                        .await
                        .is_err()
                );
                assert!(st.become_queue_owner("q", 0, None).await.is_err());
                for _ in 0..2 {
                    let batch = ReplicatedEventBatch {
                        epoch: 7,
                        first_offset: offset as u64,
                        events: vec![event.clone()],
                        durability: Some(KDurability::AfterFsync),
                        encoded: None,
                    };
                    st.apply_replicated_queue_batch("q", 0, None, batch)
                        .await
                        .unwrap();
                }
                if offset == 0 {
                    st.shutdown().await.unwrap();
                    drop(st);
                    st = open(&root).await;
                    receipt = st
                        .prepare_queue_learner_storage("q", 0, None, binding.clone(), [6; 32])
                        .await
                        .unwrap();
                    assert_eq!(
                        st.queue_replication_next_offset("q", 0, None)
                            .await
                            .unwrap(),
                        1
                    );
                    assert!(!st.msg_tp_part_dir("q", 0, None).exists());
                }
            }
            st.verify_queue_learner_caught_up("q", 0, None, 7, 3)
                .await
                .unwrap();
            let ticket = st.queue_handle("q", 0, None).await.unwrap();
            let h = ticket.resolve().unwrap();
            assert_eq!(h.work_queue().unwrap().retries(1).await, 1);
            assert_eq!(
                fetch_payload(&h, 0, false).unwrap().unwrap().payload,
                vec![42; 2048]
            );
            assert_eq!(
                fetch_payload(&h, 1, false).unwrap().unwrap().payload,
                vec![42; 2048]
            );
            let mut state = crate::QueueInternalState::new("q".into(), 0);
            state
                .load_snapshot(&h.force_encode_snapshot(2).await.unwrap())
                .unwrap();
            let mut expected = crate::QueueInternalState::new("q".into(), 0);
            expected.load_snapshot(&cut.state_snapshot).unwrap();
            assert_eq!(
                state.recovery_state_digest(),
                expected.recovery_state_digest()
            );
            st.install_queue_learner_checkpoint(receipt, install)
                .await
                .unwrap();
            assert_eq!(h.event_log().next_offset(), 3);
            drop(h);
            st.shutdown().await.unwrap();
        }
    }
    owner.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_learner_requires_exact_backfill_receipt_and_real_payload_records() {
    use crate::{QueueStateCheckpoint, StorageHistoryBinding};
    for missing_receipt in [true, false] {
        let dir = keratin_log::test_dir!("combined_learner_rejects_bad_backfill");
        let st = open(&dir.root).await;
        let binding = StorageHistoryBinding {
            resource_incarnation: [1; 16],
            accepted_history: [2; 16],
            writer_session: [3; 16],
        };
        let receipt = st
            .prepare_queue_learner_storage("q", 0, None, binding.clone(), [4; 32])
            .await
            .unwrap();
        st.become_queue_follower_with_epoch("q", 0, None, 7)
            .await
            .unwrap();
        let mut state = crate::QueueInternalState::new("q".into(), 0);
        state.enqueue(0, 1, None);
        state.ack(1);
        st.install_queue_learner_checkpoint(
            receipt,
            QueueStateCheckpoint {
                epoch: 7,
                start_offset: 0,
                next_offset: 2,
                state_snapshot: state.into_recovery_snapshot(2),
            },
        )
        .await
        .unwrap();
        if !missing_receipt {
            let err = st
                .apply_replicated_queue_batch(
                    "q",
                    0,
                    None,
                    ReplicatedEventBatch {
                        epoch: 7,
                        first_offset: 0,
                        durability: Some(KDurability::AfterFsync),
                        events: vec![
                            StromaEvent::AckMany { reqs: vec![] },
                            StromaEvent::Ack { off: 0 },
                        ],
                        encoded: None,
                    },
                )
                .await
                .unwrap_err();
            assert!(
                err.to_string().contains("retained payload enqueue"),
                "{err}"
            );
            assert!(
                st.verify_queue_learner_caught_up("q", 0, None, 7, 2)
                    .await
                    .is_err()
            );
        }
        let journal = st.snap_dir("q", 0, None).join("checkpoint.install");
        st.shutdown().await.unwrap();
        drop(st);
        if missing_receipt {
            fs::remove_file(journal).unwrap();
        }
        let st = open(&dir.root).await;
        st.prepare_queue_learner_storage("q", 0, None, binding, [5; 32])
            .await
            .unwrap();
        let error = match st.queue_handle("q", 0, None).await {
            Ok(_) => panic!("invalid learner became readable"),
            Err(e) => e,
        };
        let expected = if missing_receipt {
            "exact checkpoint does not cover"
        } else {
            "retained payload enqueue"
        };
        assert!(error.to_string().contains(expected), "{error}");
        st.shutdown().await.unwrap();
    }
}

async fn combined_reseed_case(root: &Path, seal_during_copy: bool) {
    use crate::{OwnerReplicationRead, PreparedStorageHistory, StorageHistoryBinding};
    let st = open(root).await;
    let binding = StorageHistoryBinding {
        resource_incarnation: [1; 16],
        accepted_history: [2; 16],
        writer_session: [3; 16],
    };
    st.initialize_empty_storage_history("q", 0, None, PartitionKind::Queue, binding.clone())
        .await
        .unwrap();
    st.ensure_queue_owner_epoch("q", 0, None, Some(7))
        .await
        .unwrap();
    assert_eq!(publish(&st, false).await, 0);
    let OwnerReplicationRead::Batch(batch) = st
        .read_owner_queue_records("q", 0, None, 0, 100, usize::MAX)
        .await
        .unwrap()
    else {
        panic!("retained original")
    };
    let events = batch.records.into_iter().map(|(_, event)| event).collect();
    st.become_queue_follower_with_epoch("q", 0, None, 7)
        .await
        .unwrap();
    let receipt = PreparedStorageHistory {
        topic: "q".into(),
        partition: 0,
        group: None,
        stream: false,
        binding: binding.clone(),
        storage_instance: st.storage_session,
    };
    let view = st
        .prepare_queue_reseed_storage(receipt.clone(), [41; 32])
        .await
        .unwrap();
    view.become_queue_follower_with_epoch("q", 0, None, 7)
        .await
        .unwrap();
    assert!(
        st.install_queue_reseed_storage(receipt.clone(), [41; 32], 7, 0)
            .await
            .is_err()
    );
    assert_eq!(
        st.queue_replication_next_offset("q", 0, None)
            .await
            .unwrap(),
        1
    );
    let original_root = st.partition_root("q", 0, None);
    if seal_during_copy {
        let seal = st
            .seal_replica_for_recovery(
                "q",
                0,
                None,
                RecoverySealRequest {
                    transition: [45; 32],
                    fence_epoch: 8,
                },
            )
            .await
            .unwrap();
        assert_eq!(seal.history.queue_log().unwrap().next, 1);
    }
    view.apply_replicated_queue_batch(
        "q",
        0,
        None,
        ReplicatedEventBatch {
            epoch: 7,
            first_offset: 0,
            events,
            durability: Some(KDurability::AfterFsync),
            encoded: None,
        },
    )
    .await
    .unwrap();
    let installed = st
        .install_queue_reseed_storage(receipt.clone(), [41; 32], 7, 1)
        .await;
    if seal_during_copy {
        assert!(installed.is_err());
        assert_eq!(st.partition_root("q", 0, None), original_root);
    } else {
        installed.unwrap();
        assert_ne!(st.partition_root("q", 0, None), original_root);
        assert!(view.verify_admitted_storage_history(&receipt).is_err());
        assert!(
            view.resume_unaccepted_storage_history("q", 0, None, binding)
                .await
                .is_err()
        );
        st.become_queue_follower_with_epoch("q", 0, None, 7)
            .await
            .unwrap();
        st.verify_queue_learner_caught_up("q", 0, None, 7, 1)
            .await
            .unwrap();
        assert_eq!(
            st.installed_queue_reseed_cut(&receipt, [41; 32]).unwrap(),
            Some((7, 1))
        );
        assert!(
            st.prepare_queue_reseed_storage(receipt.clone(), [41; 32])
                .await
                .is_err()
        );
        st.install_queue_reseed_storage(receipt, [41; 32], 7, 1)
            .await
            .unwrap();
        let ticket = st.queue_handle("q", 0, None).await.unwrap();
        let h = ticket.resolve().unwrap();
        assert!(h.optional_message_log().is_none());
        assert_eq!(
            fetch_payload(&h, 0, false).unwrap().unwrap().payload,
            vec![42; 2048]
        );
    }
    st.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_reseed_preserves_original_and_only_switches_complete_unsealed_history() {
    for seal_during_copy in [false, true] {
        let dir = keratin_log::test_dir!("combined_reseed_switch");
        combined_reseed_case(&dir.root, seal_during_copy).await;
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_reseed_crash_child() {
    let Some(root) = std::env::var_os("STROMA_COMBINED_RESEED_CRASH_ROOT") else {
        return;
    };
    combined_reseed_case(Path::new(&root), false).await;
    panic!("child did not stop at the requested reseed boundary");
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_reseed_sigkill_preserves_recoverable_payloads_on_either_route() {
    struct KillOnDrop(std::process::Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    for boundary in ["reseed_before_switch", "reseed_retired", "reseed_switched"] {
        let dir = keratin_log::test_dir!("combined_reseed_sigkill");
        let ready = dir.root.join("tmp/child-ready");
        let mut child = KillOnDrop(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "stroma::combined::tests::combined_reseed_crash_child",
                    "--nocapture",
                ])
                .env("STROMA_COMBINED_RESEED_CRASH_ROOT", &dir.root)
                .env("STROMA_INSTALL_CRASH_READY", &ready)
                .env("STROMA_INSTALL_CRASH_BOUNDARY", boundary)
                .stdout(std::process::Stdio::null())
                .spawn()
                .unwrap(),
        );
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            while !ready.exists() {
                assert!(
                    child.0.try_wait().unwrap().is_none(),
                    "child exited before {boundary}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        child.0.kill().unwrap();
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(child.0.wait().unwrap().signal(), Some(9));
        let st = open(&dir.root).await;
        let request = RecoverySealRequest {
            transition: [49; 32],
            fence_epoch: 8,
        };
        let seal = st
            .seal_replica_for_recovery("q", 0, None, request.clone())
            .await
            .unwrap();
        assert_eq!(seal.history.queue_log().unwrap().next, 1, "{boundary}");
        let page = st
            .read_sealed_replica(
                "q",
                0,
                None,
                PartitionKind::Queue,
                RecoveryReadRequest {
                    seal: request,
                    history_id: seal.history.id,
                    source: RecoveryReadSource::Log,
                    from: 0,
                    max_records: 8,
                    max_bytes: 65536,
                },
            )
            .await
            .unwrap();
        assert_eq!(page.records.len(), 1);
        let StromaEvent::PayloadEnqueue { payload, .. } =
            StromaEvent::decode(&page.records[0].payload).unwrap()
        else {
            panic!("retained event is not a payload enqueue at {boundary}");
        };
        assert_eq!(payload, vec![42; 2048], "{boundary}");
        st.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_direct_settlement_paths_leave_no_metadata_frontier_holes() {
    let dir = keratin_log::test_dir!("combined_direct_settlements");
    let st = open(&dir.root.join("owner")).await;
    assert_eq!(publish(&st, false).await, 0);
    assert_eq!(publish(&st, false).await, 1);
    let delivered = st
        .poll_ready("q", 0, None, 2, u64::MAX - 1, u64::MAX)
        .await
        .unwrap();
    assert_eq!(delivered.iter().map(|m| m.0).collect::<Vec<_>>(), [0, 1]);
    let (completion, rx) = KeratinAppendCompletion::pair();
    st.nack_enqueue("q", 0, None, 0, true, completion)
        .await
        .unwrap();
    rx.await.unwrap().unwrap();
    let (completion, rx) = KeratinAppendCompletion::pair();
    st.release_inflight_many("q", 0, None, vec![AckEventMeta { off: 1 }], completion)
        .await
        .unwrap();
    rx.await.unwrap().unwrap();
    ack(&st, 0).await;
    ack(&st, 1).await;
    // The default discard policy takes NACK's second durable commit path.
    let discarded = publish(&st, false).await;
    let (completion, rx) = KeratinAppendCompletion::pair();
    st.nack_enqueue("q", 0, None, discarded, false, completion)
        .await
        .unwrap();
    rx.await.unwrap().unwrap();
    let q = st.queue_handle("q", 0, None).await.unwrap();
    let h = q.resolve().unwrap();
    let next = h.event_log().next_offset();
    assert_eq!(next, discarded + 3);
    assert_eq!(h.work_queue().unwrap().settled_until().await, next);
    let crate::OwnerReplicationRead::Batch(batch) = st
        .read_owner_queue_records("q", 0, None, 0, 100, usize::MAX)
        .await
        .unwrap()
    else {
        panic!("retained history")
    };
    let follower = open(&dir.root.join("follower")).await;
    follower
        .become_queue_follower_with_epoch("q", 0, None, batch.epoch)
        .await
        .unwrap();
    follower
        .apply_replicated_queue_batch(
            "q",
            0,
            None,
            ReplicatedEventBatch {
                epoch: batch.epoch,
                first_offset: 0,
                events: batch.records.into_iter().map(|(_, event)| event).collect(),
                durability: Some(KDurability::AfterFsync),
                encoded: None,
            },
        )
        .await
        .unwrap();
    let fq = follower.queue_handle("q", 0, None).await.unwrap();
    let fh = fq.resolve().unwrap();
    assert_eq!(fh.work_queue().unwrap().settled_until().await, next);
    let mut owner_state = crate::QueueInternalState::new("q".into(), 0);
    owner_state
        .load_snapshot(&h.force_encode_snapshot(next - 1).await.unwrap())
        .unwrap();
    let mut replica_state = crate::QueueInternalState::new("q".into(), 0);
    replica_state
        .load_snapshot(&fh.force_encode_snapshot(next - 1).await.unwrap())
        .unwrap();
    assert_eq!(
        owner_state.recovery_state_digest(),
        replica_state.recovery_state_digest()
    );
    follower.shutdown().await.unwrap();
    drop(fh);
    drop(fq);
    drop(follower);
    drop(h);
    drop(q);
    st.shutdown().await.unwrap();
    drop(st);
    let st = open(&dir.root.join("owner")).await;
    let q = st.queue_handle("q", 0, None).await.unwrap();
    let h = q.resolve().unwrap();
    assert_eq!(h.work_queue().unwrap().settled_until().await, next);
    st.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_owner_rejects_future_references_before_mutation() {
    let dir = keratin_log::test_dir!("combined_owner_reference_preflight");
    let st = open(&dir.root).await;
    let (completion, rx) = KeratinAppendCompletion::pair();
    assert!(st.ack_enqueue("q", 0, None, 0, completion).await.is_err());
    assert!(rx.await.is_err());
    assert_eq!(publish(&st, false).await, 0);
    let q = st.queue_handle("q", 0, None).await.unwrap();
    let h = q.resolve().unwrap();
    for invalid in [1, u64::MAX] {
        let (completion, rx) = KeratinAppendCompletion::pair();
        assert!(
            st.ack_enqueue_many(
                "q",
                0,
                None,
                vec![AckEventMeta { off: 0 }, AckEventMeta { off: invalid }],
                completion
            )
            .await
            .is_err()
        );
        assert!(rx.await.is_err());
        let (completion, rx) = KeratinAppendCompletion::pair();
        assert!(
            st.nack_enqueue_many(
                "q",
                0,
                None,
                vec![
                    NackEventMeta {
                        off: 0,
                        requeue: true,
                        not_before: None
                    },
                    NackEventMeta {
                        off: invalid,
                        requeue: true,
                        not_before: None
                    },
                ],
                completion
            )
            .await
            .is_err()
        );
        assert!(rx.await.is_err());
        let (completion, rx) = KeratinAppendCompletion::pair();
        assert!(
            st.release_inflight_many(
                "q",
                0,
                None,
                vec![AckEventMeta { off: invalid }],
                completion
            )
            .await
            .is_err()
        );
        assert!(rx.await.is_err());
        assert!(
            st.append_events_durable(
                "q",
                0,
                None,
                vec![
                    StromaEvent::Ack { off: 0 },
                    StromaEvent::CancelEnqueueMany {
                        offs: vec![invalid]
                    },
                ],
                KDurability::AfterFsync
            )
            .await
            .is_err()
        );
        assert_eq!(h.event_log().next_offset(), 1);
        assert_eq!(h.work_queue().unwrap().retries(0).await, 0);
        assert_eq!(h.work_queue().unwrap().settled_until().await, 0);
    }
    ack(&st, 0).await;
    assert_eq!(publish(&st, false).await, 2);
    assert_eq!(
        st.poll_ready("q", 0, None, 10, u64::MAX - 1, u64::MAX)
            .await
            .unwrap()
            .iter()
            .map(|m| m.0)
            .collect::<Vec<_>>(),
        [2]
    );
    st.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_follower_rejects_entire_invalid_frame_before_dirty_fence() {
    let dir = keratin_log::test_dir!("combined_follower_reference_preflight");
    let st = open(&dir.root).await;
    st.become_queue_follower_with_epoch("q", 0, None, 7)
        .await
        .unwrap();
    let q = st.queue_handle("q", 0, None).await.unwrap();
    let h = q.resolve().unwrap();
    let payload = StromaEvent::PayloadEnqueue {
        not_before: None,
        expire_at: None,
        headers: headers().encode().unwrap(),
        payload: vec![42],
    };
    for invalid in [
        StromaEvent::Ack { off: 1 },
        StromaEvent::CancelEnqueueMany {
            offs: vec![u64::MAX],
        },
    ] {
        let error = st
            .apply_replicated_queue_batch(
                "q",
                0,
                None,
                ReplicatedEventBatch {
                    epoch: 7,
                    first_offset: 0,
                    events: vec![payload.clone(), invalid],
                    durability: Some(KDurability::AfterFsync),
                    encoded: None,
                },
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("forward payload reference"),
            "{error}"
        );
        assert_eq!(h.event_log().next_offset(), 0);
        assert_eq!(h.ordered_applied_next().unwrap(), Some(0));
        assert!(!*h.follower_apply_state().await);
    }
    assert!(
        st.apply_replicated_queue_batch(
            "q",
            0,
            None,
            ReplicatedEventBatch {
                epoch: 7,
                first_offset: u64::MAX,
                events: vec![payload.clone()],
                durability: Some(KDurability::AfterFsync),
                encoded: None,
            },
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("offset exhausted")
    );
    assert_eq!(h.event_log().next_offset(), 0);
    for _ in 0..2 {
        let batch = ReplicatedEventBatch {
            epoch: 7,
            first_offset: 0,
            events: vec![
                payload.clone(),
                StromaEvent::Nack {
                    off: 0,
                    requeue: true,
                },
            ],
            durability: Some(KDurability::AfterFsync),
            encoded: None,
        };
        st.apply_replicated_queue_batch("q", 0, None, batch)
            .await
            .unwrap();
    }
    assert_eq!(h.event_log().next_offset(), 2);
    assert_eq!(h.ordered_applied_next().unwrap(), Some(2));
    assert_eq!(h.work_queue().unwrap().retries(0).await, 1);
    assert_eq!(
        fetch_payload(&h, 0, false).unwrap().unwrap().payload,
        vec![42]
    );
    st.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_open_group_keeps_dirty_fence_when_later_frame_is_invalid() {
    use futures::StreamExt;
    let dir = keratin_log::test_dir!("combined_open_reference_preflight");
    let st = open(&dir.root).await;
    st.become_queue_follower_with_epoch("q", 0, None, 7)
        .await
        .unwrap();
    let first = ReplicatedEventBatch {
        epoch: 7,
        first_offset: 0,
        durability: None,
        events: vec![StromaEvent::PayloadEnqueue {
            not_before: None,
            expire_at: None,
            headers: headers().encode().unwrap(),
            payload: vec![42],
        }],
        encoded: None,
    };
    let invalid = || ReplicatedEventBatch {
        epoch: 7,
        first_offset: 1,
        durability: None,
        events: vec![StromaEvent::Ack { off: 1 }],
        encoded: None,
    };
    let frames = futures::stream::iter([Ok(first), Ok(invalid())]).boxed();
    let error = st
        .apply_open_replicated_queue_batch("q", 0, None, frames)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("forward payload reference"),
        "{error}"
    );
    let q = st.queue_handle("q", 0, None).await.unwrap();
    let h = q.resolve().unwrap();
    assert_eq!(h.event_log().next_offset(), 1);
    assert_eq!(h.ordered_applied_next().unwrap(), Some(0));
    assert!(*h.follower_apply_state().await);
    assert!(
        st.apply_replicated_queue_batch("q", 0, None, invalid())
            .await
            .unwrap_err()
            .to_string()
            .contains("incomplete follower apply")
    );
    st.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_queue_and_stream_replicate_retain_and_reopen_independently() {
    use crate::OwnerReplicationRead;
    let dir = keratin_log::test_dir!("combined_mixed_stream");
    let owner = open(&dir.root.join("owner")).await;
    let follower = open(&dir.root.join("follower")).await;
    owner
        .create_stream(
            "s",
            0,
            Some(RetentionConfig {
                max_records: Some(3),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    follower.create_stream("s", 0, None).await.unwrap();
    let delayed = publish(&owner, true).await;
    for i in 0..12u8 {
        let ready = publish(&owner, false).await;
        ack(&owner, ready).await;
        assert_eq!(
            owner
                .append_stream_record("s", 0, &headers(), vec![i; 2048])
                .await
                .unwrap(),
            i as u64
        );
    }
    owner
        .commit_stream_cursor("s", 0, "reader", 1)
        .await
        .unwrap();
    let OwnerReplicationRead::Batch(messages) = owner
        .read_owner_stream_records("s", 0, 0, 100, usize::MAX)
        .await
        .unwrap()
    else {
        panic!("stream messages retained")
    };
    let OwnerReplicationRead::Batch(events) = owner
        .read_owner_stream_cursor_events("s", 0, 0, 100, usize::MAX)
        .await
        .unwrap()
    else {
        panic!("stream events retained")
    };
    assert_eq!(messages.records.len(), 12);
    assert_eq!(events.records.len(), 1);
    follower
        .become_stream_follower_with_epoch("s", 0, events.epoch)
        .await
        .unwrap();
    follower
        .apply_replicated_stream_batch(
            "s",
            0,
            Some(ReplicatedMessageBatch {
                epoch: messages.epoch,
                first_offset: 0,
                records: messages
                    .records
                    .into_iter()
                    .map(|(_, message)| message)
                    .collect(),
                durability: Some(KDurability::AfterFsync),
            }),
            Some(ReplicatedEventBatch {
                epoch: events.epoch,
                first_offset: 0,
                events: events.records.into_iter().map(|(_, event)| event).collect(),
                durability: Some(KDurability::AfterFsync),
                encoded: None,
            }),
        )
        .await
        .unwrap();

    let OwnerReplicationRead::Batch(events) = owner
        .read_owner_queue_records("q", 0, None, 0, 100, usize::MAX)
        .await
        .unwrap()
    else {
        panic!("queue events retained")
    };
    follower
        .become_queue_follower_with_epoch("q", 0, None, events.epoch)
        .await
        .unwrap();
    follower
        .apply_replicated_queue_batch(
            "q",
            0,
            None,
            ReplicatedEventBatch {
                epoch: events.epoch,
                first_offset: 0,
                events: events.records.into_iter().map(|(_, event)| event).collect(),
                durability: Some(KDurability::AfterFsync),
                encoded: None,
            },
        )
        .await
        .unwrap();

    // Queue compaction cannot discard its old delayed payload. Stream retention
    // advances independently and replicates its head and cursor clamp.
    let q = owner.queue_handle("q", 0, None).await.unwrap();
    owner.write_exact_queue_checkpoint(q, true).await.unwrap();
    assert_eq!(
        owner
            .truncate_queue_before("q", 0, None, u64::MAX)
            .await
            .unwrap(),
        0
    );
    let head = owner
        .enforce_stream_retention("s", 0)
        .await
        .unwrap()
        .unwrap();
    assert!(head > 0 && head <= 9);
    let OwnerReplicationRead::Batch(events) = owner
        .read_owner_stream_cursor_events("s", 0, 1, 100, usize::MAX)
        .await
        .unwrap()
    else {
        panic!("retention event retained")
    };
    follower
        .apply_replicated_stream_batch(
            "s",
            0,
            None,
            Some(ReplicatedEventBatch {
                epoch: events.epoch,
                first_offset: 1,
                events: events.records.into_iter().map(|(_, event)| event).collect(),
                durability: Some(KDurability::AfterFsync),
                encoded: None,
            }),
        )
        .await
        .unwrap();
    for st in [&owner, &follower] {
        let q = st.queue_handle("q", 0, None).await.unwrap();
        let h = q.resolve().unwrap();
        assert!(h.has_combined_log());
        assert_eq!(h.physical_logs().count(), 1);
        assert!(!st.msg_tp_part_dir("q", 0, None).exists());
        assert_eq!(
            st.fetch_message_by_offset(&h, delayed)
                .await
                .unwrap()
                .unwrap()
                .payload,
            vec![42; 2048]
        );
        assert_eq!(
            h.work_queue().unwrap().lowest_not_settled_offset().await,
            delayed
        );
        let s = st.queue_handle("s", 0, None).await.unwrap();
        let h = s.resolve().unwrap();
        assert!(!h.has_combined_log());
        assert_eq!(h.physical_logs().count(), 2);
        // Follower append batches may have different segment boundaries.
        assert!(h.msg_log().unwrap().head_offset() <= head);
        assert_eq!(st.stream_head_tail("s", 0).await.unwrap(), (head, 12));
        assert_eq!(
            st.stream_cursor("s", 0, "reader").await.unwrap(),
            Some(head)
        );
        assert_eq!(
            st.fetch_message_by_offset(&h, 11)
                .await
                .unwrap()
                .unwrap()
                .payload,
            vec![11; 2048]
        );
        st.snapshot_partition("s", 0, None).await.unwrap();
        st.truncate_partition_log(s, 1).await.unwrap();
    }
    owner.shutdown().await.unwrap();
    follower.shutdown().await.unwrap();
    drop(owner);
    drop(follower);
    for node in ["owner", "follower"] {
        let st = open(&dir.root.join(node)).await;
        assert_eq!(st.partition_kind("q", 0, None), PartitionKind::Queue);
        assert_eq!(st.partition_kind("s", 0, None), PartitionKind::Stream);
        let q = st.queue_handle("q", 0, None).await.unwrap();
        let h = q.resolve().unwrap();
        assert!(h.has_combined_log());
        assert_eq!(
            h.work_queue().unwrap().lowest_not_settled_offset().await,
            delayed
        );
        assert!(
            st.fetch_message_by_offset(&h, delayed)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(st.stream_head_tail("s", 0).await.unwrap(), (head, 12));
        assert_eq!(
            st.stream_cursor("s", 0, "reader").await.unwrap(),
            Some(head)
        );
        let records = st.read_stream_records("s", 0, head, 100).await.unwrap();
        assert_eq!(records.len() as u64, 12 - head);
        for (off, payload, _) in records {
            assert_eq!(payload, vec![off as u8; 2048]);
        }
        st.commit_stream_cursor("s", 0, "reader", 12).await.unwrap();
        assert_eq!(st.stream_cursor("s", 0, "reader").await.unwrap(), Some(12));
        st.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn combined_root_preserves_stream_preparation_and_seal_layout() {
    use crate::{RecoverySealRequest, StorageHistoryBinding};
    let dir = keratin_log::test_dir!("combined_stream_preparation");
    let st = open(&dir.root).await;
    st.initialize_empty_storage_history(
        "s",
        0,
        None,
        PartitionKind::Stream,
        StorageHistoryBinding {
            resource_incarnation: [1; 16],
            accepted_history: [2; 16],
            writer_session: [3; 16],
        },
    )
    .await
    .unwrap();
    st.append_stream_record("s", 0, &headers(), b"stream payload".to_vec())
        .await
        .unwrap();
    st.commit_stream_cursor("s", 0, "reader", 1).await.unwrap();
    // Exercise the cold physical-log open used for sealing an evicted stream.
    st.unmaterialize("s", 0, None).await.unwrap();
    let sealed = st
        .seal_replica_for_recovery(
            "s",
            0,
            None,
            RecoverySealRequest {
                transition: [9; 32],
                fence_epoch: 8,
            },
        )
        .await
        .unwrap();
    assert!(sealed.history.storage_history.as_ref().unwrap().stream);
    assert!(matches!(
        sealed.history.logs,
        crate::RetainedLogs::Stream { records, cursor_events } if records.next == 1 && cursor_events.next == 1
    ));
    st.shutdown().await.unwrap();
}

/// Disk amplification probe, independent of failover timing. One delayed oldest
/// payload pins increasingly long settled histories. All disposable data is
/// owned by the test directory and removed on return.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn sparse_live_retention_probe() {
    fn file_bytes(root: &Path) -> u64 {
        std::fs::read_dir(root)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    file_bytes(&entry.path())
                } else {
                    entry.metadata().unwrap().len()
                }
            })
            .sum()
    }
    for count in [1_000usize, 10_000, 50_000] {
        let dir = keratin_log::test_dir!("sparse_live_retention_probe");
        let mut cfg = KeratinConfig::test_default();
        cfg.segment_max_bytes = 1024 * 1024;
        cfg.segment_preallocate_bytes = 0;
        cfg.tail_cache_bytes = 0;
        let st = Stroma::open_with_options(
            &dir.root,
            StromaKeratinConfig::from_message_log(cfg),
            SnapshotConfig {
                every_events: u64::MAX,
            },
            StromaOptions::default(),
        )
        .await
        .unwrap();
        let held = publish(&st, true).await;
        for start in (0..count).step_by(256) {
            let mut completions = Vec::new();
            let mut batch = Vec::new();
            for _ in start..(start + 256).min(count) {
                let (completion, rx) = KeratinAppendCompletion::pair();
                completions.push(rx);
                batch.push(PublishItem {
                    headers: headers(),
                    payload: vec![17; 1024],
                    completion,
                    not_before: None,
                    expire_at: None,
                });
            }
            st.append_message_batch("q", 0, None, batch).await.unwrap();
            let mut reqs = Vec::new();
            for rx in completions {
                reqs.push(AckEventMeta {
                    off: rx.await.unwrap().unwrap().base_offset,
                });
            }
            let (completion, rx) = KeratinAppendCompletion::pair();
            st.ack_enqueue_many("q", 0, None, reqs, completion)
                .await
                .unwrap();
            rx.await.unwrap().unwrap();
        }
        let q = st.queue_handle("q", 0, None).await.unwrap();
        st.write_exact_queue_checkpoint(q.clone(), true)
            .await
            .unwrap();
        let h = q.resolve().unwrap();
        assert_eq!(
            h.work_queue().unwrap().lowest_not_settled_offset().await,
            held
        );
        assert_eq!(h.event_log().head_offset(), 0);
        let retained_records = h.event_log().next_offset();
        assert_eq!(
            fetch_payload(&h, held, false).unwrap().unwrap().payload,
            vec![42; 2048]
        );
        let pinned_bytes = file_bytes(&dir.root);
        drop(h);
        ack(&st, held).await;
        st.write_exact_queue_checkpoint(q.clone(), true)
            .await
            .unwrap();
        let head_after = q.resolve().unwrap().event_log().head_offset();
        assert!(head_after > held);
        let reclaimed_bytes = file_bytes(&dir.root);
        println!(
            "retention_probe settled={count} live_payload_bytes=2048 retained_records={retained_records} pinned_file_bytes={pinned_bytes} released_file_bytes={reclaimed_bytes} head_after={head_after}"
        );
        assert!(reclaimed_bytes < pinned_bytes);
        st.shutdown().await.unwrap();
        drop(q);
        drop(st);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_log_queue_api_and_stream_api_refuse_the_other_kind() {
    use crate::OwnerReplicationRead;
    use futures::StreamExt;
    let dir = keratin_log::test_dir!("combined_single_log_kind_guards");
    let owner = open(&dir.root.join("owner")).await;
    let follower = open(&dir.root.join("follower")).await;
    owner.create_stream("s", 0, None).await.unwrap();
    owner
        .ensure_queue_owner_epoch("q", 0, None, Some(3))
        .await
        .unwrap();
    owner
        .append_stream_record("s", 0, &headers(), vec![1; 16])
        .await
        .unwrap();
    assert_eq!(publish(&owner, false).await, 0);
    assert_eq!(publish(&owner, false).await, 1);

    let wrong_kind = |e: StromaError| matches!(e, StromaError::WrongPartitionKind { .. });
    assert!(wrong_kind(
        owner
            .read_owner_queue_records("s", 0, None, 0, 10, usize::MAX)
            .await
            .unwrap_err()
    ));
    assert!(wrong_kind(
        owner
            .read_owner_stream_records("q", 0, 0, 10, usize::MAX)
            .await
            .unwrap_err()
    ));
    assert!(wrong_kind(
        owner
            .read_owner_stream_cursor_events("q", 0, 0, 10, usize::MAX)
            .await
            .unwrap_err()
    ));

    // One cursor covers payloads and state: both publishes come back from
    // the single queue log with contiguous offsets.
    let OwnerReplicationRead::Batch(batch) = owner
        .read_owner_queue_records("q", 0, None, 0, 10, usize::MAX)
        .await
        .unwrap()
    else {
        panic!("queue log retained")
    };
    assert_eq!(
        (batch.epoch, batch.requested_offset, batch.next_offset),
        (3, 0, 2)
    );
    assert!(
        batch
            .records
            .iter()
            .all(|(_, e)| matches!(e, StromaEvent::PayloadEnqueue { .. }))
    );

    follower
        .become_stream_follower_with_epoch("s", 0, 3)
        .await
        .unwrap();
    follower
        .become_queue_follower_with_epoch("q", 0, None, 3)
        .await
        .unwrap();
    let events: Vec<_> = batch.records.into_iter().map(|(_, e)| e).collect();
    let queue_batch = |first_offset, events: Vec<StromaEvent>| ReplicatedEventBatch {
        epoch: 3,
        first_offset,
        events,
        durability: Some(KDurability::AfterFsync),
        encoded: None,
    };
    assert!(wrong_kind(
        follower
            .apply_replicated_queue_batch("s", 0, None, queue_batch(0, events[..1].to_vec()))
            .await
            .unwrap_err()
    ));
    let frames = futures::stream::iter([Ok(queue_batch(0, events[..1].to_vec()))]).boxed();
    assert!(wrong_kind(
        follower
            .apply_open_replicated_queue_batch("s", 0, None, frames)
            .await
            .unwrap_err()
    ));
    assert!(wrong_kind(
        follower
            .apply_replicated_stream_batch("q", 0, None, None)
            .await
            .unwrap_err()
    ));
    assert!(wrong_kind(
        follower
            .queue_replication_next_offset("s", 0, None)
            .await
            .unwrap_err()
    ));

    // Empty single-log batches are refused instead of being treated as progress.
    let empty = follower
        .apply_replicated_queue_batch("q", 0, None, queue_batch(0, vec![]))
        .await;
    assert!(matches!(empty, Err(StromaError::InvalidArgument(_))));
    let frames = futures::stream::iter([Ok(queue_batch(0, vec![]))]).boxed();
    assert!(
        follower
            .apply_open_replicated_queue_batch("q", 0, None, frames)
            .await
            .is_err()
    );
    let q = follower.queue_handle("q", 0, None).await.unwrap();
    let h = q.resolve().unwrap();
    assert!(
        !*h.follower_apply_state().await,
        "a refused empty frame must not set the dirty fence"
    );
    drop(h);
    drop(q);

    let outcome = follower
        .apply_replicated_queue_batch("q", 0, None, queue_batch(0, events[..1].to_vec()))
        .await
        .unwrap();
    assert!(
        matches!(outcome, ReplicatedAppendOutcome::Applied(r) if r.base_offset == 0 && r.count == 1)
    );
    let frames = futures::stream::iter([Ok(queue_batch(1, events[1..].to_vec()))]).boxed();
    let outcomes = follower
        .apply_open_replicated_queue_batch("q", 0, None, frames)
        .await
        .unwrap();
    assert!(
        matches!(outcomes.as_slice(), [ReplicatedAppendOutcome::Applied(r)] if r.base_offset == 1)
    );
    assert_eq!(
        follower
            .queue_replication_next_offset("q", 0, None)
            .await
            .unwrap(),
        2
    );
    owner.shutdown().await.unwrap();
    follower.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_commits_name_one_offset_and_one_epoch_in_the_single_log() {
    let dir = keratin_log::test_dir!("combined_single_log_commits");
    let st = open(&dir.root).await;
    st.ensure_queue_owner_epoch("q", 0, None, Some(5))
        .await
        .unwrap();
    st.create_stream("s", 0, None).await.unwrap();
    assert!(matches!(
        st.queue_durable_commit("s", 0, None).await,
        Err(StromaError::WrongPartitionKind { .. })
    ));
    assert_eq!(
        st.queue_durable_commit("q", 0, None).await.unwrap(),
        QueuePublishCommit {
            next: 0,
            epoch: 5,
            buffered: false
        }
    );
    assert_eq!(publish(&st, false).await, 0);

    // A two-record publish commits once, at the end of its last record.
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut completions = Vec::new();
    let items = (0..2)
        .map(|_| {
            let (completion, done) = KeratinAppendCompletion::pair();
            completions.push(done);
            PublishItem {
                headers: headers(),
                payload: vec![7; 64],
                completion,
                not_before: None,
                expire_at: None,
            }
        })
        .collect();
    st.append_message_batch_observed(
        "q",
        0,
        None,
        items,
        Some(Box::new(move |c| {
            let _ = tx.send(c);
        })),
    )
    .await
    .unwrap();
    let commit = rx.await.unwrap();
    for done in completions {
        done.await.unwrap().unwrap();
    }
    assert_eq!(
        commit,
        QueuePublishCommit {
            next: 3,
            epoch: 5,
            buffered: false
        }
    );
    let durable = st.queue_durable_commit("q", 0, None).await.unwrap();
    assert_eq!(durable, commit);
    st.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_and_stream_role_calls_refuse_the_other_kind() {
    let dir = keratin_log::test_dir!("combined_role_calls_refuse_other_kind");
    let st = open(&dir.root).await;
    st.ensure_queue_owner_epoch("q", 0, None, Some(1))
        .await
        .unwrap();
    st.create_stream("s", 0, None).await.unwrap();
    let wrong_kind =
        |result: Result<()>| matches!(result, Err(StromaError::WrongPartitionKind { .. }));
    assert!(wrong_kind(
        st.export_queue_state_checkpoint("s", 0, None)
            .await
            .map(|_| ())
    ));
    assert!(wrong_kind(
        st.verify_queue_learner_caught_up("s", 0, None, 0, 0)
            .await
            .map(|_| ())
    ));
    assert!(wrong_kind(
        st.promote_queue_follower_if_caught_up("s", 0, None, 0)
            .await
            .map(|_| ())
    ));
    assert!(wrong_kind(
        st.promote_queue_follower_to_local_tail("s", 0, None, 1)
            .await
            .map(|_| ())
    ));
    assert!(wrong_kind(
        st.demote_queue_owner_to_follower("s", 0, None)
            .await
            .map(|_| ())
    ));
    assert!(wrong_kind(
        st.truncate_queue_before("s", 0, None, 1).await.map(|_| ())
    ));
    assert!(wrong_kind(
        st.promote_stream_follower_to_local_tail("q", 0, 1)
            .await
            .map(|_| ())
    ));
    assert!(wrong_kind(
        st.truncate_stream_records_before("q", 0, 1)
            .await
            .map(|_| ())
    ));
    st.shutdown().await.unwrap();
}

#[tokio::test]
async fn combined_queue_preserves_strongest_flush_policy() {
    for (message, event, expected) in [
        (
            KDurability::AfterWrite,
            KDurability::AfterWrite,
            KDurability::AfterWrite,
        ),
        (
            KDurability::AfterWrite,
            KDurability::AfterFsync,
            KDurability::AfterFsync,
        ),
        (
            KDurability::AfterFsync,
            KDurability::AfterWrite,
            KDurability::AfterFsync,
        ),
        (
            KDurability::AfterWrite,
            KDurability::AfterFlush,
            KDurability::AfterFlush,
        ),
        (
            KDurability::AfterFlush,
            KDurability::AfterWrite,
            KDurability::AfterFlush,
        ),
        (
            KDurability::AfterFlush,
            KDurability::AfterFsync,
            KDurability::AfterFsync,
        ),
    ] {
        let dir = keratin_log::test_dir!("combined_after_flush_policy");
        let mut config = StromaKeratinConfig::from_message_log(KeratinConfig::test_default());
        config.message_log.default_durability = message;
        config.event_log.default_durability = event;
        let st = Stroma::open(&dir.root, config, SnapshotConfig::default())
            .await
            .unwrap();
        assert_eq!(st.queue_event_log_config().default_durability, expected);
        st.shutdown().await.unwrap();
    }
}
