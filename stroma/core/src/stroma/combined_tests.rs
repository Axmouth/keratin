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
        assert_eq!(h.msg_log().next_offset(), 0);
        assert_eq!(h.work_queue().unwrap().settled_until().await, 1);
        let selected = read_selected(&h, &[(0, 0), (3, 1)]).unwrap();
        assert_eq!(selected.iter().map(|m| m.0).collect::<Vec<_>>(), [0, 3]);
        assert_eq!(selected[1].3, 1);
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
    assert_eq!(q.resolve().unwrap().msg_log().next_offset(), 0);
    assert_eq!(q.resolve().unwrap().msg_log().head_offset(), 0);
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
