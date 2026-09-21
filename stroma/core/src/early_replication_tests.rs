//! Real written-only owner logs, durable follower logs, and controlled local
//! completion channels. No timing assumption about how quickly the disk fsyncs.
use super::*;
use keratin_log::test_dir;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_can_persist_tentative_bytes_without_releasing_owner_apply() {
    // Exercise offset zero and an established prefix, with either local writer
    // failing or both succeeding. A follower's persistence cannot satisfy either
    // owner completion channel.
    for prefix in [0, 1] {
        for failed_log in [None, Some("message"), Some("event")] {
            let owner_dir = test_dir!("early_owner");
            let follower_dir = test_dir!("early_follower");
            let config = StromaKeratinConfig::from_message_log(KeratinConfig {
                flush_target_bytes: 8,
                ..KeratinConfig::test_default()
            });
            let owner = Stroma::open(&owner_dir.root, config.clone(), SnapshotConfig::default())
                .await
                .unwrap();
            let follower = Stroma::open(&follower_dir.root, config, SnapshotConfig::default())
                .await
                .unwrap();
            let qh = owner.queue_handle("q", 0, None).await.unwrap();
            let h = qh.resolve().unwrap();
            follower.become_queue_follower("q", 0, None).await.unwrap();
            for offset in 0..=prefix {
                let event = StromaEvent::EnqueueMany {
                    reqs: vec![EnqueueEventMeta {
                        off: offset,
                        retries: 0,
                        expire_at: None,
                    }],
                };
                let mode = if offset < prefix {
                    KDurability::AfterFsync
                } else {
                    KDurability::AfterWrite
                };
                let mut written = h.msg_log().subscribe_written();
                h.msg_log()
                    .append_enqueue_receiver(
                        Message {
                            flags: 0,
                            headers: vec![],
                            payload: vec![offset as u8],
                        },
                        Some(mode),
                    )
                    .unwrap()
                    .await
                    .unwrap()
                    .unwrap();
                tokio::time::timeout(Duration::from_secs(5), written.changed())
                    .await
                    .unwrap()
                    .unwrap();
                let mut written = h.event_log().subscribe_written();
                h.event_log()
                    .append_enqueue_receiver(event_msg(&event).unwrap(), Some(mode))
                    .unwrap()
                    .await
                    .unwrap()
                    .unwrap();
                tokio::time::timeout(Duration::from_secs(5), written.changed())
                    .await
                    .unwrap()
                    .unwrap();
            }
            assert_eq!(
                h.msg_log().reader().scan_from(0, 10).unwrap().len() as u64,
                prefix
            );
            assert_eq!(
                h.event_log().reader().scan_from(0, 10).unwrap().len() as u64,
                prefix
            );
            assert!(
                h.msg_log()
                    .reader()
                    .scan_from(prefix, 10)
                    .unwrap()
                    .is_empty()
            );
            assert!(
                h.event_log()
                    .reader()
                    .scan_from(prefix, 10)
                    .unwrap()
                    .is_empty()
            );

            let OwnerReplicationRead::Batch(messages) = owner
                .read_owner_message_records_early("q", 0, None, 0, 10)
                .await
                .unwrap()
            else {
                panic!("message checkpoint");
            };
            let OwnerReplicationRead::Batch(events) = owner
                .read_owner_event_records_early("q", 0, None, 0, 10)
                .await
                .unwrap()
            else {
                panic!("event checkpoint");
            };
            assert_eq!(messages.next_offset, prefix + 1);
            assert_eq!(events.next_offset, prefix + 1);
            follower
                .apply_replicated_queue_batch(
                    "q",
                    0,
                    None,
                    Some(ReplicatedMessageBatch {
                        epoch: messages.epoch,
                        first_offset: 0,
                        records: messages.records.into_iter().map(|(_, m)| m).collect(),
                        durability: Some(KDurability::AfterFsync),
                    }),
                    Some(ReplicatedEventBatch {
                        epoch: events.epoch,
                        first_offset: 0,
                        events: events.records.into_iter().map(|(_, e)| e).collect(),
                        durability: Some(KDurability::AfterFsync),
                    }),
                )
                .await
                .unwrap();
            let fh = follower.queue_handle("q", 0, None).await.unwrap();
            let fh = fh.resolve().unwrap();
            assert_eq!(
                fh.msg_log().reader().scan_from(0, 10).unwrap().len() as u64,
                prefix + 1
            );
            assert_eq!(
                fh.event_log().reader().scan_from(0, 10).unwrap().len() as u64,
                prefix + 1
            );
            assert_eq!(
                h.msg_log().reader().scan_from(0, 10).unwrap().len() as u64,
                prefix
            );

            let (message_tx, message_rx) = tokio::sync::oneshot::channel();
            let (event_tx, event_rx) = tokio::sync::oneshot::channel();
            let apply =
                owner.gate_and_apply_enqueue(qh.clone(), vec![], event_rx, message_rx, 1, 0, None);
            tokio::pin!(apply);
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut apply)
                    .await
                    .is_err()
            );
            let success = || {
                Ok(AppendResult {
                    base_offset: prefix,
                    count: 1,
                })
            };
            event_tx
                .send(if failed_log == Some("event") {
                    Err(IoError::new("injected event fsync failure"))
                } else {
                    success()
                })
                .unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut apply)
                    .await
                    .is_err(),
                "must drain both completions even on failure"
            );
            if failed_log == Some("message") {
                drop(message_tx);
            } else {
                message_tx.send(success()).unwrap();
            }
            let result = apply.await;
            match failed_log {
                None => assert_eq!(result.unwrap(), Some(prefix + 1)),
                Some("message") => assert_eq!(result.unwrap(), None),
                Some("event") => assert!(result.is_err()),
                _ => unreachable!(),
            }
            // Already encoded frames retain the old epoch; an explicit fence
            // causes subsequent owner reads to report the new epoch.
            h.msg_log().advance_epoch(1).await.unwrap();
            let OwnerReplicationRead::Batch(next) = owner
                .read_owner_message_records_early("q", 0, None, prefix + 1, 1)
                .await
                .unwrap()
            else {
                panic!();
            };
            assert_eq!(next.epoch, 1);
            assert!(next.records.is_empty());
            drop(h);
            drop(fh);
            owner.shutdown().await.unwrap();
            follower.shutdown().await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_publish_completion_waits_for_both_owner_writers_after_early_replication() {
    let owner_dir = test_dir!("early_publish_owner");
    let follower_dir = test_dir!("early_publish_follower");
    // Separate write from the commit scheduler deterministically. AdvanceEpoch
    // at the unchanged epoch below drains the real writer's pending fsync acks.
    let config = StromaKeratinConfig::from_message_log(KeratinConfig {
        flush_target_bytes: 8,
        min_fsync_interval_ms: 3_600_000,
        fsync_interval_ms: 3_600_000,
        ..KeratinConfig::test_default()
    });
    let owner = Stroma::open(&owner_dir.root, config, SnapshotConfig::default())
        .await
        .unwrap();
    let follower = Stroma::open(
        &follower_dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    follower.become_queue_follower("q", 0, None).await.unwrap();
    let qh = owner.queue_handle("q", 0, None).await.unwrap();
    let h = qh.resolve().unwrap();
    for offset in 0..2 {
        let (mut message_written, mut event_written) =
            owner.subscribe_owner_written("q", 0, None).await.unwrap();
        let (completion, mut receipt) = KeratinAppendCompletion::pair();
        owner
            .append_message_batch(
                "q",
                0,
                None,
                vec![PublishItem {
                    headers: MessageHeaders {
                        published: Default::default(),
                        publish_received: Default::default(),
                        content_type: None,
                        extra: Default::default(),
                    },
                    payload: vec![offset as u8],
                    completion,
                    not_before: None,
                    expire_at: None,
                }],
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            message_written.changed().await.unwrap();
            event_written.changed().await.unwrap();
        })
        .await
        .unwrap();
        let OwnerReplicationRead::Batch(m) = owner
            .read_owner_message_records_early("q", 0, None, offset, 10)
            .await
            .unwrap()
        else {
            panic!();
        };
        let OwnerReplicationRead::Batch(e) = owner
            .read_owner_event_records_early("q", 0, None, offset, 10)
            .await
            .unwrap()
        else {
            panic!();
        };
        assert_eq!(m.next_offset, offset + 1);
        assert_eq!(e.next_offset, offset + 1);
        follower
            .apply_replicated_queue_batch(
                "q",
                0,
                None,
                Some(ReplicatedMessageBatch {
                    epoch: m.epoch,
                    first_offset: offset,
                    records: m.records.into_iter().map(|(_, m)| m).collect(),
                    durability: Some(KDurability::AfterFsync),
                }),
                Some(ReplicatedEventBatch {
                    epoch: e.epoch,
                    first_offset: offset,
                    events: e.records.into_iter().map(|(_, e)| e).collect(),
                    durability: Some(KDurability::AfterFsync),
                }),
            )
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut receipt)
                .await
                .is_err()
        );
        assert!(!owner.is_ready("q", 0, None, offset).await.unwrap());
        let (first, second) = if offset == 0 {
            (h.event_log(), h.msg_log())
        } else {
            (h.msg_log(), h.event_log())
        };
        first.advance_epoch(0).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut receipt)
                .await
                .is_err(),
            "one owner log plus a fully durable follower cannot confirm"
        );
        second.advance_epoch(0).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), receipt)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .base_offset,
            offset
        );
        assert!(owner.is_ready("q", 0, None, offset).await.unwrap());
    }
    drop(h);
    owner.shutdown().await.unwrap();
    follower.shutdown().await.unwrap();
}
