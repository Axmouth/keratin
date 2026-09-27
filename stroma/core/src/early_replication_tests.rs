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
                let mut written = h.msg_log().unwrap().subscribe_written();
                h.msg_log().unwrap()
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
                h.msg_log().unwrap().reader().scan_from(0, 10).unwrap().len() as u64,
                prefix
            );
            assert_eq!(
                h.event_log().reader().scan_from(0, 10).unwrap().len() as u64,
                prefix
            );
            assert!(
                h.msg_log().unwrap()
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
                fh.msg_log().unwrap().reader().scan_from(0, 10).unwrap().len() as u64,
                prefix + 1
            );
            assert_eq!(
                fh.event_log().reader().scan_from(0, 10).unwrap().len() as u64,
                prefix + 1
            );
            assert_eq!(
                h.msg_log().unwrap().reader().scan_from(0, 10).unwrap().len() as u64,
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
            h.msg_log().unwrap().advance_epoch(1).await.unwrap();
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
            (h.event_log(), h.msg_log().unwrap())
        } else {
            (h.msg_log().unwrap(), h.event_log())
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

#[cfg(feature = "io-fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_fsync_fault_never_confirms_or_delivers_early_follower_bytes() {
    use keratin_log::io_fault_injection::{Fault, arm};
    for follower_first in [false, true] {
        for prefix in [0, 1] {
            for failed_message in [false, true] {
                for failure_first in [false, true] {
                    for fault in [Fault::Fsync, Fault::FsyncAfterSuccess] {
                        let label = format!(
                            "prefix={prefix} message={failed_message} failure_first={failure_first} fault={fault:?} follower_first={follower_first}"
                        );
                        eprintln!("CASE {label}");
                        let owner_dir = test_dir!("io_fault_owner");
                        let follower_dir = test_dir!("io_fault_follower");
                        let config = StromaKeratinConfig::from_message_log(KeratinConfig {
                            flush_target_bytes: 8,
                            min_fsync_interval_ms: 3_600_000,
                            fsync_interval_ms: 3_600_000,
                            ..KeratinConfig::test_default()
                        });
                        let owner =
                            Stroma::open(&owner_dir.root, config, SnapshotConfig::default())
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
                        for offset in 0..=prefix {
                            let (mut mw, mut ew) =
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
                                mw.changed().await.unwrap();
                                ew.changed().await.unwrap();
                            })
                            .await
                            .unwrap();
                            let OwnerReplicationRead::Batch(m) = owner
                                .read_owner_message_records_early("q", 0, None, offset, 10)
                                .await
                                .unwrap()
                            else {
                                panic!()
                            };
                            let OwnerReplicationRead::Batch(e) = owner
                                .read_owner_event_records_early("q", 0, None, offset, 10)
                                .await
                                .unwrap()
                            else {
                                panic!()
                            };
                            let replication = follower.apply_replicated_queue_batch(
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
                            );
                            tokio::pin!(replication);
                            if follower_first || offset < prefix {
                                replication.as_mut().await.unwrap();
                            }
                            if offset < prefix {
                                h.msg_log().unwrap().advance_epoch(0).await.unwrap();
                                h.event_log().advance_epoch(0).await.unwrap();
                                receipt.await.unwrap().unwrap();
                                continue;
                            }
                            assert!(!owner.is_ready("q", 0, None, offset).await.unwrap());
                            let path = if failed_message {
                                owner.msg_tp_part_dir("q", 0, None)
                            } else {
                                owner.tp_part_dir("q", 0, None)
                            }
                            .join("segments")
                            .join("00000000000000000000.log");
                            let mut cancellation_written = h.event_log().subscribe_written();
                            let armed = arm(&path, fault).unwrap();
                            let (failed, healthy) = if failed_message {
                                (h.msg_log().unwrap(), h.event_log())
                            } else {
                                (h.event_log(), h.msg_log().unwrap())
                            };
                            if failure_first {
                                failed.advance_epoch(0).await.unwrap();
                            } else {
                                healthy.advance_epoch(0).await.unwrap();
                            }
                            assert!(
                                tokio::time::timeout(Duration::from_millis(20), &mut receipt)
                                    .await
                                    .is_err(),
                                "both completions must be drained: {label}"
                            );
                            if failure_first {
                                healthy.advance_epoch(0).await.unwrap();
                            } else {
                                failed.advance_epoch(0).await.unwrap();
                            }
                            assert_eq!(armed.hits(), 1, "{label}");
                            if failed_message {
                                // The payload failure path emits a compensating event.
                                // Its fsync is held by the same deterministic scheduler.
                                tokio::time::timeout(
                                    Duration::from_secs(3),
                                    cancellation_written.changed(),
                                )
                                .await
                                .unwrap()
                                .unwrap();
                                h.event_log().advance_epoch(0).await.unwrap();
                            }
                            let outcome =
                                tokio::time::timeout(Duration::from_secs(3), &mut receipt).await;
                            eprintln!("OUTCOME {label}: {outcome:?}");
                            assert!(outcome.is_ok(), "receipt stalled: {label}");
                            assert!(
                                outcome.unwrap().unwrap().is_err(),
                                "false confirmation: {label}"
                            );
                            assert!(
                                !owner.is_ready("q", 0, None, offset).await.unwrap(),
                                "false delivery: {label}"
                            );
                            assert_eq!(
                                failed.reader().scan_from(0, 10).unwrap().len() as u64,
                                prefix,
                                "failed sync advanced ordinary visibility: {label}"
                            );
                            if !follower_first {
                                replication.as_mut().await.unwrap();
                            }
                            assert_eq!(
                                h.msg_log().unwrap().next_offset(),
                                prefix + 1,
                                "failed offset cannot be reassigned while live"
                            );
                            let fh = follower.queue_handle("q", 0, None).await.unwrap();
                            assert_eq!(
                                fh.resolve()
                                    .unwrap()
                                    .msg_log().unwrap()
                                    .reader()
                                    .scan_from(0, 10)
                                    .unwrap()
                                    .len() as u64,
                                prefix + 1
                            );
                            drop(armed);
                        }
                        drop(h);
                        owner.shutdown().await.unwrap();
                        follower.shutdown().await.unwrap();
                        drop(qh);
                        drop(owner);
                        let reopened = Stroma::open(
                            &owner_dir.root,
                            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
                            SnapshotConfig::default(),
                        )
                        .await
                        .unwrap();
                        for confirmed in 0..prefix {
                            assert!(
                                reopened.is_ready("q", 0, None, confirmed).await.unwrap(),
                                "confirmed prefix lost: {label}"
                            );
                        }
                        eprintln!(
                            "REOPEN {label}: uncertain_ready={}",
                            reopened.is_ready("q", 0, None, prefix).await.unwrap()
                        );
                        reopened.shutdown().await.unwrap();
                    }
                }
            }
        }
    }
}

#[cfg(feature = "io-fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_write_fault_keeps_partial_bytes_out_of_early_replication() {
    use keratin_log::io_fault_injection::{Fault, arm_repeated};
    for prefix in [0, 1] {
        for failed_message in [false, true] {
            for fault in [Fault::Write, Fault::PartialWrite] {
                let label = format!("prefix={prefix} message={failed_message} fault={fault:?}");
                eprintln!("WRITE CASE {label}");
                let dir = test_dir!("write_fault_owner");
                let config = StromaKeratinConfig::from_message_log(KeratinConfig {
                    flush_target_bytes: 8,
                    fsync_interval_ms: 20,
                    min_fsync_interval_ms: 5,
                    ..KeratinConfig::test_default()
                });
                let owner = Stroma::open(&dir.root, config, SnapshotConfig::default())
                    .await
                    .unwrap();
                let qh = owner.queue_handle("q", 0, None).await.unwrap();
                let h = qh.resolve().unwrap();
                for offset in 0..=prefix {
                    let path = if failed_message {
                        owner.msg_tp_part_dir("q", 0, None)
                    } else {
                        owner.tp_part_dir("q", 0, None)
                    }
                    .join("segments")
                    .join("00000000000000000000.log");
                    let armed =
                        (offset == prefix).then(|| arm_repeated(&path, fault, usize::MAX).unwrap());
                    let (completion, receipt) = KeratinAppendCompletion::pair();
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
                                payload: vec![offset as u8; 128],
                                completion,
                                not_before: None,
                                expire_at: None,
                            }],
                        )
                        .await
                        .unwrap();
                    let outcome = tokio::time::timeout(Duration::from_secs(5), receipt).await;
                    eprintln!("WRITE OUTCOME {label}: {outcome:?}");
                    let outcome = outcome.expect("write error must complete receipt").unwrap();
                    if offset < prefix {
                        outcome.unwrap();
                        continue;
                    }
                    assert!(outcome.is_err(), "false confirmation: {label}");
                    assert!(armed.as_ref().unwrap().hits() > 0);
                    assert!(!owner.is_ready("q", 0, None, offset).await.unwrap());
                    let failed = if failed_message {
                        h.msg_log().unwrap()
                    } else {
                        h.event_log()
                    };
                    assert_eq!(
                        failed.reader().scan_from(0, 10).unwrap().len() as u64,
                        prefix
                    );
                    if failed_message {
                        let OwnerReplicationRead::Batch(batch) = owner
                            .read_owner_message_records_early("q", 0, None, prefix, 10)
                            .await
                            .unwrap()
                        else {
                            panic!()
                        };
                        assert!(
                            batch.records.is_empty(),
                            "partial write entered tentative cache"
                        );
                    } else {
                        let OwnerReplicationRead::Batch(batch) = owner
                            .read_owner_event_records_early("q", 0, None, prefix, 10)
                            .await
                            .unwrap()
                        else {
                            panic!()
                        };
                        assert!(
                            batch.records.is_empty(),
                            "partial write entered tentative cache"
                        );
                    }
                    assert_eq!(h.msg_log().unwrap().next_offset(), prefix + 1);
                    drop(armed);
                }
                let shutdown = owner.shutdown().await;
                if let Err(error) = shutdown {
                    // Immediate staging fences the writer on a buffer-write error.
                    // Shutdown reports that failure and still drains the other log.
                    assert_eq!(
                        std::env::var("KERATIN_STAGE_THEN_COMMIT").as_deref(),
                        Ok("1"),
                        "unexpected shutdown error: {error:?} ({label})"
                    );
                    assert!(
                        matches!(error, crate::StromaError::Io(ref message) if message == "writer gone" || message == "writer dropped"),
                        "unexpected shutdown error: {error:?} ({label})"
                    );
                }
                for log in [h.event_log(), h.msg_log().unwrap()] {
                    let error = tokio::time::timeout(Duration::from_secs(5), log.sync())
                        .await
                        .expect("shutdown must finish both writers")
                        .expect_err("shutdown must close both writers");
                    assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe, "{label}");
                }
                drop(h);
                drop(qh);
                drop(owner);
                let reopened = Stroma::open(
                    &dir.root,
                    StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
                    SnapshotConfig::default(),
                )
                .await
                .unwrap();
                for confirmed in 0..prefix {
                    assert!(
                        reopened.is_ready("q", 0, None, confirmed).await.unwrap(),
                        "confirmed prefix lost: {label}"
                    );
                }
                eprintln!(
                    "WRITE REOPEN {label}: uncertain_ready={}",
                    reopened.is_ready("q", 0, None, prefix).await.unwrap()
                );
                reopened.shutdown().await.unwrap();
            }
        }
    }
}

#[cfg(feature = "io-fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_io_fault_process_exit_preserves_confirmed_prefix() {
    use keratin_log::io_fault_injection::{Fault, arm_repeated};
    if let Ok(root) = std::env::var("KERATIN_IO_FAULT_CHILD_ROOT") {
        let prefix: u64 = std::env::var("KERATIN_IO_FAULT_PREFIX")
            .unwrap()
            .parse()
            .unwrap();
        let message = std::env::var("KERATIN_IO_FAULT_LOG").unwrap() == "message";
        let fault = if std::env::var("KERATIN_IO_FAULT_KIND").unwrap() == "partial" {
            Fault::PartialWrite
        } else {
            Fault::Fsync
        };
        let config = StromaKeratinConfig::from_message_log(KeratinConfig {
            flush_target_bytes: 8,
            min_fsync_interval_ms: 5,
            fsync_interval_ms: 20,
            ..KeratinConfig::test_default()
        });
        let owner = Stroma::open(root, config, SnapshotConfig::default())
            .await
            .unwrap();
        let qh = owner.queue_handle("q", 0, None).await.unwrap();
        for offset in 0..=prefix {
            let path = if message {
                owner.msg_tp_part_dir("q", 0, None)
            } else {
                owner.tp_part_dir("q", 0, None)
            }
            .join("segments")
            .join("00000000000000000000.log");
            let armed = (offset == prefix).then(|| arm_repeated(&path, fault, usize::MAX).unwrap());
            let (completion, receipt) = KeratinAppendCompletion::pair();
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
                        payload: vec![offset as u8; 128],
                        completion,
                        not_before: None,
                        expire_at: None,
                    }],
                )
                .await
                .unwrap();
            let result = tokio::time::timeout(Duration::from_secs(5), receipt)
                .await
                .unwrap()
                .unwrap();
            if offset < prefix {
                result.unwrap();
            } else {
                assert!(result.is_err());
                assert!(armed.as_ref().unwrap().hits() > 0);
                assert!(!owner.is_ready("q", 0, None, offset).await.unwrap());
                // Bypass Rust destructors and all graceful Stroma/Keratin shutdown.
                // The parent reopens real files left at this error boundary.
                std::process::exit(77);
            }
        }
        drop(qh);
        panic!("child did not reach error boundary");
    }
    for prefix in [0, 1] {
        for log in ["message", "event"] {
            for kind in ["partial", "fsync"] {
                let dir = test_dir!("fault_process_exit");
                let output = tokio::time::timeout(Duration::from_secs(15), tokio::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", "stroma::early_replication_tests::owner_io_fault_process_exit_preserves_confirmed_prefix", "--nocapture"])
                    .env("KERATIN_IO_FAULT_CHILD_ROOT", &dir.root)
                    .env("KERATIN_IO_FAULT_PREFIX", prefix.to_string())
                    .env("KERATIN_IO_FAULT_LOG", log)
                    .env("KERATIN_IO_FAULT_KIND", kind)
                    .kill_on_drop(true).output()).await.unwrap().unwrap();
                assert_eq!(
                    output.status.code(),
                    Some(77),
                    "child prefix={prefix} log={log} kind={kind}: {} {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                let reopened = Stroma::open(
                    &dir.root,
                    StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
                    SnapshotConfig::default(),
                )
                .await
                .unwrap();
                for confirmed in 0..prefix {
                    assert!(reopened.is_ready("q", 0, None, confirmed).await.unwrap());
                }
                eprintln!(
                    "PROCESS EXIT prefix={prefix} log={log} kind={kind} uncertain_ready={}",
                    reopened.is_ready("q", 0, None, prefix).await.unwrap()
                );
                reopened.shutdown().await.unwrap();
            }
        }
    }
}
