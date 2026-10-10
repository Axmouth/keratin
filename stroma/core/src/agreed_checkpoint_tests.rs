use super::*;

#[derive(Debug)]
struct NoLaggingReplicas;
impl QueueReplicationRetention for NoLaggingReplicas {
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

async fn open(root: &Path) -> Stroma {
    let options = StromaOptions::default();
    let mut config = KeratinConfig::test_default();
    config.segment_max_bytes = 1024;
    let st = Stroma::open_with_options(
        root,
        StromaKeratinConfig::from_message_log(config),
        SnapshotConfig::default(), options,
    )
    .await
    .unwrap();
    // These tests isolate durable checkpoint pins with no lagging live replicas.
    st.set_queue_replication_retention(Arc::new(NoLaggingReplicas));
    st
}
async fn admitted(st: &Stroma) -> PreparedStorageHistory {
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
    storage
}
async fn publish(st: &Stroma, count: u64) {
    for n in 0..count {
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
                payload: vec![n as u8; 512],
                completion,
                not_before: None,
                expire_at: None,
            }],
        )
        .await
        .unwrap();
        rx.await.unwrap().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pin_keeps_separate_base_while_snapshots_and_writes_advance() {
    let dir = keratin_log::test_dir!("agreement_pin_compaction");
    let st = open(&dir.root).await;
    let storage = admitted(&st).await;
    publish(&st, 8).await;
    let base = st
        .begin_queue_checkpoint_pin(storage.clone(), [7; 32])
        .await
        .unwrap();
    assert_eq!(base.pin.applied_next, 8);
    assert_eq!(base.pin.required_next, 8);
    publish(&st, 32).await;
    st.ack_batch("q".into(), 0, None, &(0..40).collect::<Vec<_>>())
        .await
        .unwrap();
    let ticket = st.queue_handle("q", 0, None).await.unwrap();
    st.write_exact_queue_checkpoint(ticket.clone(), true)
        .await
        .unwrap();
    st.truncate_queue_before("q", 0, None, 40).await.unwrap();
    st.truncate_partition_log(ticket.clone(), u64::MAX)
        .await
        .unwrap();
    {
        let h = ticket.resolve().unwrap();
        assert_eq!(h.payload_log().head_offset(), 0);
        assert!(h.event_log().head_offset() <= base.pin.applied_next);
        assert!(h.event_log().next_offset() > base.pin.applied_next);
    }
    assert_eq!(
        st.read_queue_checkpoint_base(base.pin.clone())
            .await
            .unwrap(),
        base
    );
    let (boundary, _) = st
        .read_queue_snapshot(&st.snap_file("q", 0, None))
        .unwrap()
        .unwrap();
    assert!(boundary.event_next() > base.pin.applied_next);
    assert_eq!(
        st.begin_queue_checkpoint_pin(storage, [7; 32])
            .await
            .unwrap(),
        base
    );
    st.release_queue_checkpoint_pin(base.pin.clone())
        .await
        .unwrap();
    st.release_queue_checkpoint_pin(base.pin.clone())
        .await
        .unwrap();
    st.truncate_partition_log(ticket.clone(), boundary.event_next())
        .await
        .unwrap();
    assert!(ticket.resolve().unwrap().payload_log().head_offset() > 0);
    assert!(st.read_queue_checkpoint_base(base.pin).await.is_err());
    st.shutdown().await.unwrap();
}

#[tokio::test]
async fn pin_zero_and_cold_restart_preserve_original_storage_identity() {
    let dir = keratin_log::test_dir!("agreement_pin_zero_restart");
    let st = open(&dir.root).await;
    let storage = admitted(&st).await;
    let base = st
        .begin_queue_checkpoint_pin(storage, [7; 32])
        .await
        .unwrap();
    assert_eq!(
        (
            base.pin.applied_next,
            base.pin.payload_head,
            base.pin.log_next
        ),
        (0, 0, 0)
    );
    publish(&st, 2).await;
    st.shutdown().await.unwrap();
    drop(st);
    let st = open(&dir.root).await;
    assert_eq!(
        st.read_queue_checkpoint_base(base.pin.clone())
            .await
            .unwrap(),
        base
    );
    assert!(
        st.begin_queue_checkpoint_pin(base.pin.storage.clone(), [7; 32])
            .await
            .is_err()
    );
    assert!(st.release_queue_checkpoint_pin(base.pin).await.is_err());
    st.shutdown().await.unwrap();
}

#[tokio::test]
async fn pin_faults_before_and_after_index_publication_are_retryable() {
    for boundary in ["agreement_base", "agreement_pin"] {
        let dir = keratin_log::test_dir!("agreement_pin_fault");
        let st = open(&dir.root).await;
        let storage = admitted(&st).await;
        publish(&st, 1).await;
        *st.checkpoint_fault.lock().unwrap() = Some(boundary);
        assert!(
            st.begin_queue_checkpoint_pin(storage.clone(), [7; 32])
                .await
                .is_err()
        );
        publish(&st, 1).await;
        let base = st
            .begin_queue_checkpoint_pin(storage.clone(), [7; 32])
            .await
            .unwrap();
        assert_eq!(
            base.pin.applied_next,
            if boundary == "agreement_base" { 2 } else { 1 }
        );
        assert_eq!(
            st.read_queue_checkpoint_base(base.pin.clone())
                .await
                .unwrap(),
            base
        );
        assert_eq!(
            st.begin_queue_checkpoint_pin(storage, [7; 32])
                .await
                .unwrap(),
            base
        );
        st.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn pin_bounded_count_conflicting_identity_and_release_are_checked() {
    let dir = keratin_log::test_dir!("agreement_pin_bound");
    let st = open(&dir.root).await;
    let storage = admitted(&st).await;
    assert!(
        st.begin_queue_checkpoint_pin(storage.clone(), [0; 32])
            .await
            .is_err()
    );
    let mut stream = storage.clone();
    stream.stream = true;
    assert!(
        st.begin_queue_checkpoint_pin(stream, [1; 32])
            .await
            .is_err()
    );
    let a = st
        .begin_queue_checkpoint_pin(storage.clone(), [7; 32])
        .await
        .unwrap();
    publish(&st, 1).await;
    let b = st
        .begin_queue_checkpoint_pin(storage.clone(), [8; 32])
        .await
        .unwrap();
    assert!(
        st.begin_queue_checkpoint_pin(storage.clone(), [9; 32])
            .await
            .is_err()
    );
    let mut bad = a.pin.clone();
    bad.snapshot_digest = [99; 32];
    assert!(st.release_queue_checkpoint_pin(bad.clone()).await.is_err());
    assert!(st.read_queue_checkpoint_base(bad).await.is_err());
    let mut bad = storage.clone();
    bad.binding.writer_session = [99; 16];
    assert!(st.begin_queue_checkpoint_pin(bad, [7; 32]).await.is_err());
    st.release_queue_checkpoint_pin(a.pin).await.unwrap();
    let c = st
        .begin_queue_checkpoint_pin(storage, [9; 32])
        .await
        .unwrap();
    assert_eq!(c.pin.applied_next, b.pin.applied_next);
    assert_eq!(
        st.read_queue_checkpoint_base(b.pin.clone()).await.unwrap(),
        b
    );
    st.shutdown().await.unwrap();
}

#[tokio::test]
async fn corrupt_retention_blocks_compaction_without_blocking_publish() {
    let dir = keratin_log::test_dir!("agreement_pin_corrupt");
    let st = open(&dir.root).await;
    let storage = admitted(&st).await;
    publish(&st, 4).await;
    let base = st
        .begin_queue_checkpoint_pin(storage, [7; 32])
        .await
        .unwrap();
    let root = st.checkpoint_pin_root("q", 0, None);
    let index_path = root.join("retention");
    let bytes = fs::read(&index_path).unwrap();
    fs::write(&index_path, b"broken").unwrap();
    st.ack_batch("q".into(), 0, None, &[0, 1, 2, 3])
        .await
        .unwrap();
    let ticket = st.queue_handle("q", 0, None).await.unwrap();
    assert!(
        st.write_exact_queue_checkpoint(ticket.clone(), true)
            .await
            .is_err()
    );
    assert!(st.truncate_queue_before("q", 0, None, 4).await.is_err());
    publish(&st, 1).await;
    assert_eq!(ticket.resolve().unwrap().payload_log().head_offset(), 0);
    assert!(
        st.read_queue_checkpoint_base(base.pin.clone())
            .await
            .is_err()
    );
    fs::write(&index_path, bytes).unwrap();
    assert_eq!(
        st.read_queue_checkpoint_base(base.pin.clone())
            .await
            .unwrap(),
        base
    );
    let path = base_path(&root, base.pin.attempt);
    let mut bytes = fs::read(&path).unwrap();
    bytes[20] ^= 1;
    fs::write(path, bytes).unwrap();
    assert!(st.read_queue_checkpoint_base(base.pin).await.is_err());
    st.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capsules_reconcile_unequal_bases_and_ignore_later_application() {
    let da = keratin_log::test_dir!("agreement_capsule_a");
    let db = keratin_log::test_dir!("agreement_capsule_b");
    let a = open(&da.root).await;
    let b = open(&db.root).await;
    let sa = admitted(&a).await;
    let sb = admitted(&b).await;
    publish(&a, 8).await;
    publish(&b, 8).await;
    // Ordinary owner delivery leases are actor-local, unlike the explicit
    // mark_inflight test API, which writes a replicated event.
    let ticket = a.queue_handle("q", 0, None).await.unwrap();
    let (response, rx) = tokio::sync::oneshot::channel();
    ticket
        .resolve()
        .unwrap()
        .command_enqueue(QueueCommand::MarkInflight {
            offset: 6,
            deadline: u64::MAX - 1,
            response: Some(response),
        })
        .await
        .unwrap();
    rx.await.unwrap();
    let pa = a
        .begin_queue_checkpoint_pin(sa.clone(), [7; 32])
        .await
        .unwrap();
    publish(&a, 8).await;
    publish(&b, 8).await;
    for st in [&a, &b] {
        st.ack_batch("q".into(), 0, None, &[0, 1, 2, 3, 4])
            .await
            .unwrap();
    }
    let pb = b.begin_queue_checkpoint_pin(sb, [7; 32]).await.unwrap();
    assert!(pb.pin.applied_next > pa.pin.applied_next);
    publish(&a, 8).await;
    publish(&b, 8).await;
    let target = a
        .queue_checkpoint_target(sa, pb.pin.log_next, 0)
        .await
        .unwrap();
    assert_eq!(target.payload_head, 5);
    // Both actors move beyond the selected boundary before verification begins.
    publish(&a, 3).await;
    publish(&b, 3).await;
    let ca = a
        .build_queue_checkpoint_capsule(
            pa.pin.clone(),
            target.clone(),
            QueueCheckpointBuildLimits::default(),
        )
        .await
        .unwrap();
    let cb = b
        .build_queue_checkpoint_capsule(
            pb.pin.clone(),
            target.clone(),
            QueueCheckpointBuildLimits::default(),
        )
        .await
        .unwrap();
    assert_eq!(ca.contents, cb.contents);
    assert_eq!(ca.snapshot, cb.snapshot);
    assert_eq!(ca.contents.next, target.next);
    // Twenty-four payloads plus five ACK records precede the cut.
    assert_eq!(ca.contents.required_next, 29);
    assert_ne!(ca.digest().unwrap(), cb.digest().unwrap()); // Local storage/base identities differ.
    let replay = a
        .build_queue_checkpoint_capsule(
            pa.pin.clone(),
            target.clone(),
            QueueCheckpointBuildLimits::default(),
        )
        .await
        .unwrap();
    assert_eq!(replay, ca);
    let mut wrong = target;
    wrong.next += 1;
    assert!(
        a.build_queue_checkpoint_capsule(pa.pin, wrong, QueueCheckpointBuildLimits::default())
            .await
            .is_err()
    );
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn capsule_limits_incomplete_dependencies_and_old_live_payloads_fail_closed() {
    let dir = keratin_log::test_dir!("agreement_capsule_limits");
    let st = open(&dir.root).await;
    let storage = admitted(&st).await;
    publish(&st, 2).await;
    let base = st
        .begin_queue_checkpoint_pin(storage.clone(), [7; 32])
        .await
        .unwrap();
    publish(&st, 2).await;
    let target = st
        .queue_checkpoint_target(storage, base.pin.log_next, 0)
        .await
        .unwrap();
    for mutation in 0..6 {
        let mut target = target.clone();
        let mut limits = QueueCheckpointBuildLimits::default();
        match mutation {
            0 => limits.records = 1,
            1 => limits.bytes = 32,
            2 => target.next += 100,
            3 => target.payload_head = 1, // Message zero is still live.
            4 => target.next = 1, // Below the pinned applied boundary.
            _ => target.payload_head = target.next + 1,
        }
        assert!(
            st.build_queue_checkpoint_capsule(base.pin.clone(), target, limits)
                .await
                .is_err(),
            "mutation {mutation}"
        );
    }
    assert!(!capsule_path(&st.checkpoint_pin_root("q", 0, None), base.pin.attempt).exists());
    let capsule = st
        .build_queue_checkpoint_capsule(base.pin, target, QueueCheckpointBuildLimits::default())
        .await
        .unwrap();
    capsule.validate().unwrap();
    st.shutdown().await.unwrap();
}

#[tokio::test]
async fn empty_capsule_zero_and_retry_after_durable_publication() {
    let dir = keratin_log::test_dir!("agreement_capsule_empty");
    let st = open(&dir.root).await;
    let storage = admitted(&st).await;
    let base = st
        .begin_queue_checkpoint_pin(storage.clone(), [7; 32])
        .await
        .unwrap();
    let target = st.queue_checkpoint_target(storage, 0, 0).await.unwrap();
    *st.checkpoint_fault.lock().unwrap() = Some("agreement_capsule");
    assert!(
        st.build_queue_checkpoint_capsule(
            base.pin.clone(),
            target.clone(),
            QueueCheckpointBuildLimits::default()
        )
        .await
        .is_err()
    );
    publish(&st, 1).await;
    let capsule = st
        .build_queue_checkpoint_capsule(base.pin, target, QueueCheckpointBuildLimits::default())
        .await
        .unwrap();
    assert_eq!(
        (capsule.contents.next, capsule.contents.payload_head),
        (0, 0)
    );
    assert_eq!(capsule.contents.required_next, 0);
    st.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepted_checkpoint_retries_every_publication_boundary_and_replaces_old_pin() {
    for boundary in [
        "agreement_restart_snapshot",
        "agreement_acceptance",
        "agreement_floor",
    ] {
        let dir = keratin_log::test_dir!("agreement_accept_retry");
        let st = open(&dir.root).await;
        let storage = admitted(&st).await;
        publish(&st, 12).await;
        st.ack_batch("q".into(), 0, None, &(0..12).collect::<Vec<_>>())
            .await
            .unwrap();
        let base = st
            .begin_queue_checkpoint_pin(storage.clone(), [7; 32])
            .await
            .unwrap();
        let target = st
            .queue_checkpoint_target(
                storage.clone(),
                base.pin.log_next,
                base.pin.payload_head,
            )
            .await
            .unwrap();
        let capsule = st
            .build_queue_checkpoint_capsule(
                base.pin.clone(),
                target.clone(),
                QueueCheckpointBuildLimits::default(),
            )
            .await
            .unwrap();
        *st.checkpoint_fault.lock().unwrap() = Some(boundary);
        assert!(
            st.accept_queue_checkpoint(base.pin.clone(), [8; 32], capsule.digest().unwrap(), None)
                .await
                .is_err()
        );
        st.accept_queue_checkpoint(base.pin.clone(), [8; 32], capsule.digest().unwrap(), None)
            .await
            .unwrap();
        assert!(
            st.release_queue_checkpoint_pin(base.pin.clone())
                .await
                .is_err()
        );
        let ticket = st.queue_handle("q", 0, None).await.unwrap();
        assert_eq!(
            ticket.resolve().unwrap().event_log().head_offset(),
            target.payload_head
        );
        publish(&st, 4).await;
        let new = st
            .begin_queue_checkpoint_pin(storage.clone(), [9; 32])
            .await
            .unwrap();
        let next = st
            .queue_checkpoint_target(
                storage,
                new.pin.log_next,
                new.pin.payload_head,
            )
            .await
            .unwrap();
        let capsule = st
            .build_queue_checkpoint_capsule(
                new.pin.clone(),
                next,
                QueueCheckpointBuildLimits::default(),
            )
            .await
            .unwrap();
        assert!(
            st.accept_queue_checkpoint(new.pin.clone(), [10; 32], capsule.digest().unwrap(), None)
                .await
                .is_err()
        );
        st.accept_queue_checkpoint(new.pin, [10; 32], capsule.digest().unwrap(), Some([8; 32]))
            .await
            .unwrap();
        assert!(st.read_queue_checkpoint_base(base.pin).await.is_err());
        st.shutdown().await.unwrap();
        let st = open(&dir.root).await;
        assert!(
            st.queue_handle("q", 0, None).await.is_err(),
            "restart must not silently readmit old storage"
        );
        let sealed = st
            .seal_replica_for_recovery(
                "q",
                0,
                None,
                RecoverySealRequest {
                    transition: [21; 32],
                    fence_epoch: capsule.pin.epoch + 1,
                },
            )
            .await
            .unwrap();
        assert_eq!(sealed.history.queue_log().unwrap().head, capsule.contents.payload_head);
        st.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn agreement_crash_child() {
    let Some(root) = std::env::var_os("STROMA_AGREEMENT_CRASH_ROOT") else {
        return;
    };
    let st = open(Path::new(&root)).await;
    let storage = admitted(&st).await;
    publish(&st, 12).await;
    st.ack_batch("q".into(), 0, None, &(0..8).collect::<Vec<_>>())
        .await
        .unwrap();
    let base = st
        .begin_queue_checkpoint_pin(storage.clone(), [7; 32])
        .await
        .unwrap();
    let target = st
        .queue_checkpoint_target(
            storage,
            base.pin.log_next,
            base.pin.payload_head,
        )
        .await
        .unwrap();
    let capsule = st
        .build_queue_checkpoint_capsule(
            base.pin.clone(),
            target,
            QueueCheckpointBuildLimits::default(),
        )
        .await
        .unwrap();
    st.accept_queue_checkpoint(base.pin, [8; 32], capsule.digest().unwrap(), None)
        .await
        .unwrap();
    panic!("child should stop at a durable checkpoint boundary");
}

#[tokio::test]
async fn agreement_sigkill_at_every_publication_boundary_retains_recoverable_history() {
    struct KillOnDrop(std::process::Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    {
        for boundary in [
            "agreement_base",
            "agreement_pin",
            "agreement_capsule",
            "agreement_restart_snapshot",
            "agreement_acceptance",
            "agreement_floor",
        ] {
            let dir = keratin_log::test_dir!("agreement_sigkill");
            let ready = dir.root.join("tmp/ready");
            let mut child = KillOnDrop(
                std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "stroma::agreed_checkpoint::tests::agreement_crash_child",
                        "--nocapture",
                    ])
                    .env("STROMA_AGREEMENT_CRASH_ROOT", &dir.root)
                    .env("STROMA_CHECKPOINT_CRASH_READY", &ready)
                    .env("STROMA_CHECKPOINT_CRASH_BOUNDARY", boundary)
                    .stdout(std::process::Stdio::null())
                    .spawn()
                    .unwrap(),
            );
            tokio::time::timeout(Duration::from_secs(10), async {
                while !ready.exists() {
                    assert!(
                        child.0.try_wait().unwrap().is_none(),
                        "child exited before {boundary}"
                    );
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            child.0.kill().unwrap();
            child.0.wait().unwrap();
            let st = open(&dir.root).await;
            assert!(st.queue_handle("q", 0, None).await.is_err());
            let seal = st
                .seal_replica_for_recovery(
                    "q",
                    0,
                    None,
                    RecoverySealRequest {
                        transition: [9; 32],
                        fence_epoch: 1,
                    },
                )
                .await
                .unwrap();
            let log = *seal.history.queue_log().unwrap();
            assert_eq!(log.next, 20, "{boundary}");
            assert!(log.head <= 8, "old live payloads must survive {boundary}");
            let snapshot = st
                .read_queue_snapshot(&st.recovery_snapshot_path("q", 0, None).unwrap())
                .unwrap();
            if log.head > 0 {
                assert_eq!(snapshot.as_ref().unwrap().0.event_next(), log.next);
            }
            let mut inspector = crate::recovery_inspection::RecoveryPairInspector::new(
                "q", 0, None, false, seal.clone(), seal, Default::default(),
            ).unwrap().with_queue_checkpoint_replay(20, Default::default(), 16384).unwrap();
            while let Some((side, request)) = inspector.next_read().unwrap() {
                let page = st.read_sealed_replica("q", 0, None, PartitionKind::Queue, request).await.unwrap();
                inspector.accept_page(side, page).unwrap();
            }
            let (_, [artifact, _]) = inspector.finish_with_queue_artifacts(16384).unwrap();
            let mut state = QueueInternalState::new("q".into(), 0);
            state.load_snapshot(artifact.state_snapshot()).unwrap();
            assert_eq!(state.recovery_live_ranges().iter().cloned().collect::<Vec<_>>(), vec![8..12]);
            st.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn missing_agreed_capsule_falls_back_but_corruption_cannot_authorize_recovery() {
    for missing in [true, false] {
        let dir = keratin_log::test_dir!("agreement_missing_or_corrupt");
        let st = open(&dir.root).await;
        let storage = admitted(&st).await;
        publish(&st, 4).await;
        let base = st
            .begin_queue_checkpoint_pin(storage.clone(), [7; 32])
            .await
            .unwrap();
        let target = st
            .queue_checkpoint_target(
                storage,
                base.pin.log_next,
                base.pin.payload_head,
            )
            .await
            .unwrap();
        let capsule = st
            .build_queue_checkpoint_capsule(
                base.pin.clone(),
                target,
                QueueCheckpointBuildLimits::default(),
            )
            .await
            .unwrap();
        st.accept_queue_checkpoint(base.pin, [8; 32], capsule.digest().unwrap(), None)
            .await
            .unwrap();
        publish(&st, 1).await;
        let path = capsule_path(&st.checkpoint_pin_root("q", 0, None), [7; 32]);
        if missing {
            fs::remove_file(path).unwrap();
        } else {
            fs::write(path, b"corrupted capsule").unwrap();
        }
        let seal = st
            .seal_replica_for_recovery(
                "q",
                0,
                None,
                RecoverySealRequest {
                    transition: [9; 32],
                    fence_epoch: 1,
                },
            )
            .await;
        if missing {
            let seal = seal.unwrap();
            assert_eq!(seal.history.queue_log().unwrap().head, 0);
            assert_eq!(
                st.recovery_snapshot_path("q", 0, None).unwrap(),
                st.snap_file("q", 0, None)
            );
            assert_eq!(
                st.read_queue_snapshot(&st.snap_file("q", 0, None))
                    .unwrap()
                    .unwrap()
                    .0
                    .event_next(),
                4
            );
        } else {
            assert!(seal.is_err());
        }
        st.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn activity_hint_does_not_materialize_and_observes_local_append_work() {
    let dir = keratin_log::test_dir!("checkpoint_activity_hint");
    let st = open(&dir.root).await;
    assert!(st.queue_checkpoint_activity("q", 0, None).is_none());
    admitted(&st).await;
    publish(&st, 2).await;
    let activity = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let activity = st.queue_checkpoint_activity("q", 0, None).unwrap();
            if activity.local_append_bytes >= 1024 { break activity; }
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    assert_eq!(activity.next, 2);
    assert_eq!(activity.head, 0);
    assert!(activity.local_append_bytes >= 1024);
    st.shutdown().await.unwrap();
    let reopened = open(&dir.root).await;
    assert!(reopened.queue_checkpoint_activity("q", 0, None).is_none());
    reopened.shutdown().await.unwrap();
}

fn sample_pin() -> QueueCheckpointPin {
    QueueCheckpointPin {
        attempt: [7; 32],
        storage: PreparedStorageHistory {
            topic: "q".into(),
            partition: 0,
            group: None,
            stream: false,
            storage_instance: [4; 16],
            binding: StorageHistoryBinding {
                resource_incarnation: [1; 16],
                accepted_history: [2; 16],
                writer_session: [3; 16],
            },
        },
        epoch: 1,
        applied_next: 10,
        payload_head: 4,
        log_next: 14,
        required_next: 9,
        snapshot_digest: [5; 32],
        state_digest: [6; 32],
    }
}

/// Each single-log pin invariant is checked on its own, so dropping any one
/// of them makes this test fail.
#[test]
fn pin_rejects_each_broken_single_log_invariant() {
    validate_pin(&sample_pin()).unwrap();
    let broken: [fn(&mut QueueCheckpointPin); 4] = [
        |p| p.payload_head = p.applied_next + 1,
        |p| p.log_next = p.applied_next - 1,
        |p| p.required_next = p.applied_next + 1,
        |p| p.storage.stream = true,
    ];
    for (n, mutate) in broken.into_iter().enumerate() {
        let mut pin = sample_pin();
        mutate(&mut pin);
        assert!(validate_pin(&pin).is_err(), "mutation {n}");
    }
}

#[test]
fn contents_reject_bounds_beyond_the_cut() {
    let contents = QueueCheckpointContents {
        next: 10,
        payload_head: 4,
        required_next: 10,
        snapshot_digest: [0; 32],
        state_digest: [0; 32],
        payload_digest: [0; 32],
        live_payload_digest: [0; 32],
    };
    contents.validate().unwrap();
    let mut bad = contents.clone();
    bad.payload_head = 11;
    assert!(bad.validate().is_err());
    let mut bad = contents;
    bad.required_next = 11;
    assert!(bad.validate().is_err());
}

/// A pin's physical tail can sit above its applied boundary. The cut must
/// cover both, so a target between them is refused.
#[test]
fn target_must_cover_pin_log_tail_and_applied_boundary() {
    let pin = sample_pin();
    let covers = |next, payload_head| {
        target_covers_pin(&pin, &QueueCheckpointTarget { next, payload_head })
    };
    assert!(covers(14, 4));
    assert!(covers(20, 8));
    assert!(!covers(13, 4), "cut below the pin log tail");
    assert!(!covers(9, 4), "cut below the pin applied boundary");
    assert!(!covers(14, 3), "payload floor below the pin floor");
    assert!(!covers(14, 15), "payload floor above the cut");
}

/// Queue compaction takes the payload floor as its own input. An unsettled
/// payload below the applied snapshot boundary stops truncation.
#[tokio::test]
async fn compaction_stops_at_unsettled_payload_below_applied_boundary() {
    let dir = keratin_log::test_dir!("agreement_payload_floor");
    let st = open(&dir.root).await;
    admitted(&st).await;
    publish(&st, 16).await;
    let settled: Vec<_> = (0..16).filter(|o| *o != 3).collect();
    st.ack_batch("q".into(), 0, None, &settled).await.unwrap();
    let ticket = st.queue_handle("q", 0, None).await.unwrap();
    st.write_exact_queue_checkpoint(ticket.clone(), true)
        .await
        .unwrap();
    st.truncate_queue_before("q", 0, None, u64::MAX).await.unwrap();
    st.truncate_partition_log(ticket.clone(), u64::MAX)
        .await
        .unwrap();
    let h = ticket.resolve().unwrap();
    let (boundary, _) = st
        .read_queue_snapshot(&st.snap_file("q", 0, None))
        .unwrap()
        .unwrap();
    assert!(boundary.event_next() > 3);
    assert!(h.event_log().head_offset() <= 3);
    assert_eq!(
        crate::stroma::combined::fetch_payload(&h, 3).unwrap().unwrap().payload,
        vec![3; 512]
    );
    drop(h);
    st.shutdown().await.unwrap();
}
