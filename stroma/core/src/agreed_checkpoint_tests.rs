use super::*;

async fn open(root: &Path) -> Stroma {
    let mut config = KeratinConfig::test_default();
    config.segment_max_bytes = 1024;
    Stroma::open(
        root,
        StromaKeratinConfig::from_message_log(config),
        SnapshotConfig::default(),
    )
    .await
    .unwrap()
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
    assert_eq!(base.pin.event_next, 8);
    assert_eq!(base.pin.required_message_next, 8);
    publish(&st, 32).await;
    st.ack_batch("q".into(), 0, None, &(0..40).collect::<Vec<_>>())
        .await
        .unwrap();
    let ticket = st.queue_handle("q", 0, None).await.unwrap();
    st.write_exact_queue_checkpoint(ticket.clone(), true)
        .await
        .unwrap();
    st.truncate_messages_before("q", 0, None, 40).await.unwrap();
    st.truncate_partition_log(ticket.clone(), u64::MAX)
        .await
        .unwrap();
    {
        let h = ticket.resolve().unwrap();
        assert_eq!(h.msg_log().head_offset(), 0);
        assert!(h.event_log().head_offset() <= base.pin.event_next);
        assert!(h.event_log().next_offset() > base.pin.event_next);
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
    assert!(boundary.event_next() > base.pin.event_next);
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
    assert!(ticket.resolve().unwrap().msg_log().head_offset() > 0);
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
            base.pin.event_next,
            base.pin.message_head,
            base.pin.message_next
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
            base.pin.event_next,
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
    assert_eq!(c.pin.event_next, b.pin.event_next);
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
    assert!(st.truncate_messages_before("q", 0, None, 4).await.is_err());
    publish(&st, 1).await;
    assert_eq!(ticket.resolve().unwrap().msg_log().head_offset(), 0);
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
    assert!(pb.pin.event_next > pa.pin.event_next);
    publish(&a, 8).await;
    publish(&b, 8).await;
    let target = a
        .queue_checkpoint_target(sa, pb.pin.event_next, 0, pb.pin.message_next)
        .await
        .unwrap();
    assert_eq!(target.message_head, 5);
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
    assert_eq!(ca.contents.event_next, target.event_next);
    assert_eq!(ca.contents.required_message_next, 24);
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
    wrong.event_next += 1;
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
        .queue_checkpoint_target(storage, base.pin.event_next, 0, base.pin.message_next)
        .await
        .unwrap();
    for mutation in 0..5 {
        let mut target = target.clone();
        let mut limits = QueueCheckpointBuildLimits::default();
        match mutation {
            0 => limits.records = 1,
            1 => limits.bytes = 32,
            2 => target.event_next += 100,
            3 => target.message_head = 1, // Message zero is still live.
            _ => target.message_next = 2, // Events reference newer messages.
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
    let target = st.queue_checkpoint_target(storage, 0, 0, 0).await.unwrap();
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
        (capsule.contents.event_next, capsule.contents.message_next),
        (0, 0)
    );
    assert_eq!(capsule.contents.required_message_next, 0);
    st.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accepted_checkpoint_retries_every_publication_boundary_and_replaces_old_pin() {
    for boundary in [
        "agreement_restart_snapshot",
        "agreement_acceptance",
        "agreement_event_floor",
        "agreement_message_floor",
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
                base.pin.event_next,
                base.pin.message_head,
                base.pin.message_next,
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
            target.event_next
        );
        assert_eq!(
            ticket.resolve().unwrap().msg_log().head_offset(),
            target.message_head
        );
        publish(&st, 4).await;
        let new = st
            .begin_queue_checkpoint_pin(storage.clone(), [9; 32])
            .await
            .unwrap();
        let next = st
            .queue_checkpoint_target(
                storage,
                new.pin.event_next,
                new.pin.message_head,
                new.pin.message_next,
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
                    fence_epoch: capsule.pin.event_epoch + 1,
                },
            )
            .await
            .unwrap();
        assert_eq!(sealed.history.event_head, capsule.contents.event_next);
        assert_eq!(sealed.history.message_next, 16);
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
            base.pin.event_next,
            base.pin.message_head,
            base.pin.message_next,
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
    for boundary in [
        "agreement_base",
        "agreement_pin",
        "agreement_capsule",
        "agreement_restart_snapshot",
        "agreement_acceptance",
        "agreement_event_floor",
        "agreement_message_floor",
    ] {
        let dir = keratin_log::test_dir!("agreement_sigkill");
        let ready = dir.root.join("ready");
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
        assert_eq!(seal.history.message_next, 12, "{boundary}");
        assert_eq!(seal.history.event_next, 20, "{boundary}");
        assert!(
            seal.history.message_head <= 8,
            "old live payloads must survive {boundary}"
        );
        let snapshot = st
            .read_queue_snapshot(&st.recovery_snapshot_path("q", 0, None).unwrap())
            .unwrap();
        if seal.history.event_head > 0 {
            assert_eq!(
                snapshot.as_ref().unwrap().0.event_next(),
                seal.history.event_next
            );
        }
        st.shutdown().await.unwrap();
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
                base.pin.event_next,
                base.pin.message_head,
                base.pin.message_next,
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
            assert_eq!(seal.history.message_next, 5);
            assert_eq!(seal.history.event_head, 4);
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
