use super::*;
use crate::recovery_inspection::hash_record;

async fn open(root: &Path) -> Stroma {
    Stroma::open(
        root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap()
}
fn fixture(head: u64, next: u64) -> (QueueRecoveryStageSpec, Vec<u8>, Vec<RecoveryRecord>) {
    let mut state = QueueInternalState::new("q".into(), 0);
    let mut records = vec![];
    for off in 0..head {
        state.ack(off);
    }
    for off in head..next {
        state.enqueue(off, 1, Some(1000));
        records.push(RecoveryRecord {
            offset: off,
            flags: 0,
            headers: vec![],
            payload: StromaEvent::PayloadEnqueue {
                not_before: None,
                expire_at: Some(1000),
                headers: vec![],
                payload: vec![off as u8],
            }
            .encode()
            .unwrap(),
        });
    }
    let state_digest = state.recovery_state_digest();
    let snapshot = state.into_recovery_snapshot(next);
    let mut messages = blake3::Hasher::new();
    messages.update(b"fibril-retained-log-v1\0");
    messages.update(&head.to_be_bytes());
    messages.update(&next.to_be_bytes());
    let mut live = blake3::Hasher::new();
    live.update(b"fibril-recovery-live-payloads-v1\0");
    for record in &records {
        hash_record(&mut messages, record);
        hash_record(&mut live, record);
    }
    (
        QueueRecoveryStageSpec {
            plan: [1; 32],
            topic: "q".into(),
            partition: 0,
            group: None,
            binding: StorageHistoryBinding {
                resource_incarnation: [2; 16],
                accepted_history: [3; 16],
                writer_session: [4; 16],
            },
            fence_epoch: 8,
            source_history: [5; 32],
            log: RetainedLog {
                head,
                next,
                digest: *messages.finalize().as_bytes(),
            },
            snapshot_digest: *blake3::hash(&snapshot).as_bytes(),
            snapshot_bytes: snapshot.len() as u64,
            state_digest,
            live_payload_digest: *live.finalize().as_bytes(),
        },
        snapshot,
        records,
    )
}
fn page(spec: &QueueRecoveryStageSpec, records: &[RecoveryRecord]) -> RecoveryReadPage {
    RecoveryReadPage {
        history_id: spec.source_history,
        source: RecoveryReadSource::Log,
        from: records[0].offset,
        next: records.last().unwrap().offset + 1,
        end: spec.log.next,
        records: records.to_vec(),
        snapshot_bytes: vec![],
    }
}

#[tokio::test]
async fn stage_resumes_without_touching_active_bound_sealed_storage() {
    let dir = keratin_log::test_dir!("recovery_stage_preserves_source");
    let st = open(&dir.root).await;
    let old_binding = StorageHistoryBinding {
        resource_incarnation: [2; 16],
        accepted_history: [7; 16],
        writer_session: [8; 16],
    };
    st.initialize_empty_storage_history("q", 0, None, PartitionKind::Queue, old_binding.clone())
        .await
        .unwrap();
    st.ensure_queue_owner_epoch("q", 0, None, Some(7))
        .await
        .unwrap();
    let request = RecoverySealRequest {
        transition: [9; 32],
        fence_epoch: 8,
    };
    let sealed = st
        .seal_replica_for_recovery("q", 0, None, request.clone())
        .await
        .unwrap();
    let (spec, snapshot, records) = fixture(0, 3);
    let stage = st
        .open_queue_recovery_stage(spec.clone(), snapshot.clone(), Default::default())
        .await
        .unwrap();
    assert!(
        st.open_queue_recovery_stage(spec.clone(), snapshot.clone(), Default::default())
            .await
            .is_err()
    );
    assert!(stage.finish().await.is_err());
    let first = page(&spec, &records[..1]);
    assert_eq!(stage.append(first.clone()).await.unwrap(), 1);
    assert_eq!(stage.append(first).await.unwrap(), 1);
    drop(stage);
    st.shutdown().await.unwrap();
    drop(st);
    let st = open(&dir.root).await;
    let stage = st
        .open_queue_recovery_stage(spec.clone(), snapshot.clone(), Default::default())
        .await
        .unwrap();
    assert_eq!(stage.next_offset().await, 1);
    assert_eq!(stage.append(page(&spec, &records[1..])).await.unwrap(), 3);
    let receipt = stage.finish().await.unwrap();
    assert_eq!(receipt.next, 3);
    assert_eq!(stage.finish().await.unwrap(), receipt);
    assert!(stage.append(page(&spec, &records)).await.is_err());
    assert_eq!(
        st.storage_history_binding("q", 0, None).unwrap(),
        Some(old_binding)
    );
    let read = st
        .read_sealed_replica(
            "q",
            0,
            None,
            PartitionKind::Queue,
            RecoveryReadRequest {
                seal: request.clone(),
                history_id: sealed.history.id,
                source: RecoveryReadSource::Log,
                from: 0,
                max_records: 10,
                max_bytes: 1024,
            },
        )
        .await
        .unwrap();
    assert_eq!((read.from, read.end), (0, sealed.history.queue_log().unwrap().next));
    assert!(
        st.read_sealed_replica(
            "q",
            0,
            None,
            PartitionKind::Queue,
            RecoveryReadRequest {
                seal: request,
                history_id: sealed.history.id,
                source: RecoveryReadSource::StreamRecords,
                from: 0,
                max_records: 10,
                max_bytes: 1024,
            },
        )
        .await
        .is_err()
    );
    assert!(
        st.ensure_queue_owner_epoch("q", 0, None, Some(8))
            .await
            .is_err()
    );
    drop(stage);
    let resumed = st
        .open_queue_recovery_stage(spec, snapshot, Default::default())
        .await
        .unwrap();
    assert_eq!(resumed.finish().await.unwrap(), receipt);
    drop(resumed);
    st.shutdown().await.unwrap();
}

#[tokio::test]
async fn stage_supports_empty_zero_and_compacted_boundaries() {
    for (head, next) in [(0, 0), (5, 5), (5, 8)] {
        let dir = keratin_log::test_dir!("recovery_stage_boundaries");
        let st = open(&dir.root).await;
        let (spec, snapshot, records) = fixture(head, next);
        let stage = st
            .open_queue_recovery_stage(spec.clone(), snapshot.clone(), Default::default())
            .await
            .unwrap();
        assert_eq!(stage.next_offset().await, head);
        if !records.is_empty() {
            stage.append(page(&spec, &records)).await.unwrap();
        }
        let receipt = stage.finish().await.unwrap();
        assert_eq!(receipt.next, next);
        drop(stage);
        drop(snapshot);
        let stage = st
            .resume_queue_recovery_stage(spec, Default::default())
            .await
            .unwrap();
        assert_eq!(stage.finish().await.unwrap(), receipt);
        drop(stage);
        st.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn stage_adopts_runtime_policy_on_open_and_resume() {
    let dir = keratin_log::test_dir!("recovery_stage_runtime");
    let runtime = Arc::new(
        keratin_log::LogRuntimeSettings::new(keratin_log::LogRuntimeConfig {
            segment_preallocate_bytes: 0,
        })
        .unwrap(),
    );
    let st = Stroma::open_with_runtime(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
        runtime.clone(),
    )
    .await
    .unwrap();
    let (spec, snapshot, records) = fixture(5, 8);
    runtime
        .replace(
            0,
            keratin_log::LogRuntimeConfig {
                segment_preallocate_bytes: 4096,
            },
        )
        .unwrap();
    let stage = st
        .open_queue_recovery_stage(spec.clone(), snapshot, Default::default())
        .await
        .unwrap();
    assert!(runtime.logs().iter().any(
        |log| log.root.to_string_lossy().contains("/staging/") && log.applied.revision == 1
    ));
    stage.append(page(&spec, &records)).await.unwrap();
    let receipt = stage.finish().await.unwrap();
    drop(stage);
    runtime
        .replace(
            1,
            keratin_log::LogRuntimeConfig {
                segment_preallocate_bytes: 0,
            },
        )
        .unwrap();
    let stage = st
        .resume_queue_recovery_stage(spec, Default::default())
        .await
        .unwrap();
    assert_eq!(stage.finish().await.unwrap(), receipt);
    assert!(runtime.logs().iter().any(
        |log| log.root.to_string_lossy().contains("/staging/") && log.applied.revision == 2
    ));
    drop(stage);
    st.shutdown().await.unwrap();
}

#[tokio::test]
async fn stage_rejects_wrong_pages_and_enforces_budget_across_resume() {
    let dir = keratin_log::test_dir!("recovery_stage_validation");
    let st = open(&dir.root).await;
    let (spec, snapshot, records) = fixture(0, 3);
    let limits = RecoveryStageLimits {
        max_records: 3,
        max_bytes: records[..2]
            .iter()
            .map(|r| 18 + r.headers.len() as u64 + r.payload.len() as u64)
            .sum(),
        ..Default::default()
    };
    let stage = st
        .open_queue_recovery_stage(spec.clone(), snapshot.clone(), limits)
        .await
        .unwrap();
    for mutate in 0..5 {
        let mut wrong = page(&spec, &records[..1]);
        match mutate {
            0 => wrong.history_id = [0; 32],
            1 => wrong.end += 1,
            2 => wrong.records[0].offset += 1,
            3 => wrong.source = RecoveryReadSource::StreamRecords,
            _ => wrong.snapshot_bytes.push(1),
        }
        assert!(stage.append(wrong).await.is_err());
        assert_eq!(stage.next_offset().await, 0);
    }
    assert!(stage.append(page(&spec, &records[1..])).await.is_err());
    stage.append(page(&spec, &records[..2])).await.unwrap();
    assert!(stage.append(page(&spec, &records[2..])).await.is_err());
    drop(stage);
    let stage = st
        .open_queue_recovery_stage(spec.clone(), snapshot.clone(), limits)
        .await
        .unwrap();
    assert_eq!(stage.next_offset().await, 2);
    assert!(stage.append(page(&spec, &records[2..])).await.is_err());
    let mut wrong = page(&spec, &records[..1]);
    wrong.records[0].payload.push(42);
    assert!(stage.append(wrong).await.is_err());
    assert!(stage.finish().await.is_err());
    drop(stage);
    let stage = st
        .open_queue_recovery_stage(spec.clone(), snapshot.clone(), Default::default())
        .await
        .unwrap();
    stage.append(page(&spec, &records[2..])).await.unwrap();
    stage.finish().await.unwrap();
    drop(stage);
    let mut wrong = spec.clone();
    wrong.log.next += 1;
    assert!(
        st.open_queue_recovery_stage(wrong, snapshot.clone(), Default::default())
            .await
            .is_err()
    );
    let mut wrong = spec;
    wrong.binding.writer_session = [12; 16];
    assert!(
        st.open_queue_recovery_stage(wrong, snapshot, Default::default())
            .await
            .is_err()
    );
    st.shutdown().await.unwrap();
}

#[tokio::test]
async fn stage_refuses_full_but_incorrect_payloads_and_damaged_completed_data() {
    for damage_after in [false, true] {
        let dir = keratin_log::test_dir!("recovery_stage_integrity");
        let st = open(&dir.root).await;
        let (spec, snapshot, mut records) = fixture(0, 2);
        let stage = st
            .open_queue_recovery_stage(spec.clone(), snapshot.clone(), Default::default())
            .await
            .unwrap();
        if !damage_after {
            records[0].payload[0] ^= 1;
        }
        stage.append(page(&spec, &records)).await.unwrap();
        if !damage_after {
            assert!(stage.finish().await.is_err());
            assert!(!stage.inner.lock().await.root.join("complete").exists());
        } else {
            stage.finish().await.unwrap();
            let messages = stage.inner.lock().await.root.join("log");
            drop(stage);
            fs::remove_file(messages.join("manifest.bin")).unwrap();
            assert!(
                st.open_queue_recovery_stage(spec, snapshot, Default::default())
                    .await
                    .is_err()
            );
            st.shutdown().await.unwrap();
            continue;
        }
        drop(stage);
        st.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stage_crash_child() {
    let Some(root) = std::env::var_os("STROMA_STAGE_CRASH_ROOT") else {
        return;
    };
    let st = open(Path::new(&root)).await;
    let (spec, snapshot, records) = fixture(0, 2);
    let stage = st
        .open_queue_recovery_stage(spec.clone(), snapshot, Default::default())
        .await
        .unwrap();
    stage.append(page(&spec, &records)).await.unwrap();
    stage.finish().await.unwrap();
    panic!("expected crash boundary was not reached");
}

#[tokio::test]
async fn stage_resumes_after_sigkill_at_each_durable_boundary() {
    for boundary in ["intent", "initialized", "payloads", "complete"] {
        let dir = keratin_log::test_dir!("recovery_stage_sigkill");
        let ready = dir.root.join("tmp/crash.ready");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "stroma::recovery_stage::tests::stage_crash_child",
                "--nocapture",
            ])
            .env("STROMA_STAGE_CRASH_ROOT", &dir.root)
            .env("STROMA_STAGE_CRASH_BOUNDARY", boundary)
            .env("STROMA_STAGE_CRASH_READY", &ready)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let reached = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if ready.exists() {
                    break true;
                }
                if child.try_wait().unwrap().is_some() {
                    break false;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        let _ = child.kill();
        child.wait().unwrap();
        assert!(matches!(reached, Ok(true)), "did not reach {boundary}");
        let st = open(&dir.root).await;
        let (spec, snapshot, records) = fixture(0, 2);
        drop(snapshot);
        let stage = st
            .resume_queue_recovery_stage(spec.clone(), Default::default())
            .await
            .unwrap();
        let next = stage.next_offset().await;
        assert_eq!(
            next,
            if ["payloads", "complete"].contains(&boundary) {
                2
            } else {
                0
            }
        );
        if next == 0 {
            stage.append(page(&spec, &records)).await.unwrap();
        }
        let receipt = stage.finish().await.unwrap();
        assert_eq!(receipt.next, 2);
        assert!(!st.tp_part_dir("q", 0, None).exists());
        assert!(!st.msg_tp_part_dir("q", 0, None).exists());
        assert!(!st.snap_dir("q", 0, None).exists());
        drop(stage);
        st.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn installation_preserves_source_requires_admission_and_never_resets_retry() {
    let dir = keratin_log::test_dir!("recovery_install_source");
    let st = open(&dir.root).await;
    let old_binding = StorageHistoryBinding {
        resource_incarnation: [2; 16],
        accepted_history: [7; 16],
        writer_session: [8; 16],
    };
    st.initialize_empty_storage_history("q", 0, None, PartitionKind::Queue, old_binding)
        .await
        .unwrap();
    st.ensure_queue_owner_epoch("q", 0, None, Some(7))
        .await
        .unwrap();
    let old_ticket = st.queue_handle("q", 0, None).await.unwrap();
    let old_handle = old_ticket.resolve().unwrap();
    let seal = RecoverySealRequest {
        transition: [9; 32],
        fence_epoch: 8,
    };
    let sealed = st
        .seal_replica_for_recovery("q", 0, None, seal.clone())
        .await
        .unwrap();
    let (spec, snapshot, records) = fixture(1, 3);
    let stage = st
        .open_queue_recovery_stage(spec.clone(), snapshot, Default::default())
        .await
        .unwrap();
    stage.append(page(&spec, &records)).await.unwrap();
    stage.finish().await.unwrap();
    let installed = st
        .install_queue_recovery_stage(spec.clone(), seal.clone(), &stage)
        .await
        .unwrap();
    // Accounting observes the installed generation without admitting the queue.
    let usage = st.recovery_disk_usage().await.unwrap();
    assert!(usage.active_bytes > 0);
    assert!(usage.retained_bytes > 0);
    assert!(usage.staging_bytes > 0);
    // Simulate a visible pointer whose directory-sync response was lost before
    // the in-memory generation was published. An identical retry repairs admission.
    st.generations.clear();
    assert_eq!(
        st.install_queue_recovery_stage(spec.clone(), seal.clone(), &stage)
            .await
            .unwrap(),
        installed
    );
    assert!(st.queue_handle("q", 0, None).await.is_err());
    old_handle.become_owner();
    assert_eq!(old_handle.role(), crate::QueueRole::Frozen);
    assert!(old_handle.begin_owner_operation().await.is_err());
    let old = st
        .read_sealed_replica(
            "q",
            0,
            None,
            PartitionKind::Queue,
            RecoveryReadRequest {
                seal: seal.clone(),
                history_id: sealed.history.id,
                source: RecoveryReadSource::Log,
                from: 0,
                max_records: 10,
                max_bytes: 1024,
            },
        )
        .await
        .unwrap();
    assert_eq!(old.end, sealed.history.queue_log().unwrap().next);
    st.admit_prepared_queue_recovery(installed.clone())
        .await
        .unwrap();
    st.ensure_queue_owner_epoch("q", 0, None, Some(8))
        .await
        .unwrap();
    let ticket = st.queue_handle("q", 0, None).await.unwrap();
    let h = ticket.resolve().unwrap();
    assert_eq!(h.event_log().head_offset(), 1);
    assert_eq!(h.event_log().next_offset(), 3);
    assert_eq!(h.event_log().next_offset(), 3);
    let mut installed_state = QueueInternalState::new("q".into(), 0);
    installed_state
        .load_snapshot(&h.force_encode_snapshot(2).await.unwrap())
        .unwrap();
    assert_eq!(installed_state.recovery_state_digest(), spec.state_digest);
    let (completion, rx) = KeratinAppendCompletion::pair();
    st.append_message(
        "q",
        0,
        None,
        &MessageHeaders {
            published: 0,
            publish_received: 0,
            content_type: None,
            extra: Default::default(),
        },
        b"after activation".to_vec(),
        completion,
    )
    .await
    .unwrap();
    rx.await.unwrap().unwrap();
    assert_eq!(h.event_log().next_offset(), 4);
    assert_eq!(
        st.install_queue_recovery_stage(spec.clone(), seal.clone(), &stage)
            .await
            .unwrap(),
        installed
    );
    st.admit_prepared_queue_recovery(installed.clone())
        .await
        .unwrap();
    assert_eq!(h.event_log().next_offset(), 4);
    let expected_event_next = h.event_log().next_offset();
    drop(stage);
    st.shutdown().await.unwrap();
    drop(h);
    drop(old_handle);
    drop(st);
    let st = open(&dir.root).await;
    assert!(st.admit_prepared_queue_recovery(installed).await.is_err());
    assert!(st.queue_handle("q", 0, None).await.is_err());
    let old = st
        .read_sealed_replica(
            "q",
            0,
            None,
            PartitionKind::Queue,
            RecoveryReadRequest {
                seal,
                history_id: sealed.history.id,
                source: RecoveryReadSource::Log,
                from: 0,
                max_records: 10,
                max_bytes: 1024,
            },
        )
        .await
        .unwrap();
    assert_eq!(old.end, sealed.history.queue_log().unwrap().next);
    let next_seal = st
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
    assert_eq!(next_seal.history.queue_log().unwrap().next, expected_event_next);
    assert_eq!(
        next_seal.history.storage_history.unwrap().binding,
        spec.binding
    );
    st.shutdown().await.unwrap();
}

#[tokio::test]
async fn install_crash_child() {
    let Some(root) = std::env::var_os("STROMA_INSTALL_CRASH_ROOT") else {
        return;
    };
    let st = open(Path::new(&root)).await;
    let (spec, snapshot, records) = fixture(0, 3);
    let stage = st
        .open_queue_recovery_stage(spec.clone(), snapshot, Default::default())
        .await
        .unwrap();
    if stage.next_offset().await == 0 {
        stage.append(page(&spec, &records)).await.unwrap();
    }
    stage.finish().await.unwrap();
    st.install_queue_recovery_stage(
        spec,
        RecoverySealRequest {
            transition: [9; 32],
            fence_epoch: 8,
        },
        &stage,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn installation_sigkill_resumes_each_publication_boundary_without_old_source() {
    for boundary in ["retired", "log", "prepared", "switched"] {
        let dir = keratin_log::test_dir!("install_sigkill");
        let ready = dir.root.join("tmp/ready");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "stroma::recovery_stage::tests::install_crash_child",
                "--nocapture",
            ])
            .env("STROMA_INSTALL_CRASH_ROOT", &dir.root)
            .env("STROMA_INSTALL_CRASH_READY", &ready)
            .env("STROMA_INSTALL_CRASH_BOUNDARY", boundary)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let reached = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if ready.exists() {
                    return true;
                }
                if child.try_wait().unwrap().is_some() {
                    return false;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        let _ = child.kill();
        child.wait().unwrap();
        assert!(matches!(reached, Ok(true)), "boundary {boundary}");
        let st = open(&dir.root).await;
        assert!(st.queue_handle("q", 0, None).await.is_err(), "{boundary}");
        let (spec, _, _) = fixture(0, 3);
        let stage = st
            .resume_queue_recovery_stage(spec.clone(), Default::default())
            .await
            .unwrap();
        let installed = st
            .install_queue_recovery_stage(
                spec,
                RecoverySealRequest {
                    transition: [9; 32],
                    fence_epoch: 8,
                },
                &stage,
            )
            .await
            .unwrap();
        assert!(st.queue_handle("q", 0, None).await.is_err());
        st.admit_prepared_queue_recovery(installed).await.unwrap();
        let ticket = st.queue_handle("q", 0, None).await.unwrap();
        let h = ticket.resolve().unwrap();
        assert!(h.optional_message_log().is_none());
        assert_eq!(h.event_log().next_offset(), 3);
        assert_eq!(h.event_log().head_offset(), 0);
        assert!(!st.msg_tp_part_dir("q", 0, None).exists());
        st.shutdown().await.unwrap();
    }
}

async fn local_reuse_source(st: &Stroma, divergent: bool) -> RecoverySealRequest {
    st.initialize_empty_storage_history(
        "q",
        0,
        None,
        PartitionKind::Queue,
        StorageHistoryBinding {
            resource_incarnation: [2; 16],
            accepted_history: [7; 16],
            writer_session: [8; 16],
        },
    )
    .await
    .unwrap();
    st.ensure_queue_owner_epoch("q", 0, None, Some(7))
        .await
        .unwrap();
    let ticket = st.queue_handle("q", 0, None).await.unwrap();
    let handle = ticket.resolve().unwrap();
    handle
        .event_log()
        .append_batch(
            (0..3)
                .map(|off| keratin_log::Message {
                    payload: StromaEvent::PayloadEnqueue {
                        not_before: None,
                        expire_at: Some(1000),
                        headers: vec![],
                        payload: vec![if divergent { 99 } else { off }],
                    }
                    .encode()
                    .unwrap(),
                    flags: 0,
                    headers: vec![],
                })
                .collect(),
            Some(KDurability::AfterFsync),
        )
        .await
        .unwrap();
    let seal = RecoverySealRequest {
        transition: [9; 32],
        fence_epoch: 8,
    };
    st.seal_replica_for_recovery("q", 0, None, seal.clone())
        .await
        .unwrap();
    seal
}

#[tokio::test]
async fn reuse_checks_actual_payloads_and_installs_private_writable_tail() {
    use std::os::unix::fs::MetadataExt;
    for cold in [false, true] {
        for divergent in [false, true] {
            let dir = keratin_log::test_dir!("stage_local_reuse");
            let mut st = open(&dir.root).await;
            let seal = local_reuse_source(&st, divergent).await;
            let source = st.tp_part_dir("q", 0, None);
            if cold {
                st.shutdown().await.unwrap();
                drop(st);
                st = open(&dir.root).await;
            }
            let (spec, snapshot, records) = fixture(0, 3);
            let stage = st
                .open_queue_recovery_stage_reusing(
                    spec.clone(),
                    snapshot,
                    Default::default(),
                    seal.clone(),
                )
                .await
                .unwrap();
            let prefix = "segments/00000000000000000000.log";
            if divergent {
                assert_eq!(stage.next_offset().await, 0);
                stage.append(page(&spec, &records)).await.unwrap();
            } else {
                assert_eq!(stage.next_offset().await, 3);
                assert_eq!(
                    fs::metadata(source.join(prefix)).unwrap().ino(),
                    fs::metadata(stage.inner.lock().await.root.join("log").join(prefix))
                        .unwrap()
                        .ino()
                );
            }
            stage.finish().await.unwrap();
            let original = fs::read(source.join(prefix)).unwrap();
            let installed = st
                .install_queue_recovery_stage(spec.clone(), seal, &stage)
                .await
                .unwrap();
            let destination = st.tp_part_dir("q", 0, None);
            assert_eq!(
                fs::metadata(destination.join(prefix)).unwrap().ino(),
                fs::metadata(stage.inner.lock().await.root.join("log").join(prefix))
                    .unwrap()
                    .ino()
            );
            st.admit_prepared_queue_recovery(installed).await.unwrap();
            st.ensure_queue_owner_epoch("q", 0, None, Some(8))
                .await
                .unwrap();
            let ticket = st.queue_handle("q", 0, None).await.unwrap();
            let handle = ticket.resolve().unwrap();
            handle
                .event_log()
                .append_batch(
                    vec![keratin_log::Message {
                        payload: StromaEvent::PayloadEnqueue {
                            not_before: None,
                            expire_at: None,
                            headers: vec![],
                            payload: vec![42],
                        }
                        .encode()
                        .unwrap(),
                        flags: 0,
                        headers: vec![],
                    }],
                    Some(KDurability::AfterFsync),
                )
                .await
                .unwrap();
            assert_eq!(fs::read(source.join(prefix)).unwrap(), original);
            assert_eq!(stage.next_offset().await, 3);
            assert_eq!(
                stage
                    .read_completed_messages(0, 10, 1024)
                    .await
                    .unwrap()
                    .records,
                records
            );
            st.shutdown().await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reuse_crash_child() {
    let Some(root) = std::env::var_os("STROMA_REUSE_CRASH_ROOT") else {
        return;
    };
    let st = open(Path::new(&root)).await;
    let seal = local_reuse_source(&st, false).await;
    let (spec, snapshot, _) = fixture(0, 3);
    st.open_queue_recovery_stage_reusing(spec, snapshot, Default::default(), seal)
        .await
        .unwrap();
    panic!("expected reuse interruption");
}

#[tokio::test]
async fn reuse_resumes_after_kill_before_and_after_initialized_marker() {
    for boundary in ["reused-log", "initialized"] {
        let dir = keratin_log::test_dir!("reuse_sigkill");
        let ready = dir.root.join("tmp/ready");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "stroma::recovery_stage::tests::reuse_crash_child",
                "--nocapture",
            ])
            .env("STROMA_REUSE_CRASH_ROOT", &dir.root)
            .env("STROMA_STAGE_CRASH_BOUNDARY", boundary)
            .env("STROMA_STAGE_CRASH_READY", &ready)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let reached = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if ready.exists() {
                    return true;
                }
                if child.try_wait().unwrap().is_some() {
                    return false;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        let _ = child.kill();
        child.wait().unwrap();
        assert!(matches!(reached, Ok(true)), "{boundary}");
        let st = open(&dir.root).await;
        let (spec, snapshot, _) = fixture(0, 3);
        let seal = RecoverySealRequest {
            transition: [9; 32],
            fence_epoch: 8,
        };
        let stage = st
            .open_queue_recovery_stage_reusing(spec.clone(), snapshot, Default::default(), seal)
            .await
            .unwrap();
        assert_eq!(stage.next_offset().await, 3);
        stage.finish().await.unwrap();
        assert!(st.queue_handle("q", 0, None).await.is_err());
        st.shutdown().await.unwrap();
    }
}

#[test]
fn stage_descriptor_binds_source_bounds_and_complete_target() {
    let (spec, snapshot, _) = fixture(1, 3);
    spec.validate_log().unwrap();
    for change in 0..2 {
        let mut bad = spec.clone();
        match change {
            0 => bad.log.head = 4,
            _ => bad.log.next = 0,
        }
        assert!(bad.validate_log().is_err(), "accepted mutation {change}");
    }
    // The applied boundary is the source log tail. A tail that disagrees
    // with the snapshot boundary is refused even with ordered bounds.
    for next in [2, 4] {
        let mut bad = spec.clone();
        bad.log.next = next;
        let intent = Intent { spec: bad, snapshot: snapshot.clone() };
        assert!(validate(&intent, Default::default()).is_err(), "next {next}");
    }
    validate(&Intent { spec, snapshot }, Default::default()).unwrap();
}

#[tokio::test]
async fn stage_verifies_selected_event_digest_even_when_live_payloads_match() {
    let dir = keratin_log::test_dir!("stage_selected_digest");
    let st = open(&dir.root).await;
    let (mut spec, snapshot, records) = fixture(1, 3);
    spec.log.digest[0] ^= 1;
    spec.validate_log().unwrap();
    let stage = st
        .open_queue_recovery_stage(spec.clone(), snapshot, Default::default())
        .await
        .unwrap();
    stage.append(page(&spec, &records)).await.unwrap();
    assert!(stage.finish().await.is_err());
    assert!(!stage.inner.lock().await.root.join("complete").exists());
    drop(stage);
    st.shutdown().await.unwrap();
}

#[tokio::test]
async fn snapshot_chunks_resume_before_stage_creation_and_bind_exact_plan() {
    let dir = keratin_log::test_dir!("staged_snapshot_chunks");
    let st = open(&dir.root).await;
    let (spec, snapshot, records) = fixture(3, 9);
    let seal = RecoverySealRequest { transition: [9; 32], fence_epoch: spec.fence_epoch };
    assert_eq!(st.transfer_queue_recovery_snapshot(spec.clone(), None).await.unwrap(), 0);
    st.transfer_queue_recovery_snapshot(spec.clone(), Some((0, snapshot[..30].to_vec()))).await.unwrap();
    assert!(st.open_queue_recovery_stage_from_snapshot(spec.clone(), Default::default(), seal.clone()).await.is_err());
    st.shutdown().await.unwrap();
    drop(st);
    let st = open(&dir.root).await;
    assert_eq!(st.transfer_queue_recovery_snapshot(spec.clone(), None).await.unwrap(), 30);
    assert!(st.transfer_queue_recovery_snapshot(spec.clone(), Some((0, vec![0; 30]))).await.is_err());
    let mut changed = spec.clone();
    changed.plan = [7; 32];
    assert_eq!(st.transfer_queue_recovery_snapshot(changed, None).await.unwrap(), 0);
    let mut from = 30;
    while from < snapshot.len() {
        let next = (from + 37).min(snapshot.len());
        st.transfer_queue_recovery_snapshot(spec.clone(), Some((from as u64, snapshot[from..next].to_vec()))).await.unwrap();
        from = next;
    }
    let stage = st.open_queue_recovery_stage_from_snapshot(spec.clone(), Default::default(), seal).await.unwrap();
    assert!(stage.completed_snapshot_chunk(0, 30).await.is_err());
    assert_eq!(stage.snapshot_chunk(0, 30).await.unwrap(), snapshot[..30]);
    stage.append(page(&spec, &records)).await.unwrap();
    assert!(stage.completion_receipt().await.unwrap().is_none());
    let completed = stage.finish().await.unwrap();
    assert_eq!(stage.completion_receipt().await.unwrap(), Some(completed.clone()));
    assert_eq!(stage.completed_snapshot_chunk(0, 30).await.unwrap(), snapshot[..30]);
    assert_eq!(stage.completed_snapshot().await.unwrap(), snapshot);
    drop(stage);
    let stage = st.resume_queue_recovery_stage(spec, Default::default()).await.unwrap();
    assert_eq!(stage.completed_snapshot().await.unwrap(), snapshot);
    assert_eq!(stage.completion_receipt().await.unwrap(), Some(completed));
    drop(stage);
    st.shutdown().await.unwrap();
}

#[tokio::test]
async fn incremental_stage_rejects_disk_pressure_without_losing_existing_progress() {
    let dir = keratin_log::test_dir!("incremental_stage_disk_admission");
    let st = open(&dir.root).await;
    let (spec, snapshot, records) = fixture(0, 3);
    let mut limits = RecoveryStageLimits::incremental();
    let stage = st.open_queue_recovery_stage(spec.clone(), snapshot, limits).await.unwrap();
    stage.append(page(&spec, &records[..1])).await.unwrap();
    drop(stage);
    limits.reserve_free_bytes = u64::MAX;
    let stage = st.resume_queue_recovery_stage(spec.clone(), limits).await.unwrap();
    assert_eq!(stage.next_offset().await, 1);
    let error = stage.append(page(&spec, &records[1..])).await.unwrap_err().to_string();
    assert!(error.contains("disk reserve"), "{error}");
    assert_eq!(stage.next_offset().await, 1);
    // A duplicate read/validation needs no additional storage admission.
    stage.append(page(&spec, &records[..1])).await.unwrap();
    drop(stage);
    let stage = st.resume_queue_recovery_stage(spec, RecoveryStageLimits::incremental()).await.unwrap();
    let resumed_spec = stage.inner.lock().await.intent.spec.clone();
    stage.append(page(&resumed_spec, &records[1..])).await.unwrap();
    stage.finish().await.unwrap();
    drop(stage);
    st.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "large retained history staging acceptance probe"]
async fn incremental_stage_crosses_legacy_record_and_byte_budgets() {
    let dir = keratin_log::test_dir!("incremental_stage_large_history");
    let st = open(&dir.root).await;
    let count = 1_100_002u64;
    let mut state = QueueInternalState::new("q".into(), 0);
    for off in 0..count { state.ack(off); }
    let state_digest = state.recovery_state_digest();
    let snapshot = state.into_recovery_snapshot(count);
    let make_record = |offset| RecoveryRecord {
        offset, flags: 0, headers: vec![],
        payload: if offset % 2 == 0 {
            StromaEvent::PayloadEnqueue { not_before: None, expire_at: None, headers: vec![], payload: vec![42; 2048] }
        } else { StromaEvent::Ack { off: offset - 1 } }.encode().unwrap(),
    };
    let mut digest = blake3::Hasher::new();
    digest.update(b"fibril-retained-log-v1\0"); digest.update(&0u64.to_be_bytes()); digest.update(&count.to_be_bytes());
    let mut logical_bytes = 0;
    for off in 0..count {
        let r = make_record(off);
        hash_record(&mut digest, &r);
        logical_bytes += record_bytes(&r).unwrap();
    }
    assert!(logical_bytes > RecoveryStageLimits::default().max_bytes);
    assert!(count > RecoveryStageLimits::default().max_records);
    let (mut spec, _, _) = fixture(0, 0);
    spec.log.next = count;
    spec.log.digest = *digest.finalize().as_bytes();
    spec.snapshot_bytes = snapshot.len() as u64;
    spec.snapshot_digest = *blake3::hash(&snapshot).as_bytes();
    spec.state_digest = state_digest;
    assert!(st.open_queue_recovery_stage(spec.clone(), snapshot.clone(), Default::default()).await.is_err());
    let mut stage = st.open_queue_recovery_stage(spec.clone(), snapshot, RecoveryStageLimits::incremental()).await.unwrap();
    for from in (0..count).step_by(4096) {
        let next = (from + 4096).min(count);
        stage.append(RecoveryReadPage { history_id: spec.source_history, source: RecoveryReadSource::Log,
            from, next, end: count, records: (from..next).map(make_record).collect(), snapshot_bytes: vec![] }).await.unwrap();
        if next == 4096 * 100 {
            drop(stage);
            stage = st.resume_queue_recovery_stage(spec.clone(), RecoveryStageLimits::incremental()).await.unwrap();
            assert_eq!(stage.next_offset().await, next);
        }
    }
    stage.finish().await.unwrap();
    assert_eq!(stage.inner.lock().await.used, logical_bytes);
    drop(stage);
    let stage = st.resume_queue_recovery_stage(spec, RecoveryStageLimits::incremental()).await.unwrap();
    stage.finish().await.unwrap();
    println!("large_stage_acceptance records={count} logical_bytes={logical_bytes}");
    drop(stage);
    st.shutdown().await.unwrap();
}
