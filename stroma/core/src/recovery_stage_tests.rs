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
            payload: vec![off as u8],
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
            message_head: head,
            message_next: next,
            event_next: next,
            message_digest: *messages.finalize().as_bytes(),
            snapshot_digest: *blake3::hash(&snapshot).as_bytes(),
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
        source: RecoveryReadSource::Messages,
        from: records[0].offset,
        next: records.last().unwrap().offset + 1,
        end: spec.message_next,
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
    assert_eq!(receipt.event_next, 3);
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
                seal: request,
                history_id: sealed.history.id,
                source: RecoveryReadSource::Messages,
                from: 0,
                max_records: 10,
                max_bytes: 1024,
            },
        )
        .await
        .unwrap();
    assert_eq!((read.from, read.next, read.end), (0, 0, 0));
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
        assert_eq!((receipt.message_next, receipt.event_next), (next, next));
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
async fn stage_rejects_wrong_pages_and_enforces_budget_across_resume() {
    let dir = keratin_log::test_dir!("recovery_stage_validation");
    let st = open(&dir.root).await;
    let (spec, snapshot, records) = fixture(0, 3);
    let limits = RecoveryStageLimits {
        max_records: 3,
        max_bytes: 38,
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
            3 => wrong.source = RecoveryReadSource::Events,
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
    wrong.event_next += 1;
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
            let messages = stage.inner.lock().await.root.join("messages");
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
        let ready = dir.root.join("crash.ready");
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
        assert_eq!((receipt.message_next, receipt.event_next), (2, 2));
        assert!(!st.tp_part_dir("q", 0, None).exists());
        assert!(!st.msg_tp_part_dir("q", 0, None).exists());
        assert!(!st.snap_dir("q", 0, None).exists());
        drop(stage);
        st.shutdown().await.unwrap();
    }
}
