fn completed_phase_fixture() -> (
    crate::recovery_phase::RecoveryPhaseRecipe,
    RecoveryPairInspection,
    crate::recovery_replay::RecoveryQueueStateArtifact,
) {
    use crate::recovery_phase::{RecoveryPhaseKind, RecoveryPhaseRecipe};
    let records = events(vec![
        payload_event(None),
        payload_event(Some(7)),
        StromaEvent::Ack { off: 0 },
    ]);
    let source = journal_identity(&records);
    let recipe = RecoveryPhaseRecipe {
        sources: [source.clone(), source],
        limits: Default::default(),
        state_entries: 1024,
        kind: RecoveryPhaseKind::SourceArtifact {
            replay: Default::default(),
            checkpoint_bytes: 4096,
            artifact_bytes: 4096,
        },
    };
    let mut inspector = recipe.inspector().unwrap();
    while let Some((side, request)) = inspector.next_read().unwrap() {
        let records = if request.source == RecoveryReadSource::Events {
            records[request.from as usize..].to_vec()
        } else {
            vec![]
        };
        inspector
            .accept_page(
                side,
                RecoveryReadPage {
                    history_id: request.history_id,
                    source: request.source,
                    from: request.from,
                    next: request.from + records.len() as u64,
                    end: if request.source == RecoveryReadSource::Events {
                        3
                    } else {
                        0
                    },
                    records,
                    snapshot_bytes: vec![],
                },
            )
            .unwrap();
    }
    let (report, [artifact, _]) = inspector.finish_with_queue_artifacts(4096).unwrap();
    (recipe, report, artifact)
}
#[test]
fn completed_phase_reopens_exact_evidence_and_rejects_other_recipes() {
    use crate::recovery_phase::RecoveryPhaseStore;
    let dir = keratin_log::test_dir!("completed_phase_exact");
    let (recipe, report, artifact) = completed_phase_fixture();
    let store = RecoveryPhaseStore::open(&dir.root, recipe.clone()).unwrap();
    assert!(store.load().unwrap().is_none());
    assert!(RecoveryPhaseStore::open(&dir.root, recipe.clone()).is_err());
    store.save(&report, Some(&artifact)).unwrap();
    store.save(&report, Some(&artifact)).unwrap();
    let mut changed = report.clone();
    changed.records += 1;
    assert!(store.save(&changed, Some(&artifact)).is_err());
    drop(store);
    let restored = RecoveryPhaseStore::open(&dir.root, recipe.clone())
        .unwrap()
        .load()
        .unwrap()
        .unwrap();
    assert_eq!(restored.report, report);
    assert_eq!(restored.artifact.unwrap(), artifact);
    for (i, mut other) in (0..3).map(|i| (i, recipe.clone())) {
        match i {
            0 => other.limits.page_records /= 2,
            1 => other.state_entries += 1,
            _ => {
                other.sources[0].replica = "b".into();
                other.sources[1].replica = "b".into();
            }
        }
        assert!(
            RecoveryPhaseStore::open(&dir.root, other)
                .unwrap()
                .load()
                .unwrap()
                .is_none()
        );
    }
}
#[test]
fn completed_phase_requires_complete_marker_and_checksum() {
    use crate::recovery_phase::RecoveryPhaseStore;
    let dir = keratin_log::test_dir!("completed_phase_corruption");
    let (recipe, report, artifact) = completed_phase_fixture();
    let store = RecoveryPhaseStore::open(&dir.root, recipe.clone()).unwrap();
    store.save(&report, Some(&artifact)).unwrap();
    drop(store);
    let root = std::fs::read_dir(dir.root.join("phases"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let complete = root.join("complete");
    let saved = std::fs::read(&complete).unwrap();
    std::fs::rename(&complete, root.join("phase.writing")).unwrap();
    assert!(
        RecoveryPhaseStore::open(&dir.root, recipe.clone())
            .unwrap()
            .load()
            .unwrap()
            .is_none()
    );
    let mut corrupt = saved.clone();
    corrupt[15] ^= 1;
    std::fs::write(&complete, &corrupt).unwrap();
    assert!(
        RecoveryPhaseStore::open(&dir.root, recipe.clone())
            .unwrap()
            .load()
            .is_err()
    );
    std::fs::write(&complete, &saved).unwrap();
    let artifact_root = std::fs::read_dir(root.join("artifact"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::write(artifact_root.join("data"), b"truncated").unwrap();
    assert!(
        RecoveryPhaseStore::open(&dir.root, recipe)
            .unwrap()
            .load()
            .is_err()
    );
}
#[test]
fn completed_comparison_persists_without_granting_state_artifact() {
    use crate::recovery_phase::{RecoveryPhaseKind, RecoveryPhaseStore};
    let dir = keratin_log::test_dir!("completed_phase_comparison");
    let (mut recipe, _, artifact) = completed_phase_fixture();
    recipe.sources[1].replica = "b".into();
    recipe.kind = RecoveryPhaseKind::Comparison;
    let records = events(vec![
        payload_event(None),
        payload_event(Some(7)),
        StromaEvent::Ack { off: 0 },
    ]);
    let mut inspector = recipe.inspector().unwrap();
    while let Some((side, request)) = inspector.next_read().unwrap() {
        let rs = if request.source == RecoveryReadSource::Events {
            records[request.from as usize..].to_vec()
        } else {
            vec![]
        };
        inspector
            .accept_page(
                side,
                RecoveryReadPage {
                    history_id: request.history_id,
                    source: request.source,
                    from: request.from,
                    next: request.from + rs.len() as u64,
                    end: if request.source == RecoveryReadSource::Events {
                        3
                    } else {
                        0
                    },
                    records: rs,
                    snapshot_bytes: vec![],
                },
            )
            .unwrap();
    }
    let report = inspector.finish().unwrap();
    let store = RecoveryPhaseStore::open(&dir.root, recipe.clone()).unwrap();
    assert!(store.save(&report, Some(&artifact)).is_err());
    store.save(&report, None).unwrap();
    drop(store);
    let saved = RecoveryPhaseStore::open(&dir.root, recipe)
        .unwrap()
        .load()
        .unwrap()
        .unwrap();
    assert_eq!(saved.report, report);
    assert!(saved.artifact.is_none());
}
#[test]
fn completed_phase_publication_survives_process_crash() {
    use crate::recovery_phase::RecoveryPhaseStore;
    use std::{
        process::Command,
        time::{Duration, Instant},
    };
    let (recipe, report, artifact) = completed_phase_fixture();
    if let Ok(path) = std::env::var("STROMA_TEST_PHASE_CHILD_ROOT") {
        RecoveryPhaseStore::open(std::path::Path::new(&path), recipe)
            .unwrap()
            .save(&report, Some(&artifact))
            .unwrap();
        panic!("phase crash gate was not reached");
    }
    for phase in ["file_synced", "renamed", "directory_synced"] {
        let dir = keratin_log::test_dir!(phase);
        drop(RecoveryPhaseStore::open(&dir.root, recipe.clone()).unwrap());
        let root = std::fs::read_dir(dir.root.join("phases"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "recovery_inspection::tests::completed_phase_publication_survives_process_crash",
                "--nocapture",
            ])
            .env("STROMA_TEST_PHASE_CHILD_ROOT", &dir.root)
            .env("STROMA_TEST_PHASE_CRASH_AT", phase)
            .spawn()
            .unwrap();
        let start = Instant::now();
        while !root.join("crash-marker").exists() && start.elapsed() < Duration::from_secs(10) {
            assert!(child.try_wait().unwrap().is_none());
            std::thread::sleep(Duration::from_millis(10));
        }
        let hit = root.join("crash-marker").exists();
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(hit);
        let store = RecoveryPhaseStore::open(&dir.root, recipe.clone()).unwrap();
        assert_eq!(store.load().unwrap().is_some(), phase != "file_synced");
        store.save(&report, Some(&artifact)).unwrap();
        assert_eq!(store.load().unwrap().unwrap().artifact.unwrap(), artifact);
    }
}
#[test]
fn completed_phase_cleanup_is_bounded_skips_owned_work_and_preserves_lock_inodes() {
    use crate::{recovery_cache::cleanup_recovery_cache, recovery_phase::RecoveryPhaseStore};
    let dir = keratin_log::test_dir!("completed_phase_cleanup");
    let (recipe, report, artifact) = completed_phase_fixture();
    let store = RecoveryPhaseStore::open(&dir.root, recipe.clone()).unwrap();
    store.save(&report, Some(&artifact)).unwrap();
    let held = cleanup_recovery_cache(&dir.root, 1).unwrap();
    assert_eq!(held.removed_files, 0);
    assert!(held.more);
    assert_eq!(held.busy_entries, 1);
    assert!(store.load().unwrap().is_some());
    drop(store);
    let mut removed = 0;
    loop {
        let result = cleanup_recovery_cache(&dir.root, 1).unwrap();
        assert!(result.removed_files <= 1);
        removed += result.removed_bytes;
        if !result.more {
            break;
        }
    }
    assert!(removed > artifact.state_snapshot().len() as u64);
    let store = RecoveryPhaseStore::open(&dir.root, recipe.clone()).unwrap();
    assert!(store.load().unwrap().is_none());
    assert!(RecoveryPhaseStore::open(&dir.root, recipe.clone()).is_err());
    store.save(&report, Some(&artifact)).unwrap();
    assert_eq!(store.load().unwrap().unwrap().artifact.unwrap(), artifact);
}
#[test]
fn completed_phase_cleanup_reclaims_tentative_pages_only_after_the_journal_releases() {
    use crate::{
        recovery_cache::cleanup_recovery_cache, recovery_journal::RecoveryInspectionJournal,
    };
    let dir = keratin_log::test_dir!("phase_page_cleanup");
    let (recipe, _, _) = completed_phase_fixture();
    let inspector = recipe.inspector().unwrap();
    let records = events(vec![
        payload_event(None),
        payload_event(Some(7)),
        StromaEvent::Ack { off: 0 },
    ]);
    let (_, request) = inspector.next_read().unwrap().unwrap();
    let journal = RecoveryInspectionJournal::open(&dir.root, recipe.sources[0].clone()).unwrap();
    let page = RecoveryReadPage {
        history_id: request.history_id,
        source: request.source,
        from: 0,
        next: 3,
        end: 3,
        records,
        snapshot_bytes: vec![],
    };
    journal.append(&request, &page).unwrap();
    assert_eq!(
        cleanup_recovery_cache(&dir.root, 1).unwrap().removed_files,
        0
    );
    assert_eq!(journal.read(&request).unwrap(), Some(page));
    drop(journal);
    assert_eq!(
        cleanup_recovery_cache(&dir.root, 1).unwrap().removed_files,
        1
    );
    let journal = RecoveryInspectionJournal::open(&dir.root, recipe.sources[0].clone()).unwrap();
    assert!(journal.read(&request).unwrap().is_none());
}
#[test]
fn completed_phase_retirement_waits_for_owned_work_and_fences_stale_cache_reopen() {
    use crate::{recovery_cache::retire_recovery_cache, recovery_phase::RecoveryPhaseStore};
    let dir = keratin_log::test_dir!("completed_phase_retirement");
    let (recipe, report, artifact) = completed_phase_fixture();
    let store = RecoveryPhaseStore::open(&dir.root, recipe.clone()).unwrap();
    store.save(&report, Some(&artifact)).unwrap();
    assert_eq!(retire_recovery_cache(&dir.root, 1).unwrap().busy_entries, 1);
    assert!(store.load().unwrap().is_some());
    drop(store);
    let first = retire_recovery_cache(&dir.root, 1).unwrap();
    assert!(first.more);
    assert!(first.removed_files <= 1);
    assert!(RecoveryPhaseStore::open(&dir.root, recipe).is_err());
    while retire_recovery_cache(&dir.root, 1).unwrap().more {}
    let names: std::collections::BTreeSet<_> = std::fs::read_dir(&dir.root)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(
        names,
        [".namespace.lock", "reclaimed", "retired"]
            .into_iter()
            .map(std::ffi::OsString::from)
            .collect()
    );
}
#[test]
fn completed_phase_sweep_preserves_pending_transitions_and_resumes_bounded_retirement() {
    use crate::{recovery_cache::RecoveryCacheSweep, recovery_phase::RecoveryPhaseStore};
    let dir = keratin_log::test_dir!("phase_sweep");
    let (recipe, report, artifact) = completed_phase_fixture();
    let id = "a".repeat(64);
    let root = dir.root.join(&id);
    let store = RecoveryPhaseStore::open(&root, recipe.clone()).unwrap();
    store.save(&report, Some(&artifact)).unwrap();
    drop(store);
    let mut sweep = RecoveryCacheSweep::default();
    assert!(
        sweep
            .step(
                &dir.root,
                |found| {
                    assert_eq!(found, id);
                    assert!(root.join(".namespace.lock").exists());
                    Err(std::io::Error::other("metadata unavailable"))
                },
                32,
                1
            )
            .is_err()
    );
    assert!(!root.join("retired").exists());
    let mut sweep = RecoveryCacheSweep::default();
    let active = std::collections::HashSet::from([id]);
    while !sweep
        .step(&dir.root, |id| Ok(active.contains(id)), 1, 1)
        .unwrap()
        .cycle_complete
    {}
    assert!(
        RecoveryPhaseStore::open(&root, recipe)
            .unwrap()
            .load()
            .unwrap()
            .is_some()
    );
    let mut removed = 0;
    loop {
        let step = sweep.step(&dir.root, |_| Ok(false), 1, 1).unwrap();
        assert!(step.cleanup.removed_files <= 1);
        removed += step.cleanup.removed_bytes;
        if step.cycle_complete {
            break;
        }
    }
    assert!(removed > artifact.state_snapshot().len() as u64);
    assert!(root.join("reclaimed").exists());
}
