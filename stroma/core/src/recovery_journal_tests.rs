fn journal_identity(
    records: &[RecoveryRecord],
) -> crate::recovery_journal::RecoveryJournalIdentity {
    crate::recovery_journal::RecoveryJournalIdentity {
        topic: "q".into(),
        partition: 0,
        group: None,
        replica: "node-a".into(),
        seal: combined_seal(0, records),
    }
}
#[test]
fn inspection_journal_reopens_tentative_pages_and_preserves_full_validation() {
    use crate::recovery_journal::RecoveryInspectionJournal as Journal;
    let dir = keratin_log::test_dir!("inspection_journal_resume");
    let records = events(vec![
        payload_event(None),
        payload_event(Some(7)),
        StromaEvent::Ack { off: 0 },
    ]);
    let identity = journal_identity(&records);
    let make_check = || {
        combined_inspector(
            &records,
            RecoveryInspectionLimits {
                page_records: 1,
                ..Default::default()
            },
        )
        .with_single_source()
        .unwrap()
    };
    let mut check = make_check();
    let (side, request) = check.next_read().unwrap().unwrap();
    let response = page(&request, &records, 3);
    let journal = Journal::open(&dir.root, identity.clone()).unwrap();
    assert!(Journal::open(&dir.root, identity.clone()).is_err());
    journal.append(&request, &response).unwrap();
    journal.append(&request, &response).unwrap();
    check.accept_page(side, response.clone()).unwrap();
    assert!(check.finish().is_err()); // a durable cached page grants no evidence
    drop(journal);
    let journal = Journal::open(&dir.root, identity).unwrap();
    assert_eq!(journal.read(&request).unwrap(), Some(response));
    let mut check = make_check();
    let mut fetched = 0;
    while let Some((side, request)) = check.next_read().unwrap() {
        let response = match journal.read(&request).unwrap() {
            Some(response) => response,
            None => {
                fetched += 1;
                let response = page(&request, &records, 3);
                journal.append(&request, &response).unwrap();
                response
            }
        };
        check.accept_page(side, response).unwrap();
    }
    assert_eq!(fetched, 2); // live verification reuses the same sealed pages
    let restored = check.finish_with_queue_artifacts(4096).unwrap();
    let direct = drive_checkpoints(make_check(), [&records, &records], [&[], &[]])
        .unwrap()
        .finish_with_queue_artifacts(4096)
        .unwrap();
    assert_eq!(restored, direct);
}

#[test]
fn inspection_journal_binds_source_transition_resource_and_page_limits() {
    use crate::recovery_journal::RecoveryInspectionJournal as Journal;
    let dir = keratin_log::test_dir!("inspection_journal_binding");
    let records = events(vec![payload_event(None)]);
    let identity = journal_identity(&records);
    let check = combined_inspector(&records, Default::default());
    let (_, request) = check.next_read().unwrap().unwrap();
    let response = page(&request, &records, 1);
    let journal = Journal::open(&dir.root, identity.clone()).unwrap();
    journal.append(&request, &response).unwrap();
    let mut other_node = identity.clone();
    other_node.replica = "node-b".into();
    let other = Journal::open(&dir.root, other_node).unwrap();
    assert!(other.read(&request).unwrap().is_none()); // independent witnesses
    let mut changed = request.clone();
    changed.seal.fence_epoch += 1;
    assert!(journal.read(&changed).is_err());
    let mut changed = request.clone();
    changed.history_id[0] ^= 1;
    assert!(journal.append(&changed, &response).is_err());
    let mut changed = request.clone();
    changed.max_bytes = 1;
    assert!(journal.append(&changed, &response).is_err());
    let mut changed = response.clone();
    changed.records[0].payload[20] ^= 1;
    assert!(
        journal
            .append(&request, &changed)
            .unwrap_err()
            .to_string()
            .contains("conflicting")
    );
    let mut identity = identity;
    identity.topic = "other".into();
    assert!(Journal::open(&dir.root, identity).is_err());
    let mut changed = response;
    changed.records[0].offset += 1;
    assert!(journal.append(&request, &changed).is_err());
}

#[test]
fn inspection_journal_ignores_scratch_and_rejects_corrupt_committed_frames() {
    use crate::recovery_journal::RecoveryInspectionJournal as Journal;
    let dir = keratin_log::test_dir!("inspection_journal_corrupt");
    let records = events(vec![payload_event(None)]);
    let identity = journal_identity(&records);
    let (_, request) = combined_inspector(&records, Default::default())
        .next_read()
        .unwrap()
        .unwrap();
    let response = page(&request, &records, 1);
    let journal = Journal::open(&dir.root, identity.clone()).unwrap();
    let root = std::fs::read_dir(&dir.root)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::write(root.join("page.writing"), b"interrupted download").unwrap();
    assert!(journal.read(&request).unwrap().is_none());
    journal.append(&request, &response).unwrap();
    let path = std::fs::read_dir(&root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "page"))
        .unwrap();
    let good = std::fs::read(&path).unwrap();
    for len in [0, 8, 39, good.len() - 1] {
        std::fs::write(&path, &good[..len]).unwrap();
        assert!(journal.read(&request).is_err());
    }
    let mut corrupt = good.clone();
    corrupt[20] ^= 1;
    std::fs::write(&path, corrupt).unwrap();
    assert!(
        journal
            .read(&request)
            .unwrap_err()
            .to_string()
            .contains("checksum")
    );
    std::fs::write(path, good).unwrap();
    // Failed new-page publication cannot damage a previously durable page.
    std::fs::create_dir(root.join("page.writing")).unwrap();
    let mut smaller = request.clone();
    smaller.max_records = 1;
    assert!(journal.append(&smaller, &response).is_err());
    assert_eq!(journal.read(&request).unwrap(), Some(response));
    drop(journal);
    let journal = Journal::open(&dir.root, identity).unwrap();
    assert!(journal.read(&request).unwrap().is_some());
}

#[test]
#[cfg(unix)]
fn inspection_journal_crash_child() {
    let Ok(root) = std::env::var("STROMA_TEST_JOURNAL_CRASH_ROOT") else {
        return;
    };
    let records = events(vec![payload_event(None)]);
    let journal = crate::recovery_journal::RecoveryInspectionJournal::open(
        std::path::Path::new(&root),
        journal_identity(&records),
    )
    .unwrap();
    let (_, request) = combined_inspector(&records, Default::default())
        .next_read()
        .unwrap()
        .unwrap();
    journal
        .append(&request, &page(&request, &records, 1))
        .unwrap();
    panic!("crash gate was not reached");
}

#[test]
#[cfg(unix)]
fn inspection_journal_sigkill_at_publication_boundaries_preserves_only_complete_pages() {
    use crate::recovery_journal::RecoveryInspectionJournal as Journal;
    for phase in ["file_synced", "renamed", "directory_synced"] {
        let dir = keratin_log::test_dir!("inspection_journal_sigkill");
        let root = dir.root.join("journal");
        let marker = dir.root.join("gate");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "recovery_inspection::tests::inspection_journal_crash_child",
                "--nocapture",
            ])
            .env("STROMA_TEST_JOURNAL_CRASH_ROOT", &root)
            .env("STROMA_TEST_JOURNAL_CRASH_PHASE", phase)
            .env("STROMA_TEST_JOURNAL_CRASH_MARKER", &marker)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !marker.exists() && std::time::Instant::now() < deadline {
            if let Some(status) = child.try_wait().unwrap() {
                panic!("journal crash child exited before {phase}: {status}");
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let reached = marker.exists();
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(reached, "journal crash child did not reach {phase}");
        let records = events(vec![payload_event(None)]);
        let journal = Journal::open(&root, journal_identity(&records)).unwrap();
        let (_, request) = combined_inspector(&records, Default::default())
            .next_read()
            .unwrap()
            .unwrap();
        let response = page(&request, &records, 1);
        if phase == "file_synced" {
            assert!(journal.read(&request).unwrap().is_none());
        } else {
            assert_eq!(journal.read(&request).unwrap(), Some(response.clone()));
        }
        journal.append(&request, &response).unwrap();
        assert_eq!(journal.read(&request).unwrap(), Some(response));
    }
}

#[test]
fn inspection_journal_disk_admission_preserves_progress_and_cleanup_reclaims_pages() {
    use crate::recovery_journal::{RecoveryInspectionJournal as Journal, RecoveryJournalLimits};
    let dir = keratin_log::test_dir!("inspection_journal_disk_budget");
    let records = events(vec![payload_event(None), payload_event(None)]);
    let identity = journal_identity(&records);
    let (_, request) = combined_inspector(
        &records,
        RecoveryInspectionLimits {
            page_records: 1,
            ..Default::default()
        },
    )
    .next_read()
    .unwrap()
    .unwrap();
    let first = page(&request, &records, 2);
    let journal = Journal::open(&dir.root, identity.clone()).unwrap();
    journal.append(&request, &first).unwrap();
    drop(journal);
    let journal = Journal::open_with_limits(
        &dir.root,
        identity.clone(),
        RecoveryJournalLimits {
            reserve_free_bytes: u64::MAX,
        },
    )
    .unwrap();
    let mut next = request.clone();
    next.from = 1;
    assert_eq!(
        journal
            .append(&next, &page(&next, &records, 2))
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::StorageFull
    );
    assert_eq!(journal.read(&request).unwrap(), Some(first));
    assert!(journal.read(&next).unwrap().is_none());
    drop(journal);
    let journal = Journal::open(&dir.root, identity).unwrap();
    journal.append(&next, &page(&next, &records, 2)).unwrap();
    assert!(journal.clear_pages().unwrap() > 0);
    assert!(journal.read(&request).unwrap().is_none());
    assert_eq!(journal.clear_pages().unwrap(), 0);
    assert!(Journal::open(&dir.root, journal_identity(&records)).is_err());
}
