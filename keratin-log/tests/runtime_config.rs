use keratin_log::*;
use std::sync::Arc;
fn policy(bytes: usize) -> LogRuntimeConfig {
    LogRuntimeConfig {
        segment_preallocate_bytes: bytes,
    }
}
fn config() -> KeratinConfig {
    KeratinConfig {
        segment_max_bytes: 1024,
        writer_buffer_factor: 1,
        tail_cache_bytes: 0,
        ..KeratinConfig::test_default()
    }
}
fn message(i: u8) -> Message {
    Message {
        flags: 0,
        headers: vec![],
        payload: vec![i; 80],
    }
}

#[tokio::test]
async fn runtime_preallocation_waits_for_rollover_and_preserves_records() {
    let dir = test_dir!("runtime_preallocation");
    let runtime = Arc::new(LogRuntimeSettings::new(policy(0)).unwrap());
    let log = Keratin::open_with_runtime(&dir.root, config(), false, runtime.clone())
        .await
        .unwrap();
    log.append_batch(vec![message(0)], Some(KDurability::AfterFsync))
        .await
        .unwrap();
    let before = runtime.logs();
    let old_base = before[0].active_segment_base;
    let files_before: Vec<_> = std::fs::read_dir(dir.root.join("segments"))
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            (e.path(), e.metadata().unwrap().len())
        })
        .collect();
    runtime.replace(0, policy(4096)).unwrap();
    assert_eq!(runtime.logs()[0].applied.revision, 0);
    for (path, len) in files_before {
        assert_eq!(std::fs::metadata(path).unwrap().len(), len);
    }
    for i in 1..24 {
        log.append_batch(vec![message(i)], Some(KDurability::AfterFsync))
            .await
            .unwrap();
    }
    let status = &runtime.logs()[0];
    assert_eq!(status.applied.revision, 1);
    assert_ne!(status.active_segment_base, old_base);
    assert_eq!(status.effective_preallocate_bytes, 4096);
    assert!(status.allocation_error.is_none());
    runtime.replace(1, policy(0)).unwrap();
    for i in 24..48 {
        log.append_batch(vec![message(i)], Some(KDurability::AfterFsync))
            .await
            .unwrap();
    }
    assert_eq!(runtime.logs()[0].applied.revision, 2);
    assert_eq!(runtime.logs()[0].effective_preallocate_bytes, 0);
    log.shutdown().await.unwrap();
    drop(log);
    let reopened = Keratin::open_with_runtime(&dir.root, config(), true, runtime.clone())
        .await
        .unwrap();
    let records = reopened.reader().scan_from_disk(0, 100).unwrap();
    assert_eq!(records.len(), 48);
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.payload, vec![i as u8; 80]);
    }
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn new_logs_use_current_policy_and_cas_does_not_lose_concurrent_edits() {
    let a = test_dir!("runtime_new_a");
    let b = test_dir!("runtime_new_b");
    let runtime = Arc::new(LogRuntimeSettings::new(policy(0)).unwrap());
    let first = Keratin::open_with_runtime(&a.root, config(), false, runtime.clone())
        .await
        .unwrap();
    let mut jobs = Vec::new();
    for bytes in [4096, 8192] {
        let runtime = runtime.clone();
        jobs.push(std::thread::spawn(move || {
            runtime.replace(0, policy(bytes))
        }));
    }
    assert_eq!(
        jobs.into_iter()
            .map(|j| j.join().unwrap().is_ok() as usize)
            .sum::<usize>(),
        1
    );
    if cfg!(target_pointer_width = "64") {
        assert!(runtime.replace(1, policy(usize::MAX)).is_err());
    }
    assert_eq!(runtime.current().revision, 1);
    let second = Keratin::open_with_runtime(&b.root, config(), false, runtime.clone())
        .await
        .unwrap();
    let statuses = runtime.logs();
    assert_eq!(
        statuses
            .iter()
            .find(|s| s.root == a.root)
            .unwrap()
            .applied
            .revision,
        0
    );
    assert_eq!(
        statuses.iter().find(|s| s.root == b.root).unwrap().applied,
        runtime.current()
    );
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn reset_segment_adopts_policy_without_changing_epoch_or_checkpoint() {
    let dir = test_dir!("runtime_checkpoint");
    let runtime = Arc::new(LogRuntimeSettings::new(policy(0)).unwrap());
    let log = Keratin::open_with_runtime(&dir.root, config(), false, runtime.clone())
        .await
        .unwrap();
    log.advance_epoch(7).await.unwrap();
    log.become_follower();
    runtime.replace(0, policy(4096)).unwrap();
    log.destructive_reset_to_checkpoint_at_epoch(42, 7)
        .await
        .unwrap();
    assert_eq!(runtime.logs()[0].applied.revision, 1);
    assert_eq!(log.current_epoch(), 7);
    assert_eq!(log.next_offset(), 42);
    log.shutdown().await.unwrap();
}
