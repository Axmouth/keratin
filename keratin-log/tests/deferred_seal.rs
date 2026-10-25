use keratin_log::*;
use std::path::{Path, PathBuf};

fn message(i: u32) -> Message {
    Message {
        payload: format!("r{i:05}-{}", "x".repeat(200)).into_bytes(),
        flags: 0,
        headers: vec![],
    }
}

fn small_segments() -> KeratinConfig {
    KeratinConfig {
        segment_max_bytes: 16 * 1024,
        index_stride_bytes: 1024,
        ..KeratinConfig::test_default()
    }
}

fn segment_logs(root: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(root.join("segments"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "log"))
        .collect();
    out.sort();
    out
}

fn assert_contiguous(log: &Keratin, to: u64) {
    let got = log.reader().scan_from(0, to as usize + 10).unwrap();
    assert_eq!(got.len() as u64, to);
    for (i, rec) in got.iter().enumerate() {
        assert_eq!(rec.offset, i as u64);
        assert!(rec.payload.starts_with(format!("r{i:05}-").as_bytes()));
    }
}

/// A roll leaves the outgoing segment's sync to the next fsync. A crash
/// before that can leave the sealed segment short while the next segment
/// already holds records. None of them were acknowledged, so recovery cuts
/// the log at the end of the sealed segment and drops what follows.
#[tokio::test]
async fn crash_before_the_deferred_seal_sync_cuts_at_the_short_segment() {
    let dir = test_dir!("deferred_seal_short");
    let log = Keratin::open(&dir.root, small_segments()).await.unwrap();
    for i in 0..60 {
        log.append_batch(vec![message(i)], Some(KDurability::AfterFsync))
            .await
            .unwrap();
    }
    for i in 60..200 {
        log.append_batch(vec![message(i)], Some(KDurability::AfterFlush))
            .await
            .unwrap();
    }
    log.force_close().await.unwrap();

    let logs = segment_logs(&dir.root);
    assert!(logs.len() >= 3, "expected several rolls, got {logs:?}");
    // The second-to-last segment lost its tail, the last one survived.
    let short = &logs[logs.len() - 2];
    let len = std::fs::metadata(short).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(short)
        .unwrap()
        .set_len(len - 300)
        .unwrap();

    let log = Keratin::open(&dir.root, small_segments()).await.unwrap();
    let next = log.next_offset();
    assert!(next >= 60, "acknowledged records lost, next {next}");
    assert!(next < 200);
    assert_eq!(segment_logs(&dir.root).len(), logs.len() - 1);
    assert_contiguous(&log, next);

    for i in next as u32..next as u32 + 100 {
        log.append_batch(vec![message(i)], Some(KDurability::AfterFsync))
            .await
            .unwrap();
    }
    assert_contiguous(&log, next + 100);
    log.shutdown().await.unwrap();

    let log = Keratin::open(&dir.root, small_segments()).await.unwrap();
    assert_contiguous(&log, next + 100);
    log.shutdown().await.unwrap();
}

/// Records written before a roll become durable with the next fsync, and a
/// clean reopen keeps every one of them.
#[tokio::test]
async fn records_sealed_by_a_roll_survive_a_clean_reopen() {
    let dir = test_dir!("deferred_seal_clean");
    let log = Keratin::open(&dir.root, small_segments()).await.unwrap();
    for i in 0..300 {
        log.append_batch(vec![message(i)], Some(KDurability::AfterFsync))
            .await
            .unwrap();
    }
    assert_eq!(log.durable_next(), 300);
    log.shutdown().await.unwrap();
    let log = Keratin::open(&dir.root, small_segments()).await.unwrap();
    assert_contiguous(&log, 300);
    log.shutdown().await.unwrap();
}
