use keratin_log::*;
use std::path::{Path, PathBuf};

fn message(payload: impl Into<Vec<u8>>) -> Message {
    Message {
        payload: payload.into(),
        flags: 0,
        headers: vec![],
    }
}

/// Small segments so a few hundred records roll several times.
fn small_segments() -> KeratinConfig {
    KeratinConfig {
        segment_max_bytes: 16 * 1024,
        index_stride_bytes: 1024,
        force_recovery_scan: true,
        ..KeratinConfig::test_default()
    }
}

fn files_with_extension(root: &Path, ext: &str) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(root.join("segments"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == ext))
        .collect();
    out.sort();
    out
}

async fn append_range(log: &Keratin, from: u32, to: u32) {
    for i in from..to {
        log.append_batch(
            vec![message(format!("r{i:05}-{}", "x".repeat(200)))],
            Some(KDurability::AfterFsync),
        )
        .await
        .unwrap();
    }
}

fn assert_reads(log: &Keratin, from: u64, to: u64) {
    let got = log
        .reader()
        .scan_from(from, (to - from) as usize + 10)
        .unwrap();
    assert_eq!(got.len() as u64, to - from, "records {from}..{to}");
    for (i, rec) in got.iter().enumerate() {
        let offset = from + i as u64;
        assert_eq!(rec.offset, offset);
        assert!(rec.payload.starts_with(format!("r{offset:05}-").as_bytes()));
    }
}

/// Fill segments, truncate so freed ones become spares, then roll into them.
async fn log_with_recycled_active(root: &Path) -> Keratin {
    let log = Keratin::open(root, small_segments()).await.unwrap();
    append_range(&log, 0, 300).await;
    log.truncate_before(200).await.unwrap();
    assert!(
        !files_with_extension(root, "spare").is_empty(),
        "truncate keeps spares"
    );
    let spares_before = files_with_extension(root, "spare").len();
    append_range(&log, 300, 400).await;
    assert!(
        files_with_extension(root, "spare").len() < spares_before,
        "rolls reuse spares"
    );
    log
}

#[tokio::test]
async fn recycled_segments_read_back_and_survive_a_crash() {
    let dir = test_dir!("reuse_crash");
    let log = log_with_recycled_active(&dir.root).await;
    let head = log.head_offset();
    assert_reads(&log, head, 400);
    log.force_close().await.unwrap();

    let log = Keratin::open(&dir.root, small_segments()).await.unwrap();
    assert_eq!(log.next_offset(), 400);
    assert_reads(&log, head, 400);
    append_range(&log, 400, 450).await;
    assert_reads(&log, head, 450);
    log.shutdown().await.unwrap();

    let log = Keratin::open(&dir.root, small_segments()).await.unwrap();
    assert_reads(&log, head, 450);
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn recycled_active_segment_opens_as_bound_history() {
    let dir = test_dir!("reuse_bound");
    let log = log_with_recycled_active(&dir.root).await;
    let head = log.head_offset();
    log.force_close().await.unwrap();

    let log = Keratin::open_preserving_history(&dir.root, small_segments())
        .await
        .unwrap();
    assert_eq!(log.next_offset(), 400);
    assert_reads(&log, head, 400);
    log.shutdown().await.unwrap();
}

/// A crash after a spare was renamed into place but before its new header
/// reached disk leaves the newest file carrying an older segment's header.
#[tokio::test]
async fn renamed_spare_without_its_header_is_dropped() {
    let dir = test_dir!("reuse_unstarted");
    let log = Keratin::open(&dir.root, small_segments()).await.unwrap();
    append_range(&log, 0, 300).await;
    log.truncate_before(200).await.unwrap();
    let next = log.next_offset();
    log.force_close().await.unwrap();

    let spare = files_with_extension(&dir.root, "spare").remove(0);
    let renamed = dir.root.join("segments").join(format!("{next:020}.log"));
    std::fs::rename(&spare, &renamed).unwrap();

    let log = Keratin::open(&dir.root, small_segments()).await.unwrap();
    assert_eq!(log.next_offset(), next);
    assert!(!renamed.exists(), "the unstarted segment is removed");
    assert_reads(&log, log.head_offset(), next);
    append_range(&log, 300, 320).await;
    assert_reads(&log, log.head_offset(), 320);
    log.shutdown().await.unwrap();
}

/// The index syncs lazily, so a crash can lose its tail or the whole file.
/// Recovery rebuilds it from the log.
#[tokio::test]
async fn lost_index_is_rebuilt_after_a_crash() {
    let dir = test_dir!("reuse_index");
    let cfg = KeratinConfig {
        segment_max_bytes: 1024 * 1024,
        ..small_segments()
    };
    let log = Keratin::open(&dir.root, cfg).await.unwrap();
    append_range(&log, 0, 200).await;
    log.force_close().await.unwrap();

    let index = files_with_extension(&dir.root, "idx").pop().unwrap();
    // Index header: magic, version, flags, length, base, created, entry length, reserved, crc.
    let header = 8 + 2 + 2 + 4 + 8 + 8 + 2 + 2 + 32 + 4;
    let full = std::fs::read(&index).unwrap();
    // Keep the header, drop every entry, then add one garbage entry pointing
    // past the log end.
    let mut damaged = full[..header].to_vec();
    damaged.extend_from_slice(&199u32.to_be_bytes());
    damaged.extend_from_slice(&0u32.to_be_bytes());
    damaged.extend_from_slice(&u64::MAX.to_be_bytes());
    std::fs::write(&index, &damaged).unwrap();

    let log = Keratin::open(&dir.root, cfg).await.unwrap();
    assert!(
        std::fs::metadata(&index).unwrap().len() as usize > header + 16,
        "entries rebuilt"
    );
    for from in [0u64, 57, 120, 199] {
        assert_reads(&log, from, 200);
    }
    log.shutdown().await.unwrap();
}
