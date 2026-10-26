use keratin_log::*;
use std::time::{Duration, Instant};

fn message(i: u32) -> Message {
    Message {
        payload: format!("z{i:06}-{}", "y".repeat(300)).into_bytes(),
        flags: 0,
        headers: vec![],
    }
}

fn config() -> KeratinConfig {
    KeratinConfig {
        segment_preallocate_bytes: 2 * 1024 * 1024,
        ..KeratinConfig::test_default()
    }
}

fn assert_all(log: &Keratin, count: u32) {
    let got = log.reader().scan_from(0, count as usize + 10).unwrap();
    assert_eq!(got.len(), count as usize);
    for (i, rec) in got.iter().enumerate() {
        assert_eq!(rec.offset, i as u64);
        assert_eq!(rec.payload, message(i as u32).payload);
    }
}

/// Zeros land ahead of the data in the background, never over it, and a crash
/// with a zeroed tail recovers every synced record.
#[tokio::test]
async fn zeros_stay_ahead_of_data_and_survive_a_crash() {
    let dir = test_dir!("zero_ahead");
    let log = Keratin::open(&dir.root, config()).await.unwrap();
    for i in 0..2_000 {
        log.append_batch(vec![message(i)], Some(KDurability::AfterFsync))
            .await
            .unwrap();
    }
    let segment = util::latest_segment(&dir.root).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let data = 2_000 * 300;
    while std::fs::metadata(&segment).unwrap().len() < data + 1024 * 1024 {
        assert!(
            Instant::now() < deadline,
            "zeros never got ahead of the data"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_all(&log, 2_000);
    log.force_close().await.unwrap();

    let log = Keratin::open(&dir.root, config()).await.unwrap();
    assert_all(&log, 2_000);
    for i in 2_000..3_000 {
        log.append_batch(vec![message(i)], Some(KDurability::AfterFsync))
            .await
            .unwrap();
    }
    assert_all(&log, 3_000);
    log.shutdown().await.unwrap();

    let log = Keratin::open(&dir.root, config()).await.unwrap();
    assert_all(&log, 3_000);
    log.shutdown().await.unwrap();
}

fn allocated_bytes(path: &std::path::Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).unwrap().blocks() * 512
}

/// A log with only a few small records reserves and zeroes nothing. Once it
/// holds 64 KiB it reserves one chunk ahead, rounded to a MiB, and zeros stay
/// within the zeroing window of at least 4 MiB.
#[tokio::test]
async fn an_idle_log_reserves_nothing_until_it_holds_records() {
    let dir = test_dir!("zero_ahead_idle");
    let log = Keratin::open(&dir.root, config()).await.unwrap();
    let segment = util::latest_segment(&dir.root).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(allocated_bytes(&segment) < 64 * 1024, "idle segment reserved space");

    // A few small records, like a queue declaration, still reserve nothing.
    log.append_batch(vec![message(0)], Some(KDurability::AfterFsync)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let small = allocated_bytes(&segment);
    assert!(small < 64 * 1024, "a small record reserved {small}");

    for i in 1..400 {
        log.append_batch(vec![message(i)], Some(KDurability::AfterFsync)).await.unwrap();
    }
    let allocated = allocated_bytes(&segment);
    let chunk = config().segment_preallocate_bytes as u64;
    assert!(allocated >= chunk, "reserved {allocated}");
    let window = chunk.max(4 << 20);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let allocated = allocated_bytes(&segment);
    assert!(allocated <= window + (1 << 20), "reserved {allocated}");
    log.shutdown().await.unwrap();
}
