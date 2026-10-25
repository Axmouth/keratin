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
        log.append_batch(vec![message(i)], Some(KDurability::AfterFsync)).await.unwrap();
    }
    let segment = util::latest_segment(&dir.root).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let data = 2_000 * 300;
    while std::fs::metadata(&segment).unwrap().len() < data + 1024 * 1024 {
        assert!(Instant::now() < deadline, "zeros never got ahead of the data");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_all(&log, 2_000);
    log.force_close().await.unwrap();

    let log = Keratin::open(&dir.root, config()).await.unwrap();
    assert_all(&log, 2_000);
    for i in 2_000..3_000 {
        log.append_batch(vec![message(i)], Some(KDurability::AfterFsync)).await.unwrap();
    }
    assert_all(&log, 3_000);
    log.shutdown().await.unwrap();

    let log = Keratin::open(&dir.root, config()).await.unwrap();
    assert_all(&log, 3_000);
    log.shutdown().await.unwrap();
}
