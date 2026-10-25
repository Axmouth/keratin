use keratin_log::*;
use std::time::Duration;

fn message(value: u8) -> Message {
    Message {
        flags: 0,
        headers: vec![value],
        payload: vec![value; 256],
    }
}

// No background sync, so written records stay unsynced until the test syncs.
fn config(tail_cache_bytes: usize) -> KeratinConfig {
    KeratinConfig {
        background_sync_interval: Duration::ZERO,
        tail_cache_bytes,
        ..KeratinConfig::test_default()
    }
}

fn written(log: &Keratin, from: u64, max: usize, max_bytes: usize) -> Vec<OwnedRecord> {
    log.reader()
        .scan_written_with_byte_budget(from, max, max_bytes)
        .unwrap()
}

// Written reads see records as soon as they reach the OS, from the tail cache
// or from the file, while ordinary reads wait for the sync.
#[tokio::test]
async fn written_reads_see_unsynced_records_from_cache_and_file() {
    for cache in [64 * 1024 * 1024, 0] {
        let dir = test_dir!("written_reads");
        let log = Keratin::open(&dir.root, config(cache)).await.unwrap();
        log.append(message(0), Some(KDurability::AfterFsync))
            .await
            .unwrap();
        for value in 1..5 {
            log.append(message(value), Some(KDurability::AfterFlush))
                .await
                .unwrap();
        }
        // Staged but not written: never visible.
        log.append(message(5), Some(KDurability::AfterWrite))
            .await
            .unwrap();
        assert_eq!(log.durable_next(), 1);
        assert_eq!(log.written_next(), 5);

        assert_eq!(log.reader().scan_from(0, 10).unwrap().len(), 1);
        let records = written(&log, 0, 10, usize::MAX);
        assert_eq!(records.len(), 5, "cache {cache}");
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.offset, i as u64);
            assert_eq!(record.headers, vec![i as u8]);
            assert_eq!(record.payload, vec![i as u8; 256]);
        }
        // Start in the middle, cap the count, and keep the byte lookahead.
        let tail = written(&log, 2, 2, usize::MAX);
        assert_eq!(
            tail.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![2, 3]
        );
        let budgeted = written(&log, 1, 10, 300);
        assert_eq!(
            budgeted.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(written(&log, 5, 10, usize::MAX).is_empty());

        log.sync().await.unwrap();
        assert_eq!(written(&log, 0, 10, usize::MAX).len(), 6);
        assert_eq!(log.reader().scan_from(0, 10).unwrap().len(), 6);
        log.shutdown().await.unwrap();
    }
}

// A written read across a segment roll finds the unsynced records in the new
// segment file as well.
#[tokio::test]
async fn written_reads_cross_a_segment_roll() {
    let dir = test_dir!("written_reads_roll");
    let cfg = KeratinConfig {
        segment_max_bytes: 4096,
        ..config(0)
    };
    let log = Keratin::open(&dir.root, cfg).await.unwrap();
    for value in 0..40 {
        log.append(message(value), Some(KDurability::AfterFlush))
            .await
            .unwrap();
    }
    assert!(log.segment_infos().unwrap().len() > 1);
    let records = written(&log, 0, 100, usize::MAX);
    assert_eq!(records.len(), 40);
    assert!(
        records
            .iter()
            .enumerate()
            .all(|(i, r)| r.offset == i as u64)
    );
    log.shutdown().await.unwrap();
}

// Controls that need the whole log durable sync records written without a
// waiting append, which is how every AfterFlush append ends up. A recovery
// seal fences the epoch and then reads the log up to its durable end.
#[tokio::test]
async fn epoch_fence_syncs_unwaited_written_records() {
    let dir = test_dir!("written_fence_sync");
    let log = Keratin::open(&dir.root, config(64 * 1024 * 1024))
        .await
        .unwrap();
    for value in 0..4 {
        log.append(message(value), Some(KDurability::AfterFlush))
            .await
            .unwrap();
    }
    assert_eq!(log.durable_next(), 0);
    log.advance_epoch(1).await.unwrap();
    assert_eq!(
        log.durable_next(),
        4,
        "the fence must cover the written records"
    );
    assert_eq!(log.reader().scan_from(0, 10).unwrap().len(), 4);
    log.shutdown().await.unwrap();
}
