//! Exercise adaptive linger followed by a small tail through the real writer.
//! Timings are diagnostic only. Correctness assertions are independent of timing.
use keratin_log::*;
use std::time::Instant;

#[tokio::test]
async fn adaptive_batch_tail_remains_durable_and_ordered() {
    let dir = test_dir!("adaptive_batch_tail");
    let cfg = KeratinConfig {
        max_batch_bytes: 1024,
        batch_linger_ms: 50,
        writer_buffer_factor: 1,
        tail_cache_bytes: 0,
        ..KeratinConfig::test_default()
    };
    let log = Keratin::open(&dir.root, cfg.clone()).await.unwrap();
    let mut expected = Vec::new();
    // Each medium append raises adaptive linger by one millisecond. Sequential
    // durable waits deliberately drain the local writer input between appends.
    for i in 0..24 {
        let payload = vec![i; 600];
        log.append_batch(
            vec![Message {
                flags: 0,
                headers: vec![],
                payload: payload.clone(),
            }],
            Some(KDurability::AfterFsync),
        )
        .await
        .unwrap();
        expected.push(payload);
    }
    let mut latencies = Vec::new();
    for i in 24..32 {
        let payload = vec![i; 32];
        let start = Instant::now();
        log.append_batch(
            vec![Message {
                flags: 0,
                headers: vec![],
                payload: payload.clone(),
            }],
            Some(KDurability::AfterFsync),
        )
        .await
        .unwrap();
        latencies.push(start.elapsed().as_secs_f64() * 1000.0);
        expected.push(payload);
    }
    println!("DRAIN_PROBE tail_confirm_ms={latencies:?}");
    log.shutdown().await.unwrap();
    drop(log);
    let reopened = Keratin::open(&dir.root, cfg).await.unwrap();
    let records = reopened.reader().scan_from_disk(0, 100).unwrap();
    assert_eq!(records.len(), expected.len());
    for (record, payload) in records.iter().zip(expected) {
        assert_eq!(record.payload, payload);
    }
    reopened.shutdown().await.unwrap();
}
