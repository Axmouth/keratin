//! Reproducible two-log trace exercise. Arguments: fresh-root records-per-log.
use keratin_log::{KDurability, Keratin, KeratinConfig, Message};
use std::{path::PathBuf, sync::Arc, time::Instant};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let mut args = std::env::args().skip(1);
    let root = PathBuf::from(args.next().expect("fresh output directory required"));
    let count: usize = args
        .next()
        .unwrap_or_else(|| "131072".into())
        .parse()
        .unwrap();
    assert!(count > 0);
    let config = KeratinConfig {
        max_batch_records: 256,
        fsync_interval_ms: 5,
        batch_linger_ms: 5,
        segment_max_bytes: 32 * 1024 * 1024,
        tail_cache_bytes: 0,
        default_durability: KDurability::AfterFsync,
        ..KeratinConfig::default()
    };
    if args.next().as_deref() == Some("--validate") {
        for name in ["messages", "events"] {
            let log = Keratin::open(root.join(name), config).await.unwrap();
            let reader = log.reader();
            let mut offset = 0;
            let mut seen = vec![false; count];
            while offset < count {
                let records = reader
                    .scan_from_disk(offset as u64, (count - offset).min(4096))
                    .unwrap();
                assert!(!records.is_empty());
                for record in records {
                    assert_eq!(record.offset, offset as u64);
                    assert_eq!(record.payload.len(), 1024);
                    let identity =
                        u64::from_le_bytes(record.payload[..8].try_into().unwrap()) as usize;
                    assert!(identity < count && !seen[identity]);
                    seen[identity] = true;
                    assert!(record.payload[8..].iter().all(|b| *b == 42));
                    offset += 1;
                }
            }
            assert!(seen.into_iter().all(|present| present));
            assert!(reader.scan_from_disk(count as u64, 1).unwrap().is_empty());
            log.shutdown().await.unwrap();
        }
        println!("validated_records={}", count * 2);
        return;
    }
    std::fs::create_dir(&root).expect("output directory must be new");
    let mut logs = Vec::new();
    for name in ["messages", "events"] {
        logs.push(Arc::new(
            Keratin::open(root.join(name), config).await.unwrap(),
        ));
    }
    let start = Instant::now();
    let mut tasks = Vec::new();
    for log in &logs {
        for producer in 0..4 {
            let log = log.clone();
            tasks.push(tokio::spawn(async move {
                for first in (producer * 64..count).step_by(4 * 64) {
                    let records = (first..(first + 64).min(count))
                        .map(|i| {
                            let mut payload = vec![42; 1024];
                            payload[..8].copy_from_slice(&(i as u64).to_le_bytes());
                            Message {
                                flags: 0,
                                headers: vec![],
                                payload,
                            }
                        })
                        .collect();
                    log.append_batch(records, None).await.unwrap();
                }
            }));
        }
    }
    for task in tasks {
        task.await.unwrap();
    }
    let elapsed = start.elapsed();
    for log in &logs {
        log.shutdown().await.unwrap();
    }
    drop(logs);
    #[cfg(feature = "writer-stage-trace")]
    {
        keratin_log::flush_writer_stage_trace().unwrap();
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match keratin_log::finish_writer_stage_trace() {
                Ok(()) => break,
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        && Instant::now() < deadline =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
                Err(error) => panic!("trace shutdown: {error}"),
            }
        }
    }
    // Run again with --validate and no trace environment to verify persisted data.
    println!(
        "records={} elapsed_seconds={:.6} records_per_second={:.0}",
        count * 2,
        elapsed.as_secs_f64(),
        (count * 2) as f64 / elapsed.as_secs_f64()
    );
}
