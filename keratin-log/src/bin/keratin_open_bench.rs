use std::time::Instant;

use keratin_log::{KDurability, Keratin, KeratinConfig, Message};

#[tokio::main]
async fn main() {
    let messages = arg_value("--messages")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(200_000);
    let batch = arg_value("--batch")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(2048);
    let payload_len = arg_value("--payload")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1024);
    let segment_mb = arg_value("--segment-mb")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(16);

    let root = std::env::temp_dir().join(format!("keratin-open-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);

    let cfg = KeratinConfig {
        segment_max_bytes: segment_mb * 1024 * 1024,
        index_stride_bytes: 64 * 1024,
        max_batch_bytes: 8 * 1024 * 1024,
        max_batch_records: batch,
        batch_linger_ms: 5,
        default_durability: KDurability::AfterFsync,
        fsync_interval_ms: 5,
        flush_target_bytes: 32 * 1024 * 1024,
        force_recovery_scan: has_flag("--force-recovery-scan"),
    };

    let started = Instant::now();
    let k = Keratin::open(&root, cfg.clone()).await.unwrap();
    let open_empty_ms = started.elapsed().as_secs_f64() * 1000.0;

    let payload = vec![7u8; payload_len];
    let started = Instant::now();
    let mut written = 0usize;
    while written < messages {
        let take = batch.min(messages - written);
        let mut records = Vec::with_capacity(take);
        for _ in 0..take {
            records.push(Message {
                flags: 0,
                headers: Vec::new(),
                payload: payload.clone(),
            });
        }
        k.append_batch(records, None).await.unwrap();
        written += take;
    }
    let write_ms = started.elapsed().as_secs_f64() * 1000.0;

    let started = Instant::now();
    k.shutdown().await.unwrap();
    let shutdown_ms = started.elapsed().as_secs_f64() * 1000.0;

    let segment_count = std::fs::read_dir(root.join("segments"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "log"))
        .count();

    let started = Instant::now();
    let reopened = Keratin::open(&root, cfg).await.unwrap();
    let reopen_ms = started.elapsed().as_secs_f64() * 1000.0;

    let started = Instant::now();
    reopened.shutdown().await.unwrap();
    let reopen_shutdown_ms = started.elapsed().as_secs_f64() * 1000.0;

    println!("root={}", root.display());
    println!("messages={messages}");
    println!("payload_len={payload_len}");
    println!("segment_mb={segment_mb}");
    println!("force_recovery_scan={}", cfg.force_recovery_scan);
    println!("segments={segment_count}");
    println!("open_empty_ms={open_empty_ms:.3}");
    println!("write_ms={write_ms:.3}");
    println!("shutdown_ms={shutdown_ms:.3}");
    println!("reopen_after_clean_shutdown_ms={reopen_ms:.3}");
    println!("reopen_shutdown_ms={reopen_shutdown_ms:.3}");

    let _ = std::fs::remove_dir_all(&root);
}

fn arg_value(name: &str) -> Option<String> {
    let mut args = std::env::args();
    while let Some(arg) = args.next() {
        if arg == name {
            return args.next();
        }
    }
    None
}

fn has_flag(name: &str) -> bool {
    std::env::args().any(|arg| arg == name)
}
