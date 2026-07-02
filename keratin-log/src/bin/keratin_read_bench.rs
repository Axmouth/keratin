use std::path::PathBuf;
use std::time::Instant;

use keratin_log::{KDurability, Keratin, KeratinConfig, Message};

#[tokio::main]
async fn main() {
    let messages = arg_value("--messages")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1_000_000);
    let batch = arg_value("--batch")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(4096);
    let page = arg_value("--page")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(4096);
    let payload_len = arg_value("--payload")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1024);
    let segment_mb = arg_value("--segment-mb")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(16);
    let fetches = arg_value("--fetches")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100_000);
    let fetch_stride = arg_value("--fetch-stride")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(9973);

    let root: PathBuf = arg_value("--root").map(Into::into).unwrap_or_else(|| {
        std::env::temp_dir().join(format!("keratin-read-bench-{}", std::process::id()))
    });
    let keep = has_flag("--keep");
    let _ = std::fs::remove_dir_all(&root);

    let cfg = KeratinConfig {
        segment_max_bytes: segment_mb * 1024 * 1024,
        index_stride_bytes: 64 * 1024,
        max_batch_bytes: 8 * 1024 * 1024,
        max_batch_records: batch,
        batch_linger_ms: 5,
        default_durability: KDurability::AfterFsync,
        fsync_interval_ms: 5,
        min_fsync_interval_ms: 0,
        flush_target_bytes: 32 * 1024 * 1024,
        force_recovery_scan: false,
    };

    let k = Keratin::open(&root, cfg).await.unwrap();
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
    k.shutdown().await.unwrap();

    let k = Keratin::open(&root, cfg).await.unwrap();
    let reader = k.reader();

    let started = Instant::now();
    let mut offset = 0u64;
    let mut scanned = 0usize;
    while scanned < messages {
        let records = reader.scan_from(offset, page).unwrap();
        if records.is_empty() {
            break;
        }
        scanned += records.len();
        offset = records.last().unwrap().offset + 1;
    }
    let scan_elapsed = started.elapsed().as_secs_f64();

    let started = Instant::now();
    let mut fetched = 0usize;
    for idx in 0..fetches {
        let offset = ((idx * fetch_stride) % messages) as u64;
        if reader.fetch(offset).unwrap().is_some() {
            fetched += 1;
        }
    }
    let fetch_elapsed = started.elapsed().as_secs_f64();

    k.shutdown().await.unwrap();

    let segment_count = std::fs::read_dir(root.join("segments"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "log"))
        .count();

    println!("root={}", root.display());
    println!("messages={messages}");
    println!("payload_len={payload_len}");
    println!("segment_mb={segment_mb}");
    println!("segments={segment_count}");
    println!("batch={batch}");
    println!("page={page}");
    println!("write_ms={write_ms:.3}");
    println!("scanned={scanned}");
    println!("scan_elapsed_secs={scan_elapsed:.3}");
    println!("scan_msgs_per_sec={:.0}", scanned as f64 / scan_elapsed);
    println!("fetches={fetches}");
    println!("fetched={fetched}");
    println!("fetch_elapsed_secs={fetch_elapsed:.3}");
    println!("fetches_per_sec={:.0}", fetched as f64 / fetch_elapsed);

    if !keep {
        let _ = std::fs::remove_dir_all(&root);
    }
}

fn has_flag(name: &str) -> bool {
    std::env::args().any(|arg| arg == name)
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
