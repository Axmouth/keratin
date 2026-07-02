use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;

use keratin_log::{
    AppendResult, CompletionPair, IoError, KDurability, Keratin, KeratinAppendCompletion,
    KeratinConfig, Message,
};

#[tokio::main]
async fn main() {
    let mode = arg_value("--mode").unwrap_or_else(|| "batch".to_string());
    let messages = arg_value("--messages")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1_000_000);
    let producers = arg_value("--producers")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(8);
    let batch = arg_value("--batch")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(8192);
    let confirm_window = arg_value("--confirm-window")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(8192);
    let payload_len = arg_value("--payload")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1024);
    let max_batch_records = arg_value("--max-batch-records")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(4096);
    let max_batch_mb = arg_value("--max-batch-mb")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(8);
    let segment_mb = arg_value("--segment-mb")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(256);
    let fsync_ms = arg_value("--fsync-ms")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(20);
    let linger_ms = arg_value("--linger-ms")
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(fsync_ms);
    let flush_mb = arg_value("--flush-mb")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(32);
    let durability = parse_durability(
        arg_value("--durability")
            .unwrap_or_else(|| "fsync".to_string())
            .as_str(),
    );

    let root: PathBuf = arg_value("--root").map(Into::into).unwrap_or_else(|| {
        std::env::temp_dir().join(format!("keratin-throughput-bench-{}", std::process::id()))
    });
    let keep = has_flag("--keep");
    let _ = std::fs::remove_dir_all(&root);

    let cfg = KeratinConfig {
        segment_max_bytes: segment_mb * 1024 * 1024,
        index_stride_bytes: 64 * 1024,
        max_batch_bytes: max_batch_mb * 1024 * 1024,
        max_batch_records,
        batch_linger_ms: linger_ms,
        default_durability: durability,
        fsync_interval_ms: fsync_ms,
        min_fsync_interval_ms: 0,
        flush_target_bytes: flush_mb * 1024 * 1024,
        force_recovery_scan: has_flag("--force-recovery-scan"),
    };

    let k = Arc::new(Keratin::open(&root, cfg).await.unwrap());
    let payload = Arc::new(vec![9u8; payload_len]);
    let total = Arc::new(AtomicU64::new(0));
    let started = Instant::now();

    let mut handles = Vec::with_capacity(producers);
    for producer_idx in 0..producers {
        let k = k.clone();
        let payload = payload.clone();
        let total = total.clone();
        let producer_messages = messages_for_producer(messages, producers, producer_idx);
        let mode = mode.clone();
        let handle = tokio::spawn(async move {
            match mode.as_str() {
                "enqueue" => {
                    run_enqueue(k, payload, producer_messages, confirm_window).await;
                }
                "enqueue-boxed" => {
                    run_enqueue_boxed(k, payload, producer_messages, confirm_window).await;
                }
                "batch" => {
                    run_batches(k, payload, producer_messages, batch).await;
                }
                other => panic!("unsupported mode: {other}"),
            }
            total.fetch_add(producer_messages as u64, Ordering::Relaxed);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = started.elapsed().as_secs_f64();
    k.shutdown().await.unwrap();

    let written = total.load(Ordering::Relaxed);
    println!("mode={mode}");
    println!("root={}", root.display());
    println!("messages={messages}");
    println!("written={written}");
    println!("producers={producers}");
    println!("payload_len={payload_len}");
    println!("batch={batch}");
    println!("confirm_window={confirm_window}");
    println!("max_batch_records={max_batch_records}");
    println!("max_batch_mb={max_batch_mb}");
    println!("fsync_ms={fsync_ms}");
    println!("linger_ms={linger_ms}");
    println!("flush_mb={flush_mb}");
    println!("durability={durability:?}");
    println!("force_recovery_scan={}", cfg.force_recovery_scan);
    println!("elapsed_secs={elapsed:.3}");
    println!("msgs_per_sec={:.0}", written as f64 / elapsed);

    if !keep {
        let _ = std::fs::remove_dir_all(&root);
    }
}

async fn run_batches(k: Arc<Keratin>, payload: Arc<Vec<u8>>, messages: usize, batch: usize) {
    let mut written = 0usize;
    while written < messages {
        let take = batch.min(messages - written);
        let records = make_records(&payload, take);
        k.append_batch(records, None).await.unwrap();
        written += take;
    }
}

async fn run_enqueue(
    k: Arc<Keratin>,
    payload: Arc<Vec<u8>>,
    messages: usize,
    confirm_window: usize,
) {
    let mut receipts = Vec::with_capacity(confirm_window.max(1));
    for _ in 0..messages {
        let rx = k
            .append_enqueue_receiver(
                Message {
                    flags: 0,
                    headers: Vec::new(),
                    payload: payload.as_ref().clone(),
                },
                None,
            )
            .unwrap();
        receipts.push(rx);
        if receipts.len() >= confirm_window {
            drain_receipts(&mut receipts).await;
        }
    }
    drain_receipts(&mut receipts).await;
}

async fn run_enqueue_boxed(
    k: Arc<Keratin>,
    payload: Arc<Vec<u8>>,
    messages: usize,
    confirm_window: usize,
) {
    let mut receipts = Vec::with_capacity(confirm_window.max(1));
    for _ in 0..messages {
        let (completion, rx) = KeratinAppendCompletion::pair();
        k.append_enqueue(
            Message {
                flags: 0,
                headers: Vec::new(),
                payload: payload.as_ref().clone(),
            },
            None,
            completion,
        )
        .unwrap();
        receipts.push(rx);
        if receipts.len() >= confirm_window {
            drain_receipts(&mut receipts).await;
        }
    }
    drain_receipts(&mut receipts).await;
}

async fn drain_receipts(
    receipts: &mut Vec<tokio::sync::oneshot::Receiver<Result<AppendResult, IoError>>>,
) {
    for rx in receipts.drain(..) {
        rx.await.unwrap().unwrap();
    }
}

fn make_records(payload: &[u8], len: usize) -> Vec<Message> {
    let mut records = Vec::with_capacity(len);
    for _ in 0..len {
        records.push(Message {
            flags: 0,
            headers: Vec::new(),
            payload: payload.to_vec(),
        });
    }
    records
}

fn messages_for_producer(total: usize, producers: usize, producer_idx: usize) -> usize {
    let base = total / producers;
    let rem = total % producers;
    base + usize::from(producer_idx < rem)
}

fn parse_durability(value: &str) -> KDurability {
    match value {
        "write" | "after-write" => KDurability::AfterWrite,
        "fsync" | "after-fsync" => KDurability::AfterFsync,
        other => panic!("unsupported durability: {other}"),
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
