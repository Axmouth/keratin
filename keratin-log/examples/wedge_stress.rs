//! Stress repro for a suspected writer <-> fsync-worker bounded-channel deadlock.
//!
//! Hammers AfterFsync appends through the real writer on a real drive so the
//! fsync request channel fills and the writer thread parks in `fsync_tx.send`.
//! If the fsync worker's fused drain ever collects more requests than the done
//! channel has free slots while the writer is parked, both threads park forever
//! and the completion counter printed by the heartbeat stops moving.
//!
//! Run under `taskset` with few cores to widen the preemption window, and watch
//! the heartbeat externally. To capture stacks of a wedged run with a
//! non-ancestor gdb, relax yama first: `sudo sysctl kernel.yama.ptrace_scope=0`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use keratin_log::{KDurability, Keratin, KeratinConfig, Message};

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::main(worker_threads = 4)]
async fn main() -> std::io::Result<()> {
    let dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "wedge_stress_data".to_string());
    let payload_size = env_u64("WEDGE_PAYLOAD", 1024) as usize;
    let workers = env_u64("WEDGE_WORKERS", 256);
    let max_inflight = env_u64("WEDGE_MAX_INFLIGHT", 8) as usize;
    let truncate_window = env_u64("WEDGE_TRUNCATE_WINDOW", 2_000_000);

    let cfg = KeratinConfig {
        max_inflight_fsyncs: max_inflight,
        ..KeratinConfig::default()
    };

    println!(
        "wedge_stress pid={} dir={} payload={} workers={} max_inflight={}",
        std::process::id(),
        dir,
        payload_size,
        workers,
        max_inflight
    );

    let keratin = Arc::new(Keratin::open(&dir, cfg).await?);
    let completed = Arc::new(AtomicU64::new(0));
    let max_offset = Arc::new(AtomicU64::new(0));

    // Heartbeat on a dedicated OS thread so it keeps printing even when every
    // tokio worker is blocked behind a wedged writer.
    {
        let completed = completed.clone();
        std::thread::spawn(move || {
            let start = Instant::now();
            loop {
                std::thread::sleep(Duration::from_secs(1));
                println!(
                    "hb {} completed={}",
                    start.elapsed().as_secs(),
                    completed.load(Ordering::Relaxed)
                );
            }
        });
    }

    // Head truncation keeps disk use bounded and mirrors the truncate traffic
    // the field wedge ran under (TTL expiry storms).
    {
        let keratin = keratin.clone();
        let max_offset = max_offset.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let tail = max_offset.load(Ordering::Relaxed);
                if tail > truncate_window {
                    let _ = keratin.truncate_before(tail - truncate_window).await;
                }
            }
        });
    }

    let mut handles = Vec::new();
    for _ in 0..workers {
        let keratin = keratin.clone();
        let completed = completed.clone();
        let max_offset = max_offset.clone();
        let payload = vec![0xabu8; payload_size];
        handles.push(tokio::spawn(async move {
            loop {
                let msg = Message {
                    flags: 0,
                    headers: Vec::new(),
                    payload: payload.clone(),
                };
                match keratin.append(msg, Some(KDurability::AfterFsync)).await {
                    Ok(ar) => {
                        completed.fetch_add(1, Ordering::Relaxed);
                        max_offset.fetch_max(ar.base_offset, Ordering::Relaxed);
                    }
                    Err(e) => {
                        eprintln!("append error: {e}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }
    Ok(())
}
