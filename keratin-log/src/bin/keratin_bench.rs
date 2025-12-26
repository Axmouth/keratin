use keratin_log::*;
use std::{sync::{Arc, atomic::AtomicU64}, time::Instant};

fn main() {
    let temp_dir = util::test_dir("keratin-bench");

    let cfg = KeratinConfig {
        segment_max_bytes: 256 * 1024 * 1024,
        index_stride_bytes: 64 * 1024,
        max_batch_bytes: 4 * 1024 * 1024,
        max_batch_records: 2048,
        batch_linger_ms: 2,
        default_durability: Durability::AfterFsync,
        fsync_interval_ms: 2,
        flush_target_bytes: 32 * 1024 * 1024,
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let k = Arc::new(Keratin::open(&temp_dir.root, cfg).await.unwrap());

        let start = Instant::now();
        let total = Arc::new(AtomicU64::new(0));
        let batch_size = 1000;

        let mut handles = vec![];
        for _ in 0..4 {
            let total = total.clone();
            let k = k.clone();
            let handle = tokio::spawn(async move {
                for _ in 0..1000 {
                    let mut batch = Vec::new();
                    for _ in 0..batch_size {
                        let msg = Message { flags: 2, headers: vec![], payload: vec![0u8; 1024] };
                        batch.push(msg);
                    }
                    k.append_batch(batch, None)
                        .await
                        .unwrap();
                    total.fetch_add(batch_size, std::sync::atomic::Ordering::SeqCst);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let secs = start.elapsed().as_secs_f64();
        println!(
            "{} msgs/sec",
            (total.load(std::sync::atomic::Ordering::Relaxed) as f64 / secs) as u64
        );
    });
}
