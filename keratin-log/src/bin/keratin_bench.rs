use keratin_log::*;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

fn main() {
    let temp_dir = util::test_dir("keratin-bench");

    let cfg = KeratinConfig {
        segment_max_bytes: 256 * 1024 * 1024,
        index_stride_bytes: 64 * 1024,
        max_batch_bytes: 8 * 1024 * 1024,
        max_batch_records: 4096,
        batch_linger_ms: 20,
        default_durability: Durability::AfterFsync,
        fsync_interval_ms: 20,
        flush_target_bytes: 32 * 1024 * 1024,
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let k = Arc::new(Keratin::open(&temp_dir.root, cfg).await.unwrap());

        let start = Instant::now();
        let total = Arc::new(AtomicU64::new(0));
        let batch_size = 8192;

        let mut handles = vec![];
        for _ in 0..8 {
            let total = total.clone();
            let k = k.clone();
            let handle = tokio::spawn(async move {
                for _ in 0..100 {
                    let mut batch = Vec::new();
                    for _ in 0..batch_size {
                        let msg = Message {
                            flags: 2,
                            headers: vec![],
                            payload: vec![0u8; 1024],
                        };
                        batch.push(msg);
                    }
                    let batch_len = batch.len() as u64;
                    k.append_batch(batch, None).await.unwrap();
                    total.fetch_add(batch_len, Ordering::SeqCst);
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
            (total.load(Ordering::Relaxed) as f64 / secs) as u64
        );
    });
}
