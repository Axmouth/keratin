use keratin_log::{util::init_tracing, *};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

fn main() {
    init_tracing();
    let temp_dir = util::test_dir("keratin-bench");

    let cfg = KeratinConfig {
        segment_max_bytes: 256 * 1024 * 1024,
        index_stride_bytes: 64 * 1024,
        max_batch_bytes: 8 * 1024 * 1024,
        max_batch_records: 8192,
        batch_linger_ms: 25,
        fsync_interval_ms: 25,
        flush_target_bytes: 32 * 1024 * 1024,
        default_durability: KDurability::AfterFsync,
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let k = Arc::new(Keratin::open(&temp_dir.root, cfg).await.unwrap());

        let start = Instant::now();
        let total = Arc::new(AtomicU64::new(0));
        let tasks = 1;
        let batch_size = 8192 * 8;

        let mut handles = vec![];
        for _ in 0..tasks {
            let total = total.clone();
            let k = k.clone();
            let handle = tokio::spawn(async move {
                let mut receipts = vec![];
                for _ in 0..100 {
                    for _ in 0..batch_size {
                        let msg = Message {
                            flags: 2,
                            headers: vec![],
                            payload: vec![0u8; 1024],
                        };
                        let (completion, rx) = KeratinAppendCompletion::pair();
                        k.append_enqueue(msg, None, completion).unwrap();
                        receipts.push(rx);
                        total.fetch_add(1, Ordering::SeqCst);
                    }
                    // let target = i * cfg.segment_max_bytes / 1024;
                    // let r = k.truncate_before(i * cfg.segment_max_bytes / 1024).await.unwrap();
                    // tracing::info!("Removed up to {}, target {}", r, target);
                }
                receipts
            });
            handles.push(handle);
        }

        let mut receipts = vec![];
        for handle in handles {
            let mut receipts_partial = handle.await.unwrap();
            receipts.append(&mut receipts_partial);
        }

        for rx in receipts {
            rx.await.unwrap().unwrap();
        }

        let secs = start.elapsed().as_secs_f64();
        println!(
            "{} msgs/sec",
            (total.load(Ordering::Relaxed) as f64 / secs) as u64
        );
    });
}
