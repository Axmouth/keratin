use keratin_log::*;

fn main() {
    let temp_dir = test_dir!("keratin-test");

    let cfg = KeratinConfig {
        segment_max_bytes: 8 * 1024 * 1024,
        index_stride_bytes: 64 * 1024,
        max_batch_bytes: 512 * 1024,
        max_batch_records: 1024,
        batch_linger_ms: 1,
        default_durability: KDurability::AfterFsync,
        fsync_interval_ms: 1,
        min_fsync_interval_ms: 0,
        flush_target_bytes: 16 * 1024 * 1024,
        force_recovery_scan: false,
        tail_cache_bytes: 0,
        segment_preallocate_bytes: 0,
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let k = Keratin::open(&temp_dir.root, cfg.clone()).await.unwrap();

        // write 100k messages
        for i in 0..3_000u64 {
            let payload = format!("msg-{i}").into_bytes();
            let msg = Message {
                flags: 2,
                headers: vec![],
                payload,
            };
            k.append_batch(vec![msg], None).await.unwrap();
        }

        let reader = k.reader();

        // verify reads
        for i in 0..3_000u64 {
            let r = reader.fetch(i).unwrap().unwrap();
            assert_eq!(r.payload, format!("msg-{i}").into_bytes());
        }

        println!("PASS: basic write/read");
    });
}
