use keratin_log::*;

#[tokio::test]
async fn byte_budget_matches_full_scan_prefix_on_cache_and_file_paths() {
    for cache_bytes in [0, 16 * 1024 * 1024] {
        let dir = test_dir!("read_byte_budget");
        let log = Keratin::open(
            &dir.root,
            KeratinConfig {
                tail_cache_bytes: cache_bytes,
                segment_max_bytes: 512,
                ..KeratinConfig::test_default()
            },
        )
        .await
        .unwrap();
        for i in 0..24 {
            log.append_batch(
                vec![Message {
                    flags: 0,
                    headers: vec![i],
                    payload: vec![i; [16, 80, 4096][i as usize % 3]],
                }],
                Some(KDurability::AfterFsync),
            )
            .await
            .unwrap();
        }
        let reader = log.reader();
        for from in [0, 3, 23, 24] {
            for max in [0, 1, 2, 100] {
                let full = reader.scan_from(from, max).unwrap();
                for budget in [0usize, 1, 49, 128, 1024, usize::MAX] {
                    let mut expected = Vec::new();
                    let mut used = 0usize;
                    for r in &full {
                        let bytes = 32 + r.headers.len() + r.payload.len();
                        let over = !expected.is_empty() && used.saturating_add(bytes) > budget;
                        expected.push(r.offset);
                        used = used.saturating_add(bytes);
                        if over {
                            break;
                        }
                    }
                    let got = reader
                        .scan_from_with_byte_budget(from, max, budget)
                        .unwrap();
                    assert_eq!(
                        got.iter().map(|r| r.offset).collect::<Vec<_>>(),
                        expected,
                        "cache={cache_bytes}, from={from}, max={max}, budget={budget}"
                    );
                    for (got, expected) in got.iter().zip(&full) {
                        assert_eq!(got.headers, expected.headers);
                        assert_eq!(got.payload, expected.payload);
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn ten_mib_record_survives_an_eight_mib_scan_budget() {
    for cache_bytes in [0, 32 * 1024 * 1024] {
        let dir = test_dir!("oversized_read_budget");
        let log = Keratin::open(
            &dir.root,
            KeratinConfig {
                tail_cache_bytes: cache_bytes,
                ..KeratinConfig::test_default()
            },
        )
        .await
        .unwrap();
        log.append_batch(
            vec![
                Message {
                    flags: 0,
                    headers: vec![],
                    payload: vec![7; 10 * 1024 * 1024],
                },
                Message {
                    flags: 0,
                    headers: vec![],
                    payload: vec![8; 100],
                },
                Message {
                    flags: 0,
                    headers: vec![],
                    payload: vec![9; 100],
                },
            ],
            Some(KDurability::AfterFsync),
        )
        .await
        .unwrap();
        let got = log
            .reader()
            .scan_from_with_byte_budget(0, 10, 8 * 1024 * 1024)
            .unwrap();
        assert_eq!(
            got.len(),
            2,
            "oversized first record plus boundary lookahead"
        );
        assert_eq!(got[0].payload, vec![7; 10 * 1024 * 1024]);
        assert_eq!(got[1].offset, 1);
    }
}
