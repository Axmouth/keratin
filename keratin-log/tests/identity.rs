use std::sync::Arc;

use keratin_log::*;

#[tokio::test]
async fn open_fails_when_already_open() {
    let dir = test_dir!("keratin_lock");

    let cfg = KeratinConfig::test_default();

    // First open should succeed
    let k1 = Keratin::open(&dir.root, cfg)
        .await
        .expect("first open should succeed");

    // Second open on same directory should fail
    let k2 = Keratin::open(&dir.root, cfg).await;

    dbg!(&k2);

    assert!(k2.is_err(), "second open on same root must fail");

    // Drop first instance
    drop(k1);

    // Now it should succeed again
    let k3 = Keratin::open(&dir.root, cfg)
        .await
        .expect("open after drop should succeed");

    drop(k3);
}

#[tokio::test]
async fn wal_append_scan_identity() {
    let dir = test_dir!("wal_identity");

    let cfg = KeratinConfig::test_default();
    let k = Keratin::open(&dir.root, cfg).await.unwrap();

    let mut sent = Vec::new();

    for _ in 0..5 {
        let mut to_send = vec![];
        for i in 0..2_000 {
            let payload = format!("msg-{i}").into_bytes();
            let m = Message {
                flags: 0,
                headers: vec![],
                payload: payload.clone(),
            };

            to_send.push(m);
            sent.push(payload);
        }
        k.append_batch(to_send, None).await.unwrap();
    }

    let reader = k.reader();
    let got = reader.scan_from(0, 20_000).unwrap();

    assert_eq!(got.len(), sent.len());
    for (i, r) in got.iter().enumerate() {
        assert_eq!(r.offset as usize, i);
        assert_eq!(r.payload, sent[i]);
    }
}

#[tokio::test]
async fn wal_recovery_identity() {
    let dir = test_dir!("wal_recovery");

    {
        let k = Keratin::open(&dir.root, KeratinConfig::test_default())
            .await
            .unwrap();

        for i in 0..10 {
            let mut batch = vec![];
            for j in 0..500 {
                batch.push(Message {
                    flags: 0,
                    headers: vec![],
                    payload: format!("msg-{}-{}", i, j).into_bytes(),
                });
            }
            k.append_batch(batch, None).await.unwrap();
        }
    } // drop without clean shutdown

    let k = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    let got = k.reader().scan_from(0, 100_000).unwrap();

    assert_eq!(got.len(), 5000);
    for (i, r) in got.iter().enumerate() {
        assert_eq!(r.offset as usize, i);
    }
}

#[tokio::test]
async fn scan_past_end_must_not_hang() {
    let dir = test_dir!("scan_past_end");

    let k = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();

    let mut batch = vec![];
    for i in 0..100 {
        batch.push(Message {
            flags: 0,
            headers: vec![],
            payload: format!("msg-{i}").into_bytes(),
        });
    }
    k.append_batch(batch, None).await.unwrap();

    // Ask for more than exist
    let got = k.reader().scan_from(0, 10_000).unwrap();
    assert_eq!(got.len(), 100);
}

#[tokio::test]
async fn wal_multi_producer_ordering() {
    let dir = test_dir!("wal_multi");

    let k = Arc::new(
        Keratin::open(&dir.root, KeratinConfig::test_default())
            .await
            .unwrap(),
    );

    let mut handles = vec![];
    for p in 0..4 {
        let k = k.clone();
        handles.push(tokio::spawn(async move {
            let mut batch = vec![];
            for i in 0..1000 {
                batch.push(Message {
                    flags: 0,
                    headers: vec![],
                    payload: format!("p{}-{}", p, i).into_bytes(),
                });
            }
            k.append_batch(batch, None).await.unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let got = k.reader().scan_from(0, 10_000).unwrap();
    assert_eq!(got.len(), 4000);

    for (i, r) in got.iter().enumerate() {
        assert_eq!(r.offset as usize, i);
    }
}

#[tokio::test]
async fn wal_segment_roll_continuity() {
    let dir = test_dir!("wal_roll");

    let mut cfg = KeratinConfig::test_default();
    cfg.segment_max_bytes = 64 * 1024; // force many segments

    let k = Keratin::open(&dir.root, cfg).await.unwrap();

    let mut batch = vec![];
    for i in 0..10_000 {
        batch.push(Message {
            flags: 0,
            headers: vec![],
            payload: vec![i as u8; 128],
        });
    }
    k.append_batch(batch, None).await.unwrap();

    let got = k.reader().scan_from(0, 20_000).unwrap();
    assert_eq!(got.len(), 10_000);

    for (i, r) in got.iter().enumerate() {
        assert_eq!(r.offset as usize, i);
    }
}

#[tokio::test]
async fn wal_high_contention_storm() {
    let dir = test_dir!("wal_storm");

    let k = Arc::new(
        Keratin::open(&dir.root, KeratinConfig::test_default())
            .await
            .unwrap(),
    );

    let mut handles = vec![];
    for p in 0..8 {
        let k = k.clone();
        handles.push(tokio::spawn(async move {
            let mut batch = vec![];
            for i in 0..5000 {
                batch.push(Message {
                    flags: 0,
                    headers: vec![],
                    payload: format!("p{p}-{i}").into_bytes(),
                });
            }
            k.append_batch(batch, Some(KDurability::AfterFsync))
                .await
                .unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let got = k.reader().scan_from(0, 100_000).unwrap();
    assert_eq!(got.len(), 40_000);

    for (i, r) in got.iter().enumerate() {
        assert_eq!(r.offset as usize, i);
    }
}
