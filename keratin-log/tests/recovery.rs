use keratin_log::*;
use std::fs::OpenOptions;

#[tokio::test]
async fn wal_crash_recovery_truncated_tail() {
    let dir = util::test_dir("wal_recovery");
    let cfg = KeratinConfig::test_default();

    {
        let k = Keratin::open(&dir.root, cfg).await.unwrap();
        let mut to_send = vec![];
        for i in 0..5000 {
            to_send.push(Message {
                payload: format!("v{i}").into_bytes(),
                flags: 0,
                headers: vec![],
            });
        }
        k.append_batch(to_send, None).await.unwrap();
    }

    // Corrupt tail
    let seg = util::latest_segment(&dir.root).unwrap();
    let f = OpenOptions::new().write(true).open(seg).unwrap();
    let len = f.metadata().unwrap().len();
    f.set_len(len - 17).unwrap(); // mid-record truncate

    let k = Keratin::open(&dir.root, cfg).await.unwrap();
    let msgs = k.reader().scan_from(0, 10_000).unwrap();

    // Must be monotonic and gap-free
    for (i, r) in msgs.iter().enumerate() {
        assert_eq!(r.offset as usize, i);
    }
}

#[tokio::test]
async fn wal_truncates_partial_tail() {
    let dir = util::test_dir("wal_truncate");

    let k = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();

    let mut batch = vec![];
    for i in 0..1000 {
        batch.push(Message {
            flags: 0,
            headers: vec![],
            payload: vec![i as u8; 32],
        });
    }
    k.append_batch(batch, None).await.unwrap();

    // smash tail
    let seg = util::latest_segment(&dir.root).unwrap();
    use std::fs::OpenOptions;
    let f = OpenOptions::new().write(true).open(seg).unwrap();
    f.set_len(f.metadata().unwrap().len() - 17).unwrap();

    drop(k);

    let k = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    let got = k.reader().scan_from(0, 10_000).unwrap();

    assert!(got.len() < 1000);
    for (i, r) in got.iter().enumerate() {
        assert_eq!(r.offset as usize, i);
    }
}

#[tokio::test]
async fn wal_recovery_continuity() {
    let dir = util::test_dir("wal_recovery_continuity");

    {
        let k = Keratin::open(&dir.root, KeratinConfig::test_default())
            .await
            .unwrap();

        // Phase 1
        let mut batch = vec![];
        for i in 0..5000 {
            batch.push(Message {
                flags: 0,
                headers: vec![],
                payload: format!("a-{i}").into_bytes(),
            });
        }
        k.append_batch(batch, None).await.unwrap();

        // Phase 2 (partial tail)
        let mut batch = vec![];
        for i in 0..3000 {
            batch.push(Message {
                flags: 0,
                headers: vec![],
                payload: format!("b-{i}").into_bytes(),
            });
        }
        k.append_batch(batch, None).await.unwrap();
    } // crash

    let k = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();

    // Phase 3
    let mut batch = vec![];
    for i in 0..2000 {
        batch.push(Message {
            flags: 0,
            headers: vec![],
            payload: format!("c-{i}").into_bytes(),
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
async fn wal_durability_fence() {
    let dir = util::test_dir("wal_durability_fence");

    {
        let k = Keratin::open(&dir.root, KeratinConfig::test_default())
            .await
            .unwrap();

        // A – weak durability
        let mut a = vec![];
        for i in 0..2000 {
            a.push(Message {
                flags: 0,
                headers: vec![],
                payload: format!("a-{i}").into_bytes(),
            });
        }
        k.append_batch(a, Some(Durability::AfterWrite))
            .await
            .unwrap();

        // B – durability fence
        let mut b = vec![];
        for i in 0..2000 {
            b.push(Message {
                flags: 0,
                headers: vec![],
                payload: format!("b-{i}").into_bytes(),
            });
        }
        k.append_batch(b, Some(Durability::AfterFsync))
            .await
            .unwrap();

        // C – weak tail
        let mut c = vec![];
        for i in 0..2000 {
            c.push(Message {
                flags: 0,
                headers: vec![],
                payload: format!("c-{i}").into_bytes(),
            });
        }
        k.append_batch(c, Some(Durability::AfterWrite))
            .await
            .unwrap();
    } // crash immediately

    let k = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    let got = k.reader().scan_from(0, 20_000).unwrap();

    // A and B must exist, C must not
    assert_eq!(got.len(), 4000);
    for (i, r) in got.iter().enumerate() {
        assert_eq!(r.offset as usize, i);
    }
}
