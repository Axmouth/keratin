use keratin_log::*;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

fn manifest_clean_flag(root: &Path) -> bool {
    let bytes = std::fs::read(root.join("manifest.bin")).unwrap();
    let flags = u16::from_be_bytes(bytes[10..12].try_into().unwrap());
    flags & 0x0001 != 0
}

fn force_scan_config() -> KeratinConfig {
    KeratinConfig {
        force_recovery_scan: true,
        ..KeratinConfig::test_default()
    }
}

fn message(payload: impl Into<Vec<u8>>) -> Message {
    Message {
        payload: payload.into(),
        flags: 0,
        headers: vec![],
    }
}

fn assert_contiguous_offsets(records: &[OwnedRecord]) {
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.offset as usize, i);
    }
}

#[tokio::test]
async fn clean_shutdown_manifest_lifecycle() {
    let dir = test_dir!("clean_shutdown_manifest_lifecycle");
    let cfg = KeratinConfig::test_default();

    let k = Keratin::open(&dir.root, cfg).await.unwrap();
    assert!(!manifest_clean_flag(&dir.root));

    k.append_batch(
        vec![Message {
            payload: b"one".to_vec(),
            flags: 0,
            headers: vec![],
        }],
        None,
    )
    .await
    .unwrap();
    k.shutdown().await.unwrap();
    assert!(manifest_clean_flag(&dir.root));

    let k = Keratin::open(&dir.root, cfg).await.unwrap();
    assert!(!manifest_clean_flag(&dir.root));
    let got = k.reader().scan_from(0, 10).unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].payload, b"one");

    k.shutdown().await.unwrap();
    assert!(manifest_clean_flag(&dir.root));
}

#[tokio::test]
async fn wal_crash_recovery_truncated_tail() {
    let dir = test_dir!("wal_recovery");
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

    let k = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    let msgs = k.reader().scan_from(0, 10_000).unwrap();

    // Must be monotonic and gap-free
    assert_contiguous_offsets(&msgs);
}

#[tokio::test]
async fn wal_truncates_partial_tail() {
    let dir = test_dir!("wal_truncate");

    let k = Keratin::open(&dir.root, force_scan_config()).await.unwrap();

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

    let k = Keratin::open(&dir.root, force_scan_config()).await.unwrap();
    let got = k.reader().scan_from(0, 10_000).unwrap();

    assert!(got.len() < 1000);
    assert_contiguous_offsets(&got);
}

#[tokio::test]
async fn forced_recovery_scan_repairs_tail_even_after_clean_shutdown() {
    let dir = test_dir!("forced_recovery_scan_after_clean_shutdown");
    let cfg = KeratinConfig::test_default();

    let k = Keratin::open(&dir.root, cfg).await.unwrap();
    let mut batch = Vec::new();
    for i in 0..100 {
        batch.push(Message {
            flags: 0,
            headers: vec![],
            payload: vec![i as u8; 64],
        });
    }
    k.append_batch(batch, None).await.unwrap();
    k.shutdown().await.unwrap();
    assert!(manifest_clean_flag(&dir.root));

    let seg = util::latest_segment(&dir.root).unwrap();
    let f = OpenOptions::new().write(true).open(seg).unwrap();
    f.set_len(f.metadata().unwrap().len() - 17).unwrap();

    let k = Keratin::open(&dir.root, force_scan_config()).await.unwrap();
    let got = k.reader().scan_from(0, 10_000).unwrap();

    assert!(got.len() < 100);
    assert_contiguous_offsets(&got);
}

#[tokio::test]
async fn recovery_uses_verified_tail_before_next_append() {
    let dir = test_dir!("recovery_verified_tail_next_append");
    let cfg = KeratinConfig::test_default();

    let k = Keratin::open(&dir.root, cfg).await.unwrap();
    let mut batch = Vec::new();
    for i in 0..100 {
        batch.push(message(vec![i as u8; 64]));
    }
    k.append_batch(batch, None).await.unwrap();
    k.shutdown().await.unwrap();

    let seg = util::latest_segment(&dir.root).unwrap();
    let mut f = OpenOptions::new().read(true).write(true).open(seg).unwrap();
    let last = f.metadata().unwrap().len() - 1;
    let mut byte = [0u8; 1];
    f.seek(SeekFrom::Start(last)).unwrap();
    f.read_exact(&mut byte).unwrap();
    f.seek(SeekFrom::Start(last)).unwrap();
    f.write_all(&[byte[0] ^ 0xff]).unwrap();
    f.sync_all().unwrap();

    let k = Keratin::open(&dir.root, force_scan_config()).await.unwrap();
    let repaired = k.reader().scan_from(0, 10_000).unwrap();
    assert_eq!(repaired.len(), 99);
    assert_contiguous_offsets(&repaired);

    let appended = k
        .append(message(b"after-repair".to_vec()), None)
        .await
        .unwrap();
    assert_eq!(appended.base_offset, 99);

    let got = k.reader().scan_from(0, 10_000).unwrap();
    assert_eq!(got.len(), 100);
    assert_contiguous_offsets(&got);
    assert_eq!(got.last().unwrap().payload, b"after-repair");
}

#[tokio::test]
async fn recovery_truncates_garbage_tail_before_next_append() {
    let dir = test_dir!("recovery_garbage_tail_next_append");
    let cfg = KeratinConfig {
        segment_max_bytes: 512,
        force_recovery_scan: true,
        ..KeratinConfig::test_default()
    };

    let k = Keratin::open(&dir.root, cfg).await.unwrap();
    for i in 0..20 {
        k.append(message(vec![i as u8; 128]), None).await.unwrap();
    }
    k.shutdown().await.unwrap();

    let before_segments = util::all_segments(&dir.root);
    assert!(
        before_segments.len() > 1,
        "test setup should cross at least one segment boundary"
    );

    let latest = util::latest_segment(&dir.root).unwrap();
    let mut f = OpenOptions::new().append(true).open(latest).unwrap();
    f.write_all(b"not a valid keratin record").unwrap();
    f.sync_all().unwrap();

    let k = Keratin::open(&dir.root, cfg).await.unwrap();
    let repaired = k.reader().scan_from(0, 100).unwrap();
    assert_eq!(repaired.len(), 20);
    assert_contiguous_offsets(&repaired);

    let appended = k
        .append(message(b"after-garbage".to_vec()), None)
        .await
        .unwrap();
    assert_eq!(appended.base_offset, 20);

    let got = k.reader().scan_from(0, 100).unwrap();
    assert_eq!(got.len(), 21);
    assert_contiguous_offsets(&got);
    assert_eq!(got.last().unwrap().payload, b"after-garbage");
}

#[tokio::test]
async fn wal_recovery_continuity() {
    let dir = test_dir!("wal_recovery_continuity");

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

    assert_contiguous_offsets(&got);
}

#[tokio::test]
async fn wal_durability_fence() {
    let dir = test_dir!("wal_durability_fence");

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
        k.append_batch(a, Some(KDurability::AfterWrite))
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
        k.append_batch(b, Some(KDurability::AfterFsync))
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
        k.append_batch(c, Some(KDurability::AfterWrite))
            .await
            .unwrap();
        k.force_close().await.unwrap();
    } // crash immediately

    let k = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    let got = k.reader().scan_from(0, 20_000).unwrap();

    // A and B must exist, C must not
    assert_eq!(got.len(), 4000);
    assert_contiguous_offsets(&got);
}

#[tokio::test]
async fn sync_makes_after_write_durable() {
    let dir = test_dir!("sync_makes_after_write_durable");
    let cfg = KeratinConfig::test_default();
    let k = Keratin::open(&dir.root, cfg).await.unwrap();

    // AfterWrite stages + writes to the OS but does not fsync, so the durable
    // watermark must not advance yet.
    k.append_batch(
        vec![message("a"), message("b"), message("c")],
        Some(KDurability::AfterWrite),
    )
    .await
    .unwrap();
    let before = k.durable_offset();

    // sync() fsyncs the staged data and advances the durable watermark.
    k.sync().await.unwrap();
    let after = k.durable_offset();
    assert!(
        after > before,
        "sync must advance the durable watermark: {before} -> {after}"
    );

    // And the data survives a reopen.
    k.shutdown().await.unwrap();
    let k = Keratin::open(&dir.root, cfg).await.unwrap();
    let got = k.reader().scan_from(0, 10).unwrap();
    assert_eq!(got.len(), 3);
    k.shutdown().await.unwrap();
}
