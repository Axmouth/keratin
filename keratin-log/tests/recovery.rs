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
async fn forced_recovery_appends_contiguously_after_multichunk_records() {
    let dir = test_dir!("recovery_multichunk_append");
    let cfg = force_scan_config();
    let payloads = [vec![1; 1000], vec![2; 180_000], vec![3; 65_530]];
    let log = Keratin::open(&dir.root, cfg).await.unwrap();
    log.append_batch(
        payloads.iter().cloned().map(message).collect(),
        Some(KDurability::AfterFsync),
    )
    .await
    .unwrap();
    log.shutdown().await.unwrap();
    drop(log);
    let log = Keratin::open(&dir.root, cfg).await.unwrap();
    log.append_batch(
        vec![message(b"after-recovery".to_vec())],
        Some(KDurability::AfterFsync),
    )
    .await
    .unwrap();
    log.shutdown().await.unwrap();
    drop(log);
    let log = Keratin::open(&dir.root, cfg).await.unwrap();
    let records = log.reader().scan_from_disk(0, 10).unwrap();
    assert_eq!(records.len(), 4);
    assert_contiguous_offsets(&records);
    for (record, payload) in records.iter().zip(payloads) {
        assert_eq!(record.payload, payload);
    }
    assert_eq!(records[3].payload, b"after-recovery");
    log.shutdown().await.unwrap();
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

    let before_segments = util::all_segments(&dir.root).unwrap();
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

#[tokio::test]
async fn staged_offset_fires_with_assigned_offset_before_durable() {
    let dir = test_dir!("staged_offset_before_durable");
    let k = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();

    let (completion, done_rx) = KeratinAppendCompletion::pair();
    let (staged_tx, staged_rx) = tokio::sync::oneshot::channel();
    k.append_enqueue_staged(
        message("a"),
        Some(KDurability::AfterFsync),
        completion,
        staged_tx,
    )
    .unwrap();

    // The staged signal carries the assigned base offset, and the durable
    // completion later reports the same offset.
    let staged_offset = staged_rx.await.unwrap();
    assert_eq!(staged_offset, 0);
    let durable = done_rx.await.unwrap().unwrap();
    assert_eq!(durable.base_offset, staged_offset);
}

fn prealloc_config() -> KeratinConfig {
    KeratinConfig {
        // Preallocate 1 MiB ahead of the write cursor.
        segment_preallocate_bytes: 1024 * 1024,
        ..KeratinConfig::test_default()
    }
}

/// With preallocation on, records append into the preallocated region, read back
/// correctly, survive a clean reopen (padding trimmed at shutdown), and appending
/// continues afterward.
#[tokio::test]
async fn preallocated_segment_clean_lifecycle() {
    let dir = test_dir!("prealloc_clean");
    let cfg = prealloc_config();
    {
        let k = Keratin::open(&dir.root, cfg).await.unwrap();
        for i in 0..100u32 {
            k.append_batch(vec![message(format!("m{i}"))], None)
                .await
                .unwrap();
        }
        k.sync().await.unwrap();
        let got = k.reader().scan_from(0, 200).unwrap();
        assert_eq!(got.len(), 100);
        assert_eq!(got[0].payload, b"m0");
        assert_eq!(got[99].payload, b"m99");
        k.shutdown().await.unwrap();
    }
    {
        let k = Keratin::open(&dir.root, cfg).await.unwrap();
        let got = k.reader().scan_from(0, 200).unwrap();
        assert_eq!(
            got.len(),
            100,
            "records survive a clean reopen with preallocation"
        );
        assert_eq!(got[50].payload, b"m50");
        k.append_batch(vec![message("m100".to_string())], None)
            .await
            .unwrap();
        k.sync().await.unwrap();
        assert_eq!(k.reader().scan_from(0, 200).unwrap().len(), 101);
        k.shutdown().await.unwrap();
    }
}

/// A crash with preallocation leaves the active segment zero-padded and the
/// manifest dirty. Recovery scans past the padding to the last valid record, so
/// every confirmed record survives.
#[tokio::test]
async fn preallocated_segment_recovers_after_crash() {
    let dir = test_dir!("prealloc_crash");
    let cfg = prealloc_config();
    {
        let k = Keratin::open(&dir.root, cfg).await.unwrap();
        for i in 0..50u32 {
            k.append_batch(vec![message(format!("c{i}"))], None)
                .await
                .unwrap();
        }
        k.sync().await.unwrap(); // durable
        k.force_close().await.unwrap(); // crash: padding remains, manifest dirty
    }
    {
        let k = Keratin::open(&dir.root, cfg).await.unwrap();
        let got = k.reader().scan_from(0, 100).unwrap();
        assert_eq!(
            got.len(),
            50,
            "confirmed records survive a crash with preallocation padding"
        );
        assert_eq!(got[49].payload, b"c49");
        k.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn accepted_history_reopen_preserves_padding_and_rejects_damage_without_mutation() {
    for damage in [0, 1, 2, 3] {
        let dir = test_dir!("bound_recovery_preserves_bytes");
        let cfg = KeratinConfig {
            segment_preallocate_bytes: if damage == 0 { 1024 * 1024 } else { 0 },
            ..KeratinConfig::test_default()
        };
        let log = Keratin::open(&dir.root, cfg).await.unwrap();
        log.append_batch(
            vec![message(vec![6; 180_000])],
            Some(KDurability::AfterFsync),
        )
        .await
        .unwrap();
        log.shutdown().await.unwrap();
        drop(log);
        let segment = util::latest_segment(&dir.root).unwrap();
        let mut bytes = std::fs::read(&segment).unwrap();
        match damage {
            0 => {}
            1 => {
                bytes.truncate(bytes.len() - 8);
            }
            2 => {
                let last = bytes.len() - 1;
                bytes[last] ^= 1;
            }
            _ => bytes.extend_from_slice(b"partial write"),
        }
        std::fs::write(&segment, &bytes).unwrap();
        let manifest = std::fs::read(dir.root.join("manifest.bin")).unwrap();
        let opened = Keratin::open_preserving_history(&dir.root, cfg).await;
        if damage == 0 {
            let log = opened.unwrap();
            assert_eq!(log.next_offset(), 1);
            log.append_batch(
                vec![message(b"next".to_vec())],
                Some(KDurability::AfterFsync),
            )
            .await
            .unwrap();
            log.shutdown().await.unwrap();
            drop(log);
            let log = Keratin::open_preserving_history(&dir.root, cfg)
                .await
                .unwrap();
            assert_eq!(log.reader().scan_from_disk(0, 8).unwrap().len(), 2);
            log.shutdown().await.unwrap();
        } else {
            assert!(opened.is_err(), "damage {damage}");
            assert_eq!(std::fs::read(&segment).unwrap(), bytes);
            assert_eq!(
                std::fs::read(dir.root.join("manifest.bin")).unwrap(),
                manifest
            );
        }
    }
}

#[tokio::test]
async fn strict_live_scan_requires_frozen_role_and_verifies_disk_beyond_manifest_tail() {
    let dir = test_dir!("strict_live_reader");
    let log = Keratin::open(&dir.root, KeratinConfig::test_default()).await.unwrap();
    assert!(log.frozen_reader().is_err());
    let payload = b"strict-live-reader-payload".to_vec();
    log.append_batch(vec![message(payload.clone())], Some(KDurability::AfterFsync)).await.unwrap();
    assert!(log.frozen_reader().is_err());
    log.freeze();
    let mut seen = vec![];
    log.frozen_reader().unwrap().scan(|r| { seen.push((r.offset,r.payload.to_vec())); Ok(()) }).unwrap();
    assert_eq!(seen, vec![(0,payload.clone())]);
    // The cold sealed-reader API still requires an exact persisted boundary.
    assert!(FrozenLogReader::open(&dir.root, log.current_epoch(), 0, 2).is_err());
    let segment = dir.root.join("segments/00000000000000000000.log");
    let bytes = std::fs::read(&segment).unwrap();
    let at = bytes.windows(payload.len()).position(|w| w == payload).unwrap();
    let mut file = OpenOptions::new().write(true).open(segment).unwrap();
    file.seek(SeekFrom::Start(at as u64)).unwrap();
    file.write_all(&[payload[0] ^ 1]).unwrap();
    file.sync_all().unwrap();
    assert!(log.frozen_reader().unwrap().scan(|_| Ok(())).is_err());
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn strict_frozen_scan_handles_varying_records_across_read_buffer_boundaries() {
    let dir = test_dir!("strict_frozen_buffer_boundaries");
    let log = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    let messages: Vec<_> = [3, 100_000, 1, 70_003, 65_530, 7]
        .into_iter()
        .enumerate()
        .map(|(i, size)| Message {
            payload: vec![i as u8 + 1; size],
            headers: vec![i as u8 + 10; i * 7],
            flags: 0,
        })
        .collect();
    log.append_batch(messages.clone(), Some(KDurability::AfterFsync))
        .await
        .unwrap();
    log.freeze();
    let mut count = 0;
    log.frozen_reader().unwrap().scan(|record| {
        assert_eq!(record.offset, count as u64);
        assert_eq!(record.headers, messages[count].headers);
        assert_eq!(record.payload, messages[count].payload);
        count += 1;
        Ok(())
    }).unwrap();
    assert_eq!(count, messages.len());
    // Corrupt a later record after a successful scan. A new buffered scan must
    // still read the changed bytes and reject their checksum.
    let segment = dir.root.join("segments/00000000000000000000.log");
    let bytes = std::fs::read(&segment).unwrap();
    let payload = &messages[3].payload;
    let at = bytes.windows(payload.len()).position(|w| w == payload).unwrap();
    let mut file = OpenOptions::new().write(true).open(segment).unwrap();
    file.seek(SeekFrom::Start(at as u64)).unwrap();
    file.write_all(&[payload[0] ^ 1]).unwrap();
    file.sync_all().unwrap();
    assert!(log.frozen_reader().unwrap().scan(|_| Ok(())).is_err());
    log.shutdown().await.unwrap();
}
