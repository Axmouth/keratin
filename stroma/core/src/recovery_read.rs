//! Bounded pages from sealed storage, verified without opening ordinary writers.
use super::*;
use keratin_log::{FrozenLogReader, lock_existing_log};
use std::io::Read;

const MAX_PAGE_BYTES: u32 = 16 * 1024 * 1024;
const MAX_PAGE_RECORDS: u32 = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoveryReadSource {
    Messages,
    Events,
    Snapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryReadRequest {
    pub seal: RecoverySealRequest,
    pub history_id: [u8; 32],
    pub source: RecoveryReadSource,
    /// Record offset for logs; byte offset for the raw snapshot envelope.
    pub from: u64,
    pub max_records: u32,
    /// Log budget includes offset/flags/length prefixes (18 bytes per record).
    pub max_bytes: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryRecord {
    pub offset: u64,
    pub flags: u16,
    pub headers: Vec<u8>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryReadPage {
    pub history_id: [u8; 32],
    pub source: RecoveryReadSource,
    pub from: u64,
    pub next: u64,
    pub end: u64,
    pub records: Vec<RecoveryRecord>,
    pub snapshot_bytes: Vec<u8>,
}

fn invalid(message: &str) -> StromaError {
    StromaError::InvalidArgument(message.into())
}
fn corrupt(message: &str) -> StromaError {
    StromaError::Corruption(message.into())
}

impl Stroma {
    /// Explicit read-only recovery access. A completed durable seal and receipt
    /// must already exist. Full retained contents are scanned for every page;
    /// output/memory are bounded, total scan I/O is proportional to retained data.
    pub async fn read_sealed_replica(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        expected_kind: PartitionKind,
        request: RecoveryReadRequest,
    ) -> Result<RecoveryReadPage> {
        if !cfg!(unix) {
            return Err(StromaError::Unsupported(
                "sealed recovery reads require Unix log locking".into(),
            ));
        }
        if request.max_bytes == 0
            || request.max_bytes > MAX_PAGE_BYTES
            || request.max_records == 0
            || request.max_records > MAX_PAGE_RECORDS
        {
            return Err(invalid(
                "recovery page budget must be 1..16 MiB and 1..4096 records",
            ));
        }
        let permit = self
            .recovery_read_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| invalid("another sealed recovery read is in progress; retry later"))?;
        let stroma = self.clone();
        let topic = topic.to_owned();
        let group = normalize_group(group).map(str::to_owned);
        // Owned work retains lifecycle and admission guards through blocking I/O,
        // even when the connection/request future disappears.
        tokio::spawn(async move {
            let lifecycle = stroma
                .lock_partition_lifecycle(&topic, part, group.as_deref())
                .await;
            let handle = {
                let registry = stroma.queue_handles.load();
                slot_lookup_no_alloc(&registry, &topic, part, group.as_deref())
                    .and_then(|slot| slot.handle.get().cloned())
            };
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let _lifecycle = lifecycle;
                let _snapshot_io = handle.as_ref().map(|h| h.recovery_gate.snapshot_io.lock());
                stroma.require_matching_recovery_seal(
                    &topic,
                    part,
                    group.as_deref(),
                    &request.seal,
                )?;
                stroma.ensure_checkpoint_not_pending(&topic, part, group.as_deref())?;
                let actual = stroma.read_partition_kind(&topic, part, group.as_deref());
                if actual != expected_kind {
                    return Err(StromaError::WrongPartitionKind {
                        expected: expected_kind,
                        actual,
                    });
                }
                let history = stroma.require_retained_history(
                    &topic,
                    part,
                    group.as_deref(),
                    expected_kind == PartitionKind::Stream,
                    &request.seal,
                    request.history_id,
                )?;
                let roots = [
                    stroma.msg_tp_part_dir(&topic, part, group.as_deref()),
                    stroma.tp_part_dir(&topic, part, group.as_deref()),
                ];
                let mut cold_locks = Vec::new();
                let _live_logs = if let Some(handle) = &handle {
                    let logs = [handle.msg_log(), handle.event_log()];
                    if logs.iter().any(|log| log.role() != KeratinRole::Frozen) {
                        return Err(corrupt("sealed logs are not frozen"));
                    }
                    Some(logs)
                } else {
                    for root in &roots {
                        cold_locks.push(lock_existing_log(root).map_err(io_err)?);
                    }
                    None
                };
                let mut page = RecoveryReadPage {
                    history_id: request.history_id,
                    source: request.source,
                    from: request.from,
                    next: request.from,
                    end: 0,
                    records: vec![],
                    snapshot_bytes: vec![],
                };
                for (index, (head, end, digest)) in [
                    (
                        history.message_head,
                        history.message_next,
                        history.message_digest,
                    ),
                    (history.event_head, history.event_next, history.event_digest),
                ]
                .into_iter()
                .enumerate()
                {
                    let selected = (index == 0 && request.source == RecoveryReadSource::Messages)
                        || (index == 1 && request.source == RecoveryReadSource::Events);
                    if selected {
                        if request.from < head || request.from > end {
                            return Err(invalid("recovery offset outside sealed retained range"));
                        }
                        page.end = end;
                    }
                    let reader =
                        FrozenLogReader::open(&roots[index], request.seal.fence_epoch, head, end)
                            .map_err(io_err)?;
                    let mut hash = blake3::Hasher::new();
                    hash.update(b"fibril-retained-log-v1\0");
                    hash.update(&head.to_be_bytes());
                    hash.update(&end.to_be_bytes());
                    let mut used = 0usize;
                    let mut full = false;
                    reader
                        .scan(|record| {
                            hash.update(&record.offset.to_be_bytes());
                            hash.update(&record.flags.to_be_bytes());
                            hash.update(&(record.headers.len() as u64).to_be_bytes());
                            hash.update(record.headers);
                            hash.update(&(record.payload.len() as u64).to_be_bytes());
                            hash.update(record.payload);
                            if selected && record.offset >= request.from && !full {
                                let size = 18 + record.headers.len() + record.payload.len();
                                if page.records.len() == request.max_records as usize
                                    || size > request.max_bytes as usize - used
                                {
                                    if page.records.is_empty() {
                                        return Err(io::Error::new(
                                            io::ErrorKind::InvalidInput,
                                            "next recovery record exceeds page byte budget",
                                        ));
                                    }
                                    full = true;
                                } else {
                                    used += size;
                                    page.next = record.offset + 1;
                                    page.records.push(RecoveryRecord {
                                        offset: record.offset,
                                        flags: record.flags,
                                        headers: record.headers.to_vec(),
                                        payload: record.payload.to_vec(),
                                    });
                                }
                            }
                            Ok(())
                        })
                        .map_err(io_err)?;
                    if hash.finalize().as_bytes() != &digest {
                        return Err(corrupt(
                            "sealed log content changed; recovery page withheld",
                        ));
                    }
                }
                read_snapshot(
                    &stroma.snap_file(&topic, part, group.as_deref()),
                    &history,
                    &request,
                    &mut page,
                )?;
                // Require the same durable metadata after the scan as well.
                stroma.require_matching_recovery_seal(
                    &topic,
                    part,
                    group.as_deref(),
                    &request.seal,
                )?;
                if stroma.require_retained_history(
                    &topic,
                    part,
                    group.as_deref(),
                    expected_kind == PartitionKind::Stream,
                    &request.seal,
                    request.history_id,
                )? != history
                {
                    return Err(corrupt("recovery receipt changed during read"));
                }
                Ok(page)
            })
            .await
            .map_err(io_err)?
        })
        .await
        .map_err(io_err)?
    }
}

fn read_snapshot(
    path: &Path,
    history: &RetainedHistoryIdentity,
    request: &RecoveryReadRequest,
    page: &mut RecoveryReadPage,
) -> Result<()> {
    let selected = request.source == RecoveryReadSource::Snapshot;
    let mut file = match fs::File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound && history.snapshot_digest.is_none() => {
            if selected && request.from != 0 {
                return Err(invalid("recovery snapshot offset outside sealed range"));
            }
            return Ok(());
        }
        Err(err) => return Err(io_err(err)),
    };
    let expected = history
        .snapshot_digest
        .ok_or_else(|| corrupt("unexpected snapshot in sealed history"))?;
    let len = file.metadata().map_err(io_err)?.len();
    let mut header = [0u8; 24];
    file.read_exact(&mut header).map_err(io_err)?;
    if &header[..8] != b"SSNAP\0\0\0"
        || u16::from_be_bytes(header[8..10].try_into().unwrap()) != 1
        || len != 28 + u32::from_be_bytes(header[20..24].try_into().unwrap()) as u64
    {
        return Err(corrupt("invalid sealed snapshot envelope"));
    }
    if selected {
        if request.from > len {
            return Err(invalid("recovery snapshot offset outside sealed range"));
        }
        page.end = len;
    }
    let mut hash = blake3::Hasher::new();
    let mut crc = 0;
    let mut offset = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    use std::io::{Seek, SeekFrom};
    file.seek(SeekFrom::Start(0)).map_err(io_err)?;
    let mut checksum = [0u8; 4];
    while offset < len {
        let n = (len - offset).min(buffer.len() as u64) as usize;
        file.read_exact(&mut buffer[..n]).map_err(io_err)?;
        hash.update(&buffer[..n]);
        let start = offset.max(8);
        let end = (offset + n as u64).min(len - 4);
        if start < end {
            crc = crc32c::crc32c_append(
                crc,
                &buffer[(start - offset) as usize..(end - offset) as usize],
            );
        }
        let start = offset.max(len - 4);
        let end = offset + n as u64;
        if start < end {
            checksum[(start - (len - 4)) as usize..(end - (len - 4)) as usize]
                .copy_from_slice(&buffer[(start - offset) as usize..n]);
        }
        if selected {
            let start = offset.max(request.from);
            let end =
                (offset + n as u64).min(request.from.saturating_add(request.max_bytes as u64));
            if start < end {
                page.snapshot_bytes
                    .extend_from_slice(&buffer[(start - offset) as usize..(end - offset) as usize]);
            }
        }
        offset += n as u64;
    }
    if hash.finalize().as_bytes() != &expected || crc != u32::from_be_bytes(checksum) {
        return Err(corrupt("sealed snapshot changed; recovery page withheld"));
    }
    if selected {
        page.next = request.from + page.snapshot_bytes.len() as u64;
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use keratin_log::test_dir;

    async fn populate(root: &Path) -> Stroma {
        let mut cfg = KeratinConfig::test_default();
        cfg.segment_max_bytes = 256;
        let s = Stroma::open(
            root,
            StromaKeratinConfig::from_message_log(cfg),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        s.become_queue_follower_with_epoch("q", 0, None, 7)
            .await
            .unwrap();
        for offset in 0..6 {
            s.apply_replicated_queue_batch(
                "q",
                0,
                None,
                Some(ReplicatedMessageBatch {
                    epoch: 7,
                    first_offset: offset,
                    durability: Some(KDurability::AfterFsync),
                    records: vec![Message {
                        flags: offset as u16,
                        headers: vec![2, 3],
                        payload: vec![offset as u8; 80],
                    }],
                }),
                Some(ReplicatedEventBatch {
                    epoch: 7,
                    first_offset: offset,
                    durability: Some(KDurability::AfterFsync),
                    events: vec![StromaEvent::Enqueue {
                        off: offset,
                        retries: 0,
                        expire_at: None,
                    }],
                }),
            )
            .await
            .unwrap();
        }
        s
    }
    async fn seal(s: &Stroma) -> RecoveryReadRequest {
        let seal = RecoverySealRequest {
            transition: [17; 32],
            fence_epoch: 8,
        };
        let sealed = s
            .seal_replica_for_recovery("q", 0, None, seal.clone())
            .await
            .unwrap();
        RecoveryReadRequest {
            seal,
            history_id: sealed.history.id,
            source: RecoveryReadSource::Messages,
            from: 0,
            max_records: 2,
            max_bytes: 200,
        }
    }
    async fn read(s: &Stroma, request: RecoveryReadRequest) -> Result<RecoveryReadPage> {
        s.read_sealed_replica("q", 0, None, PartitionKind::Queue, request)
            .await
    }
    fn files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        let mut result = BTreeMap::new();
        fn visit(root: &Path, result: &mut BTreeMap<PathBuf, Vec<u8>>) {
            for entry in fs::read_dir(root).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    visit(&path, result);
                } else {
                    result.insert(path.clone(), fs::read(path).unwrap());
                }
            }
        }
        visit(root, &mut result);
        result
    }
    use std::collections::BTreeMap;

    #[tokio::test]
    async fn frozen_pages_span_segments_and_are_identical_after_cold_restart_without_writes() {
        let dir = test_dir!("frozen_pages_restart");
        let s = populate(&dir.root).await;
        let request = seal(&s).await;
        let before = files(&dir.root);
        let first = read(&s, request.clone()).await.unwrap();
        assert_eq!((first.next, first.end, first.records.len()), (2, 6, 2));
        assert_eq!(first.records[1].payload, vec![1; 80]);
        let mut second = request.clone();
        second.from = first.next;
        assert_eq!(read(&s, second).await.unwrap().next, 4);
        assert_eq!(files(&dir.root), before);
        assert!(s.queue_handle("q", 0, None).await.is_err());
        s.shutdown().await.unwrap();
        drop(s);
        let s = Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let before = files(&dir.root);
        assert_eq!(read(&s, request).await.unwrap(), first);
        assert_eq!(files(&dir.root), before);
        assert_eq!(s.lazy_recoveries_started.load(Ordering::Relaxed), 0);
        assert!(s.queue_handle("q", 0, None).await.is_err());
        s.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn frozen_read_requires_completed_matching_seal_and_strict_budgets() {
        let dir = test_dir!("frozen_read_binding");
        let s = populate(&dir.root).await;
        let unsealed = RecoveryReadRequest {
            seal: RecoverySealRequest {
                transition: [17; 32],
                fence_epoch: 8,
            },
            history_id: [0; 32],
            source: RecoveryReadSource::Messages,
            from: 0,
            max_records: 1,
            max_bytes: 200,
        };
        assert!(read(&s, unsealed).await.is_err());
        let request = seal(&s).await;
        let before = files(&dir.root);
        for variant in 0..7 {
            let mut bad = request.clone();
            match variant {
                0 => bad.seal.transition[0] ^= 1,
                1 => bad.seal.fence_epoch += 1,
                2 => bad.history_id[0] ^= 1,
                3 => bad.from = 7,
                4 => bad.max_bytes = 99,
                5 => bad.max_records = 0,
                _ => bad.max_bytes = MAX_PAGE_BYTES + 1,
            }
            assert!(read(&s, bad).await.is_err(), "variant {variant}");
        }
        assert!(
            s.read_sealed_replica("q", 0, None, PartitionKind::Stream, request.clone())
                .await
                .is_err()
        );
        let mut eof = request.clone();
        eof.from = 6;
        assert!(read(&s, eof).await.unwrap().records.is_empty());
        let mut snapshot = request.clone();
        snapshot.source = RecoveryReadSource::Snapshot;
        assert_eq!(read(&s, snapshot).await.unwrap().end, 0);
        assert_eq!(files(&dir.root), before);
        fs::remove_file(s.snap_dir("q", 0, None).join("recovery.history")).unwrap();
        assert!(read(&s, request).await.is_err());
        assert!(!s.snap_dir("q", 0, None).join("recovery.history").exists());
        s.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn frozen_scan_detects_changed_suffix_outside_page_and_corrupt_headers_without_repair() {
        use std::io::{Seek, SeekFrom, Write};
        let dir = test_dir!("frozen_read_damage");
        let s = populate(&dir.root).await;
        let request = seal(&s).await;
        let first = s
            .msg_tp_part_dir("q", 0, None)
            .join("segments/00000000000000000000.log");
        let original = fs::read(&first).unwrap();
        for mutation in 0..3 {
            let mut bytes = original.clone();
            match mutation {
                0 => bytes[0] ^= 1,
                1 => bytes[68 + 12..68 + 16].copy_from_slice(&u32::MAX.to_be_bytes()),
                _ => bytes[68 + 24..68 + 32].copy_from_slice(&9u64.to_be_bytes()),
            }
            fs::write(&first, bytes).unwrap();
            let before = files(&dir.root);
            assert!(read(&s, request.clone()).await.is_err());
            assert_eq!(files(&dir.root), before);
            fs::write(&first, &original).unwrap();
        }
        // Change a valid CRC-protected record beyond the requested two records.
        let path = s
            .msg_tp_part_dir("q", 0, None)
            .join("segments/00000000000000000005.log");
        // Segment bases depend on packing; find the last data segment instead.
        let path = if path.exists() {
            path
        } else {
            let mut p: Vec<_> = fs::read_dir(s.msg_tp_part_dir("q", 0, None).join("segments"))
                .unwrap()
                .map(|e| e.unwrap().path())
                .filter(|p| p.extension().is_some_and(|e| e == "log"))
                .collect();
            p.sort();
            p.pop().unwrap()
        };
        let mut bytes = fs::read(&path).unwrap();
        let start = 68usize;
        let h = u32::from_be_bytes(bytes[start + 8..start + 12].try_into().unwrap()) as usize;
        let p = u32::from_be_bytes(bytes[start + 12..start + 16].try_into().unwrap()) as usize;
        bytes[start + 32 + h] ^= 1;
        let end = start + 32 + h + p;
        let crc = crc32c::crc32c(&bytes[start + 2..end]);
        bytes[end..end + 4].copy_from_slice(&crc.to_be_bytes());
        let mut f = fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&bytes).unwrap();
        f.sync_all().unwrap();
        assert!(matches!(
            read(&s, request).await,
            Err(StromaError::Corruption(_))
        ));
        assert!(s.queue_handle("q", 0, None).await.is_err());
        s.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn frozen_snapshot_pages_verify_whole_envelope_and_preserve_seal() {
        let dir = test_dir!("frozen_snapshot_pages");
        let s = populate(&dir.root).await;
        let mut state = crate::QueueInternalState::new("q".into(), 0);
        state.enqueue(0, 0, None);
        // Exercise a checksum split across the scanner's 64 KiB chunks. This
        // API treats the snapshot payload as opaque; state validation is later.
        let mut blob = state.encode_snapshot(0);
        blob.resize(65_509, 5);
        s.write_queue_snapshot("q", 0, None, 0, &blob).unwrap();
        let mut request = seal(&s).await;
        request.source = RecoveryReadSource::Snapshot;
        request.max_bytes = 10_007;
        let raw = fs::read(s.snap_file("q", 0, None)).unwrap();
        let mut assembled = vec![];
        loop {
            let page = read(&s, request.clone()).await.unwrap();
            assert!(page.snapshot_bytes.len() <= 10_007);
            assembled.extend(page.snapshot_bytes);
            if page.next == page.end {
                break;
            }
            request.from = page.next;
        }
        assert_eq!(assembled, raw);
        let mut changed = raw;
        let i = changed.len() - 5;
        changed[i] ^= 1;
        fs::write(s.snap_file("q", 0, None), changed).unwrap();
        request.from = 0;
        assert!(read(&s, request).await.is_err());
        s.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn frozen_reads_preserve_nonzero_retained_heads_and_respect_cold_locks() {
        let dir = test_dir!("frozen_read_head_lock");
        let s = populate(&dir.root).await;
        let ticket = s.queue_handle("q", 0, None).await.unwrap();
        let handle = ticket.resolve().unwrap();
        let head = handle.msg_log().truncate_before(4).await.unwrap();
        assert!(head > 0 && head < 6);
        drop(handle);
        drop(ticket);
        let mut request = seal(&s).await;
        assert!(read(&s, request.clone()).await.is_err());
        request.from = head;
        let page = read(&s, request.clone()).await.unwrap();
        assert_eq!(page.records[0].offset, head);
        let root = s.msg_tp_part_dir("q", 0, None);
        s.shutdown().await.unwrap();
        drop(s);
        let s = Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let lock = lock_existing_log(&root).unwrap();
        let before = files(&dir.root);
        assert!(read(&s, request.clone()).await.is_err());
        assert_eq!(files(&dir.root), before);
        drop(lock);
        assert_eq!(read(&s, request.clone()).await.unwrap(), page);
        for name in ["recovery.history", "recovery.seal"] {
            let path = s.snap_dir("q", 0, None).join(name);
            let original = fs::read(&path).unwrap();
            fs::write(&path, vec![0; 65_537]).unwrap();
            assert!(read(&s, request.clone()).await.is_err());
            fs::write(path, original).unwrap();
        }
        s.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn frozen_reads_cover_empty_replicas_and_streams() {
        let dir = test_dir!("frozen_read_empty_stream");
        let s = Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        s.become_queue_follower_with_epoch("q", 0, None, 7)
            .await
            .unwrap();
        let request = seal(&s).await;
        let page = read(&s, request).await.unwrap();
        assert_eq!((page.from, page.next, page.end), (0, 0, 0));
        assert!(page.records.is_empty());
        s.create_stream("stream", 0, None).await.unwrap();
        s.append_stream_record(
            "stream",
            0,
            &MessageHeaders {
                published: 0,
                publish_received: 0,
                content_type: None,
                extra: HashMap::new(),
            },
            b"body".to_vec(),
        )
        .await
        .unwrap();
        let seal = RecoverySealRequest {
            transition: [17; 32],
            fence_epoch: 8,
        };
        let sealed = s
            .seal_replica_for_recovery("stream", 0, None, seal.clone())
            .await
            .unwrap();
        let request = RecoveryReadRequest {
            seal,
            history_id: sealed.history.id,
            source: RecoveryReadSource::Messages,
            from: 0,
            max_records: 2,
            max_bytes: 1024,
        };
        let page = s
            .read_sealed_replica("stream", 0, None, PartitionKind::Stream, request.clone())
            .await
            .unwrap();
        assert_eq!(page.records[0].payload, b"body");
        assert_eq!(page.end, 1);
        assert!(
            s.read_sealed_replica("stream", 0, None, PartitionKind::Queue, request)
                .await
                .is_err()
        );
        s.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_frozen_read_retains_admission_until_its_owned_work_finishes() {
        let dir = test_dir!("frozen_read_cancel");
        let s = populate(&dir.root).await;
        let request = seal(&s).await;
        let guard = s.lock_partition_lifecycle("q", 0, None).await;
        let copy = s.clone();
        let req = request.clone();
        let call = tokio::spawn(async move { read(&copy, req).await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while s.recovery_read_slots.available_permits() != 0 {
                tokio::task::yield_now().await
            }
        })
        .await
        .unwrap();
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        assert!(read(&s, request.clone()).await.is_err());
        drop(guard);
        tokio::time::timeout(Duration::from_secs(2), async {
            while s.recovery_read_slots.available_permits() == 0 {
                tokio::task::yield_now().await
            }
        })
        .await
        .unwrap();
        assert!(read(&s, request).await.is_ok());
        s.shutdown().await.unwrap();
    }
}
