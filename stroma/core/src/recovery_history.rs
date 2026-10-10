//! Content identity for frozen retained records, independent of promise epochs.
//! Exact identity is useful for retries; it does not establish ancestry, a
//! committed prefix, or compatibility across different compaction boundaries.

use super::*;
use std::io::Write;

const MAGIC: &[u8; 8] = b"RHIST\0\0\x02";
/// Domain tag of the retained history id. Bump it whenever the identity
/// layout changes so an old id never verifies under a new meaning.
const HISTORY_ID_TAG: &[u8] = b"fibril-retained-history-v2\0";

/// Physical queue layout bound into recovery evidence. Empty separate queues
/// remain distinct from a queue whose payloads live in the event records.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueLogLayout {
    #[default]
    Separate,
    Combined,
}
impl QueueLogLayout {
    pub fn is_separate(&self) -> bool {
        matches!(self, Self::Separate)
    }
}

/// Retained bounds and content digest of one physical log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetainedLog {
    pub head: u64,
    pub next: u64,
    pub digest: [u8; 32],
}

/// Physical logs bound into recovery evidence. The variant is part of the
/// history identity, so a queue history can never be read as a stream one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetainedLogs {
    /// One log carrying payloads and state events.
    Queue(RetainedLog),
    /// Record log and cursor-commit event log.
    Stream {
        records: RetainedLog,
        cursor_events: RetainedLog,
    },
}
impl RetainedLogs {
    pub fn is_stream(&self) -> bool {
        matches!(self, Self::Stream { .. })
    }

    /// Log answering a read source. `StreamRecords` names no log in a queue
    /// history and `Snapshot` is not a log.
    pub fn for_source(&self, source: RecoveryReadSource) -> Option<&RetainedLog> {
        match (self, source) {
            (Self::Queue(log), RecoveryReadSource::Log) => Some(log),
            (Self::Stream { records, .. }, RecoveryReadSource::StreamRecords) => Some(records),
            (Self::Stream { cursor_events, .. }, RecoveryReadSource::Log) => Some(cursor_events),
            _ => None,
        }
    }

    /// Every retained log with its read source, record log first.
    pub fn sources(&self) -> Vec<(RecoveryReadSource, &RetainedLog)> {
        match self {
            Self::Queue(log) => vec![(RecoveryReadSource::Log, log)],
            Self::Stream { records, cursor_events } => vec![
                (RecoveryReadSource::StreamRecords, records),
                (RecoveryReadSource::Log, cursor_events),
            ],
        }
    }

    fn well_formed(&self) -> bool {
        match self {
            Self::Queue(log) => log.head <= log.next,
            Self::Stream { records, cursor_events } => {
                records.head <= records.next && cursor_events.head <= cursor_events.next
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedHistoryIdentity {
    pub version: u32,
    /// Present for version two: original receipt, included in the content ID.
    /// Its storage instance remains the original instance after a cold reopen.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_history: Option<PreparedStorageHistory>,
    pub id: [u8; 32],
    pub snapshot_digest: Option<[u8; 32]>,
    pub logs: RetainedLogs,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Receipt {
    topic: String,
    partition: u32,
    group: Option<String>,
    stream: bool,
    request: RecoverySealRequest,
    history: RetainedHistoryIdentity,
    #[serde(default)]
    checkpoint_snapshot: bool,
}

impl RetainedHistoryIdentity {
    /// Version one has no storage receipt and version two must have one. A
    /// queue history needs version two with a queue receipt. A stream history
    /// with a receipt needs a stream receipt. Log bounds must be ordered.
    pub fn valid_version(&self) -> bool {
        matches!(
            (self.version, self.storage_history.is_some()),
            (1, false) | (2, true)
        ) && self.logs.well_formed()
            && match &self.logs {
                RetainedLogs::Queue(_) => {
                    self.version == 2 && self.storage_history.as_ref().is_some_and(|h| !h.stream)
                }
                RetainedLogs::Stream { .. } => self.storage_history.as_ref().is_none_or(|h| h.stream),
            }
    }

    /// The single queue log, or None for a stream history.
    pub fn queue_log(&self) -> Option<&RetainedLog> {
        match &self.logs {
            RetainedLogs::Queue(log) => Some(log),
            RetainedLogs::Stream { .. } => None,
        }
    }

    /// True when any retained log starts above offset zero.
    pub fn compacted(&self) -> bool {
        self.logs.sources().iter().any(|(_, log)| log.head > 0)
    }

    /// Content id over the resource and this identity with a zero id field.
    pub fn compute_id(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        stream: bool,
    ) -> Result<[u8; 32]> {
        let mut identity = self.clone();
        identity.id = [0; 32];
        let mut hash = blake3::Hasher::new();
        hash.update(HISTORY_ID_TAG);
        hash.update(
            &rmp_serde::to_vec_named(&(topic, part, group, stream, &identity))
                .map_err(encode_err)?,
        );
        Ok(*hash.finalize().as_bytes())
    }
}

fn retained_log(log: &Keratin) -> Result<RetainedLog> {
    Ok(RetainedLog {
        head: log.head_offset(),
        next: log.next_offset(),
        digest: log_digest(log)?,
    })
}

fn log_digest(log: &Keratin) -> Result<[u8; 32]> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"fibril-retained-log-v1\0");
    let mut next = log.head_offset();
    let end = log.next_offset();
    hash.update(&next.to_be_bytes());
    hash.update(&end.to_be_bytes());
    let mut cursor = log
        .retained_durable_cursor(log.current_epoch(), next, end, u64::MAX)
        .map_err(io_err)?;
    while let Some(record) = cursor
        .next_record(keratin_log::MAX_FROZEN_RECORD_BYTES)
        .map_err(io_err)?
    {
        hash.update(&record.offset.to_be_bytes());
        hash.update(&record.flags.to_be_bytes());
        hash.update(&(record.headers.len() as u64).to_be_bytes());
        hash.update(record.headers);
        hash.update(&(record.payload.len() as u64).to_be_bytes());
        hash.update(record.payload);
        next += 1;
    }
    if next != end {
        return Err(StromaError::Corruption(
            "sealed log ended before durable boundary".into(),
        ));
    }
    Ok(*hash.finalize().as_bytes())
}

fn read_receipt(path: &Path) -> Result<Option<Receipt>> {
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(io_err(err)),
    };
    use std::io::Read;
    let mut bytes = Vec::new();
    file.take(65_537).read_to_end(&mut bytes).map_err(io_err)?;
    if bytes.len() > 65_536 {
        return Err(StromaError::Corruption(
            "recovery metadata exceeds size limit".into(),
        ));
    }
    if bytes.len() < 12 || &bytes[..8] != MAGIC {
        return Err(StromaError::Corruption(
            "invalid retained-history header".into(),
        ));
    }
    let end = bytes.len() - 4;
    if crc32c::crc32c(&bytes[..end]) != u32::from_be_bytes(bytes[end..].try_into().unwrap()) {
        return Err(StromaError::Corruption(
            "retained-history checksum mismatch".into(),
        ));
    }
    rmp_serde::from_slice(&bytes[8..end])
        .map(Some)
        .map_err(|err| StromaError::Corruption(format!("invalid retained-history receipt: {err}")))
}

impl Stroma {
    /// Reopen a previously completed queue seal's exact immutable receipt.
    /// This restates recorded evidence and does not revalidate physical bytes.
    /// Inspection and installation must still verify every required record.
    pub fn completed_queue_recovery_seal(
        &self, topic: &str, part: u32, group: Option<&str>, request: &RecoverySealRequest,
    ) -> Result<Option<SealedReplicaFrontiers>> {
        let group = normalize_group(group);
        if !self.recovery_seal_path(topic, part, group).try_exists().map_err(io_err)? { return Ok(None); }
        self.require_matching_recovery_seal(topic, part, group, request)?;
        if self.read_partition_kind(topic, part, group) != PartitionKind::Queue {
            return Err(StromaError::InvalidArgument("completed queue seal cannot describe a stream".into()));
        }
        let path = self.snap_dir(topic, part, group).join("recovery.history");
        let Some(receipt) = read_receipt(&path)? else { return Ok(None); };
        let history = self.require_retained_history(topic, part, group, false, request, receipt.history.id)?;
        // Complete a visible publication barrier left by an interrupted caller.
        fs::File::open(&path).and_then(|file| file.sync_all()).map_err(io_err)?;
        recovery_seal::sync_directories(path.parent().unwrap())?;
        Ok(Some(SealedReplicaFrontiers { request: request.clone(), history }))
    }

    pub(super) fn recovery_snapshot_path(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<PathBuf> {
        let root = self.snap_dir(topic, part, group);
        let receipt = read_receipt(&root.join("recovery.history"))?
            .ok_or_else(|| StromaError::Corruption("sealed snapshot has no receipt".into()))?;
        Ok(if receipt.checkpoint_snapshot {
            root.join("recovery.snapshot")
        } else {
            self.snap_file(topic, part, group)
        })
    }

    pub(super) fn require_retained_history(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        stream: bool,
        request: &RecoverySealRequest,
        expected_id: [u8; 32],
    ) -> Result<RetainedHistoryIdentity> {
        let receipt = read_receipt(&self.snap_dir(topic, part, group).join("recovery.history"))?
            .ok_or_else(|| {
                StromaError::InvalidArgument(
                    "seal has no completed history receipt; retry sealing first".into(),
                )
            })?;
        if receipt.topic != topic
            || receipt.partition != part
            || receipt.group.as_deref() != group
            || receipt.stream != stream
            || &receipt.request != request
            || receipt.history.id != expected_id
            || !receipt.history.valid_version()
            || receipt.history.logs.is_stream() != stream
            || receipt.history.storage_history
                != self.durable_storage_history_receipt(topic, part, group)?
        {
            return Err(StromaError::InvalidArgument(
                "recovery read does not match history receipt".into(),
            ));
        }
        if receipt.history.compute_id(topic, part, group, stream)? != expected_id {
            return Err(StromaError::Corruption(
                "retained-history identity mismatch".into(),
            ));
        }
        Ok(receipt.history)
    }

    pub(super) fn persist_retained_history(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        request: RecoverySealRequest,
        messages: Option<&Keratin>,
        events: &Keratin,
    ) -> Result<RetainedHistoryIdentity> {
        let path = self.snap_dir(topic, part, group).join("recovery.history");
        let stream = self.read_partition_kind(topic, part, group) == PartitionKind::Stream;
        let storage_history = self.durable_storage_history_receipt(topic, part, group)?;
        let agreed = if stream {
            None
        } else {
            self.agreed_recovery_snapshot(
                topic,
                part,
                group,
                storage_history.as_ref(),
                events,
            )?
        };
        let checkpoint_snapshot = agreed.is_some();
        let snapshot = if let Some(bytes) = agreed {
            let root = self.snap_dir(topic, part, group);
            let target = root.join("recovery.snapshot");
            let temporary = root.join("recovery.snapshot.pending");
            let mut file = fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temporary)
                .map_err(io_err)?;
            file.write_all(&bytes).map_err(io_err)?;
            file.sync_all().map_err(io_err)?;
            drop(file);
            fs::rename(&temporary, &target).map_err(io_err)?;
            recovery_seal::sync_directories(&root)?;
            target
        } else {
            self.snap_file(topic, part, group)
        };
        // Validate the selected envelope and bind its exact bytes to the seal.
        let snapshot_digest = match self.read_queue_snapshot(&snapshot)? {
            Some(_) => Some(*blake3::hash(&fs::read(snapshot).map_err(io_err)?).as_bytes()),
            None => None,
        };
        // The partition kind decides the variant. A queue never has a record
        // log and a stream always has one.
        let logs = match (stream, messages) {
            (false, None) => RetainedLogs::Queue(retained_log(events)?),
            (true, Some(records)) => RetainedLogs::Stream {
                records: retained_log(records)?,
                cursor_events: retained_log(events)?,
            },
            _ => {
                return Err(StromaError::InvalidArgument(
                    "retained logs do not match the partition kind".into(),
                ));
            }
        };
        let mut history = RetainedHistoryIdentity {
            version: if storage_history.is_some() { 2 } else { 1 },
            storage_history,
            id: [0; 32],
            snapshot_digest,
            logs,
        };
        if !history.valid_version() {
            return Err(StromaError::InvalidArgument(
                "unsupported retained history layout or binding".into(),
            ));
        }
        history.id = history.compute_id(topic, part, group, stream)?;
        let receipt = Receipt {
            topic: topic.into(),
            partition: part,
            group: group.map(str::to_owned),
            stream,
            request,
            history: history.clone(),
            checkpoint_snapshot,
        };
        if let Some(old) = read_receipt(&path)? {
            if old != receipt {
                return Err(StromaError::Corruption(
                    "sealed retained history changed; recovery evidence withheld".into(),
                ));
            }
            fs::File::open(&path)
                .and_then(|f| f.sync_all())
                .map_err(io_err)?;
            recovery_seal::sync_directories(path.parent().unwrap())?;
            return Ok(history);
        }
        let mut bytes = MAGIC.to_vec();
        bytes.extend(rmp_serde::to_vec_named(&receipt).map_err(encode_err)?);
        bytes.extend(crc32c::crc32c(&bytes).to_be_bytes());
        let temp =
            path.with_file_name(format!("recovery.history.{}.pending", uuid::Uuid::now_v7()));
        let result = (|| {
            let mut f = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)
                .map_err(io_err)?;
            f.write_all(&bytes).map_err(io_err)?;
            f.sync_all().map_err(io_err)?;
            drop(f);
            fs::rename(&temp, &path).map_err(io_err)?;
            recovery_seal::sync_directories(path.parent().unwrap())
        })();
        if result.is_err() {
            let _ = fs::remove_file(temp);
        }
        result?;
        Ok(history)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use keratin_log::test_dir;

    fn storage(stream: bool) -> PreparedStorageHistory {
        PreparedStorageHistory {
            topic: "q".into(),
            partition: 0,
            group: None,
            stream,
            binding: StorageHistoryBinding {
                resource_incarnation: [1; 16],
                accepted_history: [2; 16],
                writer_session: [3; 16],
            },
            storage_instance: [4; 16],
        }
    }

    #[test]
    fn retained_logs_variant_is_explicit_and_bound_to_identity() {
        let log = RetainedLog { head: 2, next: 9, digest: [5; 32] };
        let mut queue = RetainedHistoryIdentity {
            version: 2,
            storage_history: Some(storage(false)),
            id: [0; 32],
            snapshot_digest: None,
            logs: RetainedLogs::Queue(log),
        };
        assert!(queue.valid_version());
        assert_eq!(queue.queue_log(), Some(&log));
        assert_eq!(queue.logs.for_source(RecoveryReadSource::Log), Some(&log));
        assert_eq!(queue.logs.for_source(RecoveryReadSource::StreamRecords), None);
        assert_eq!(queue.logs.for_source(RecoveryReadSource::Snapshot), None);
        let encoded = rmp_serde::to_vec_named(&queue).unwrap();
        assert_eq!(rmp_serde::from_slice::<RetainedHistoryIdentity>(&encoded).unwrap(), queue);

        // The same bounds as a stream history never share a queue id.
        let mut stream = queue.clone();
        stream.storage_history = Some(storage(true));
        stream.logs = RetainedLogs::Stream { records: log, cursor_events: log };
        assert!(stream.valid_version());
        assert_eq!(stream.queue_log(), None);
        assert_ne!(
            queue.compute_id("q", 0, None, false).unwrap(),
            stream.compute_id("q", 0, None, false).unwrap()
        );
        assert_ne!(
            queue.compute_id("q", 0, None, false).unwrap(),
            queue.compute_id("q", 0, None, true).unwrap()
        );

        // A queue needs version two with a queue receipt.
        queue.storage_history.as_mut().unwrap().stream = true;
        assert!(!queue.valid_version());
        queue.storage_history = None;
        queue.version = 1;
        assert!(!queue.valid_version());
        // A stream receipt must say stream.
        stream.storage_history.as_mut().unwrap().stream = false;
        assert!(!stream.valid_version());
        stream.storage_history = None;
        stream.version = 1;
        assert!(stream.valid_version());
        // Inverted bounds are refused for either kind.
        let inverted = RetainedLog { head: 9, next: 2, digest: [5; 32] };
        stream.logs = RetainedLogs::Stream { records: log, cursor_events: inverted };
        assert!(!stream.valid_version());
        let mut queue = RetainedHistoryIdentity {
            version: 2,
            storage_history: Some(storage(false)),
            id: [0; 32],
            snapshot_digest: None,
            logs: RetainedLogs::Queue(inverted),
        };
        assert!(!queue.valid_version());
        queue.logs = RetainedLogs::Queue(log);
        let mut unknown = rmp_serde::to_vec_named(&queue).unwrap();
        let start = unknown.windows(5).position(|b| b == b"queue").unwrap();
        unknown[start..start + 5].copy_from_slice(b"other");
        assert!(rmp_serde::from_slice::<RetainedHistoryIdentity>(&unknown).is_err());
    }

    async fn populated(root: &Path, payload: &[u8]) -> Stroma {
        let s = Stroma::open(
            root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        s.initialize_empty_storage_history(
            "q",
            0,
            None,
            PartitionKind::Queue,
            StorageHistoryBinding {
                resource_incarnation: [1; 16],
                accepted_history: [2; 16],
                writer_session: [3; 16],
            },
        )
        .await
        .unwrap();
        s.become_queue_follower_with_epoch("q", 0, None, 7)
            .await
            .unwrap();
        s.apply_replicated_queue_batch(
            "q",
            0,
            None,
            ReplicatedEventBatch {
                epoch: 7,
                first_offset: 0,
                events: vec![StromaEvent::PayloadEnqueue {
                    not_before: None,
                    expire_at: None,
                    headers: vec![],
                    payload: payload.to_vec(),
                }],
                durability: Some(KDurability::AfterFsync),
            },
        )
        .await
        .unwrap();
        s
    }
    fn request(epoch: u64) -> RecoverySealRequest {
        RecoverySealRequest {
            transition: [19; 32],
            fence_epoch: epoch,
        }
    }

    #[tokio::test]
    async fn history_binds_storage_instance_while_content_digest_ignores_fence_and_append_time() {
        let a = test_dir!("history_a");
        let b = test_dir!("history_b");
        let c = test_dir!("history_c");
        let first = populated(&a.root, b"same").await;
        tokio::time::sleep(Duration::from_millis(3)).await;
        let second = populated(&b.root, b"same").await;
        let different = populated(&c.root, b"else").await;
        let x = first
            .seal_replica_for_recovery("q", 0, None, request(8))
            .await
            .unwrap();
        let y = second
            .seal_replica_for_recovery("q", 0, None, request(13))
            .await
            .unwrap();
        let z = different
            .seal_replica_for_recovery("q", 0, None, request(8))
            .await
            .unwrap();
        assert_ne!(x.history.id, y.history.id);
        assert_ne!(x.history.storage_history.as_ref().unwrap().storage_instance, y.history.storage_history.as_ref().unwrap().storage_instance);
        let log = |s: &SealedReplicaFrontiers| *s.history.queue_log().unwrap();
        assert_eq!(log(&x), log(&y));
        assert_ne!(x.history.id, z.history.id);
        assert_ne!(log(&x).digest, log(&z).digest);
        assert_eq!((log(&x).head, log(&x).next), (log(&z).head, log(&z).next));
        first.shutdown().await.unwrap();
        second.shutdown().await.unwrap();
        different.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn completed_queue_seal_survives_reopen_and_rejects_mismatched_or_damaged_evidence() {
        let dir = test_dir!("completed_history_receipt");
        let s = populated(&dir.root, b"data").await;
        assert_eq!(s.completed_queue_recovery_seal("q", 0, None, &request(8)).unwrap(), None);
        let expected = s.seal_replica_for_recovery("q", 0, None, request(8)).await.unwrap();
        assert_eq!(s.completed_queue_recovery_seal("q", 0, None, &request(8)).unwrap(), Some(expected.clone()));
        assert!(s.completed_queue_recovery_seal("q", 0, None, &request(9)).is_err());
        s.shutdown().await.unwrap();
        drop(s);
        let s = Stroma::open(&dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default()).await.unwrap();
        assert_eq!(s.completed_queue_recovery_seal("q", 0, None, &request(8)).unwrap(), Some(expected));
        let path = s.snap_dir("q", 0, None).join("recovery.history");
        let original = fs::read(&path).unwrap();
        fs::remove_file(&path).unwrap();
        assert_eq!(s.completed_queue_recovery_seal("q", 0, None, &request(8)).unwrap(), None);
        // A seal intent without its completed receipt remains fenced.
        assert!(s.queue_handle("q", 0, None).await.is_err());
        fs::write(&path, b"damaged").unwrap();
        assert!(s.completed_queue_recovery_seal("q", 0, None, &request(8)).is_err());
        fs::write(&path, &original).unwrap();
        // A valid frame cannot rebind the receipt to a different resource.
        let mut receipt = read_receipt(&path).unwrap().unwrap();
        receipt.topic = "other".into();
        let mut frame = MAGIC.to_vec();
        frame.extend(rmp_serde::to_vec_named(&receipt).unwrap());
        frame.extend(crc32c::crc32c(&frame).to_be_bytes());
        fs::write(&path, frame).unwrap();
        assert!(s.completed_queue_recovery_seal("q", 0, None, &request(8)).is_err());
        fs::write(&path, original).unwrap();
        assert!(s.completed_queue_recovery_seal("q", 0, None, &request(8)).unwrap().is_some());
        s.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn history_detects_valid_changed_disk_records_even_with_a_warm_cache() {
        let a = test_dir!("history_cache_a");
        let b = test_dir!("history_cache_b");
        let first = populated(&a.root, b"same").await;
        let other = populated(&b.root, b"else").await;
        first
            .seal_replica_for_recovery("q", 0, None, request(8))
            .await
            .unwrap();
        let segment = "segments/00000000000000000000.log";
        fs::copy(
            other.tp_part_dir("q", 0, None).join(segment),
            first.tp_part_dir("q", 0, None).join(segment),
        )
        .unwrap();
        // Restating a completed receipt does not bless changed bytes. The
        // strict sealing path must still detect a valid but different record.
        assert!(first.completed_queue_recovery_seal("q", 0, None, &request(8)).unwrap().is_some());
        assert!(matches!(
            first
                .seal_replica_for_recovery("q", 0, None, request(8))
                .await,
            Err(StromaError::Corruption(_))
        ));
        first.shutdown().await.unwrap();
        other.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn history_receipt_io_failure_and_corruption_never_release_a_seal() {
        let dir = test_dir!("history_receipt_failure");
        let s = populated(&dir.root, b"data").await;
        let path = s.snap_dir("q", 0, None).join("recovery.history");
        fs::create_dir_all(&path).unwrap();
        assert!(
            s.seal_replica_for_recovery("q", 0, None, request(8))
                .await
                .is_err()
        );
        assert!(s.queue_handle("q", 0, None).await.is_err());
        fs::remove_dir(&path).unwrap();
        let expected = s
            .seal_replica_for_recovery("q", 0, None, request(8))
            .await
            .unwrap();
        s.shutdown().await.unwrap();
        drop(s);
        let s = Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            s.seal_replica_for_recovery("q", 0, None, request(8))
                .await
                .unwrap(),
            expected
        );
        fs::write(path, b"damaged").unwrap();
        assert!(
            s.seal_replica_for_recovery("q", 0, None, request(8))
                .await
                .is_err()
        );
        assert!(s.queue_handle("q", 0, None).await.is_err());
        s.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn history_covers_checkpoint_bytes_and_kind_mismatch_does_not_fence() {
        let dir = test_dir!("history_snapshot_kind");
        let s = populated(&dir.root, b"data").await;
        assert!(matches!(
            s.seal_replica_for_recovery_checked("q", 0, None, PartitionKind::Stream, request(8))
                .await,
            Err(StromaError::WrongPartitionKind { .. })
        ));
        let ticket = s.queue_handle("q", 0, None).await.unwrap();
        assert_eq!(ticket.resolve().unwrap().event_log().current_epoch(), 7);
        let mut state = crate::QueueInternalState::new("q".into(), 0);
        s.write_queue_snapshot("q", 0, None, 0, &state.encode_snapshot(0))
            .unwrap();
        let original = s
            .seal_replica_for_recovery("q", 0, None, request(8))
            .await
            .unwrap();
        assert!(original.history.snapshot_digest.is_some());
        state.enqueue(0, 0, None);
        s.write_queue_snapshot("q", 0, None, 0, &state.encode_snapshot(0))
            .unwrap();
        assert!(matches!(
            s.seal_replica_for_recovery("q", 0, None, request(8)).await,
            Err(StromaError::Corruption(_))
        ));
        s.shutdown().await.unwrap();
    }
}
