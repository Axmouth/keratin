//! Content identity for frozen retained records, independent of promise epochs.
//! Exact identity is useful for retries; it does not establish ancestry, a
//! committed prefix, or compatibility across different compaction boundaries.

use super::*;
use std::io::Write;

const MAGIC: &[u8; 8] = b"RHIST\0\0\x01";

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetainedHistoryIdentity {
    // Omitting the default preserves existing separate-history identity bytes.
    #[serde(default, skip_serializing_if = "QueueLogLayout::is_separate")]
    pub log_layout: QueueLogLayout,
    pub version: u32,
    /// Present for version two: original receipt, included in the content ID.
    /// Its storage instance remains the original instance after a cold reopen.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_history: Option<PreparedStorageHistory>,
    pub id: [u8; 32],
    pub message_digest: [u8; 32],
    pub event_digest: [u8; 32],
    pub snapshot_digest: Option<[u8; 32]>,
    pub message_head: u64,
    pub message_next: u64,
    pub event_head: u64,
    pub event_next: u64,
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
    pub fn valid_version(&self) -> bool {
        matches!(
            (self.version, self.storage_history.is_some()),
            (1, false) | (2, true)
        ) && match self.log_layout {
            QueueLogLayout::Separate => true,
            QueueLogLayout::Combined => {
                cfg!(feature = "combined-queue-log-experiment")
                    && self.version == 2
                    && self.message_head == 0
                    && self.message_next == 0
                    && self.message_digest == empty_log_digest()
                    && self.storage_history.as_ref().is_some_and(|h| !h.stream)
            }
        }
    }

    /// Bounds of the physical records carrying payloads, including non-payload
    /// positions in the combined layout. This is not a count of live messages.
    pub fn payload_bounds(&self) -> (u64, u64) {
        match self.log_layout {
            QueueLogLayout::Separate => (self.message_head, self.message_next),
            QueueLogLayout::Combined => (self.event_head, self.event_next),
        }
    }
}

fn empty_log_digest() -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"fibril-retained-log-v1\0");
    hash.update(&0u64.to_be_bytes());
    hash.update(&0u64.to_be_bytes());
    *hash.finalize().as_bytes()
}

fn log_digest(log: &Keratin) -> Result<[u8; 32]> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"fibril-retained-log-v1\0");
    let mut next = log.head_offset();
    let end = log.next_offset();
    hash.update(&next.to_be_bytes());
    hash.update(&end.to_be_bytes());
    let mut cursor = log.retained_durable_cursor(log.current_epoch(), next, end, u64::MAX).map_err(io_err)?;
    while let Some(record) = cursor.next_record(keratin_log::MAX_FROZEN_RECORD_BYTES).map_err(io_err)? {
        hash.update(&record.offset.to_be_bytes());
        hash.update(&record.flags.to_be_bytes());
        hash.update(&(record.headers.len() as u64).to_be_bytes());
        hash.update(record.headers);
        hash.update(&(record.payload.len() as u64).to_be_bytes());
        hash.update(record.payload);
        next += 1;
    }
    if next != end { return Err(StromaError::Corruption("sealed log ended before durable boundary".into())); }
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
    pub(super) fn recovery_snapshot_path(&self, topic: &str, part: u32, group: Option<&str>) -> Result<PathBuf> {
        let root = self.snap_dir(topic, part, group);
        let receipt = read_receipt(&root.join("recovery.history"))?
            .ok_or_else(|| StromaError::Corruption("sealed snapshot has no receipt".into()))?;
        Ok(if receipt.checkpoint_snapshot { root.join("recovery.snapshot") } else { self.snap_file(topic, part, group) })
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
            || (receipt.history.log_layout == QueueLogLayout::Combined) != (!stream && self.queue_payloads_in_event_log())
            || receipt.history.storage_history
                != self.durable_storage_history_receipt(topic, part, group)?
            || receipt.history.message_head > receipt.history.message_next
            || receipt.history.event_head > receipt.history.event_next
        {
            return Err(StromaError::InvalidArgument(
                "recovery read does not match history receipt".into(),
            ));
        }
        let mut history = receipt.history;
        history.id = [0; 32];
        let mut hash = blake3::Hasher::new();
        hash.update(b"fibril-retained-history-v1\0");
        hash.update(
            &rmp_serde::to_vec_named(&(topic, part, group, stream, &history))
                .map_err(encode_err)?,
        );
        if hash.finalize().as_bytes() != &expected_id {
            return Err(StromaError::Corruption(
                "retained-history identity mismatch".into(),
            ));
        }
        history.id = expected_id;
        Ok(history)
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
        let agreed = match (stream, messages) {
            (false, Some(messages)) => self.agreed_recovery_snapshot(topic, part, group, storage_history.as_ref(), messages, events)?,
            _ => None,
        };
        let checkpoint_snapshot = agreed.is_some();
        let snapshot = if let Some(bytes) = agreed {
            let root = self.snap_dir(topic, part, group);
            let target = root.join("recovery.snapshot");
            let temporary = root.join("recovery.snapshot.pending");
            let mut file = fs::OpenOptions::new().create(true).truncate(true).write(true).open(&temporary).map_err(io_err)?;
            file.write_all(&bytes).map_err(io_err)?;
            file.sync_all().map_err(io_err)?;
            drop(file);
            fs::rename(&temporary, &target).map_err(io_err)?;
            recovery_seal::sync_directories(&root)?;
            target
        } else { self.snap_file(topic, part, group) };
        // Validate the selected envelope and bind its exact bytes to the seal.
        let snapshot_digest = match self.read_queue_snapshot(&snapshot)? {
            Some(_) => Some(*blake3::hash(&fs::read(snapshot).map_err(io_err)?).as_bytes()),
            None => None,
        };
        let mut history = RetainedHistoryIdentity {
            log_layout: if messages.is_some() { QueueLogLayout::Separate } else { QueueLogLayout::Combined },
            version: if storage_history.is_some() { 2 } else { 1 },
            storage_history,
            id: [0; 32],
            message_digest: messages.map(log_digest).transpose()?.unwrap_or_else(empty_log_digest),
            event_digest: log_digest(events)?,
            snapshot_digest,
            message_head: messages.map_or(0, Keratin::head_offset),
            message_next: messages.map_or(0, Keratin::next_offset),
            event_head: events.head_offset(),
            event_next: events.next_offset(),
        };
        if !history.valid_version() {
            return Err(StromaError::InvalidArgument("unsupported retained history layout or binding".into()));
        }
        let mut hash = blake3::Hasher::new();
        hash.update(b"fibril-retained-history-v1\0");
        hash.update(
            &rmp_serde::to_vec_named(&(topic, part, group, stream, &history))
                .map_err(encode_err)?,
        );
        history.id = *hash.finalize().as_bytes();
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

    #[test]
    fn recovery_layout_is_explicit_and_bound_to_serialized_identity() {
        let mut history = RetainedHistoryIdentity {
            log_layout: QueueLogLayout::Separate,
            version: 2,
            storage_history: Some(PreparedStorageHistory {
                topic: "q".into(), partition: 0, group: None, stream: false,
                binding: StorageHistoryBinding {
                    resource_incarnation: [1; 16], accepted_history: [2; 16],
                    writer_session: [3; 16],
                },
                storage_instance: [4; 16],
            }),
            id: [0; 32], message_digest: empty_log_digest(), event_digest: [5; 32],
            snapshot_digest: None, message_head: 0, message_next: 0,
            event_head: 2, event_next: 9,
        };
        let separate = rmp_serde::to_vec_named(&history).unwrap();
        let keys: std::collections::BTreeMap<String, serde::de::IgnoredAny> =
            rmp_serde::from_slice(&separate).unwrap();
        assert!(!keys.contains_key("log_layout"));
        assert_eq!(rmp_serde::from_slice::<RetainedHistoryIdentity>(&separate).unwrap(), history);
        assert!(history.valid_version());
        assert_eq!(history.payload_bounds(), (0, 0));

        history.log_layout = QueueLogLayout::Combined;
        let combined = rmp_serde::to_vec_named(&history).unwrap();
        assert_ne!(separate, combined);
        assert_eq!(rmp_serde::from_slice::<RetainedHistoryIdentity>(&combined).unwrap(), history);
        assert_eq!(history.payload_bounds(), (2, 9));
        assert_eq!(history.valid_version(), cfg!(feature = "combined-queue-log-experiment"));
        let mut unknown = combined;
        let start = unknown.windows(8).position(|b| b == b"combined").unwrap();
        unknown[start..start + 8].copy_from_slice(b"unknown!");
        assert!(rmp_serde::from_slice::<RetainedHistoryIdentity>(&unknown).is_err());

        history.message_next = 1;
        assert!(!history.valid_version());
        history.message_next = 0;
        history.message_digest = [8; 32];
        assert!(!history.valid_version());
        history.message_digest = empty_log_digest();
        history.storage_history.as_mut().unwrap().stream = true;
        assert!(!history.valid_version());
        history.storage_history = None;
        history.version = 1;
        assert!(!history.valid_version());
    }

    async fn populated(root: &Path, payload: &[u8]) -> Stroma {
        let s = Stroma::open(
            root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
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
            Some(ReplicatedMessageBatch {
                epoch: 7,
                first_offset: 0,
                records: vec![Message {
                    flags: 0,
                    headers: vec![],
                    payload: payload.to_vec(),
                }],
                durability: Some(KDurability::AfterFsync),
            }),
            None,
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
    async fn history_depends_on_contents_not_fence_or_local_append_time() {
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
        assert_eq!(x.history, y.history);
        assert_ne!(x.history.id, z.history.id);
        assert_eq!(x.history.event_digest, z.history.event_digest);
        assert_ne!(x.history.message_digest, z.history.message_digest);
        first.shutdown().await.unwrap();
        second.shutdown().await.unwrap();
        different.shutdown().await.unwrap();
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
            other.msg_tp_part_dir("q", 0, None).join(segment),
            first.msg_tp_part_dir("q", 0, None).join(segment),
        )
        .unwrap();
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
        assert_eq!(ticket.resolve().unwrap().msg_log().unwrap().current_epoch(), 7);
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
