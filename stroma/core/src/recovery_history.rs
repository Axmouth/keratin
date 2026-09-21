//! Content identity for frozen retained records, independent of promise epochs.
//! Exact identity is useful for retries; it does not establish ancestry, a
//! committed prefix, or compatibility across different compaction boundaries.

use super::*;
use std::io::Write;

const MAGIC: &[u8; 8] = b"RHIST\0\0\x01";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetainedHistoryIdentity {
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
}

impl RetainedHistoryIdentity {
    pub fn valid_version(&self) -> bool {
        matches!(
            (self.version, self.storage_history.is_some()),
            (1, false) | (2, true)
        )
    }
}

fn log_digest(log: &Keratin) -> Result<[u8; 32]> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"fibril-retained-log-v1\0");
    let mut next = log.head_offset();
    let end = log.next_offset();
    hash.update(&next.to_be_bytes());
    hash.update(&end.to_be_bytes());
    let reader = log.reader();
    while next < end {
        // Read disk rather than a cache that could hide damage since sealing.
        let records = reader.scan_from_disk(next, 64).map_err(io_err)?;
        if records.is_empty() {
            return Err(StromaError::Corruption(format!(
                "sealed log missing offset {next}"
            )));
        }
        for record in records {
            if record.offset != next || next >= end {
                return Err(StromaError::Corruption(format!(
                    "sealed log discontinuity at {next}"
                )));
            }
            hash.update(&record.offset.to_be_bytes());
            hash.update(&record.flags.to_be_bytes());
            // Storage append timestamps differ between replicas. Identity covers
            // the replicated record, including its application headers.
            hash.update(&(record.headers.len() as u64).to_be_bytes());
            hash.update(&record.headers);
            hash.update(&(record.payload.len() as u64).to_be_bytes());
            hash.update(&record.payload);
            next += 1;
        }
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
        messages: &Keratin,
        events: &Keratin,
    ) -> Result<RetainedHistoryIdentity> {
        let path = self.snap_dir(topic, part, group).join("recovery.history");
        let stream = self.read_partition_kind(topic, part, group) == PartitionKind::Stream;
        let snapshot = self.snap_file(topic, part, group);
        // Validate the snapshot envelope as well as identifying its exact bytes.
        let snapshot_digest = match self.read_queue_snapshot(&snapshot)? {
            Some(_) => Some(*blake3::hash(&fs::read(snapshot).map_err(io_err)?).as_bytes()),
            None => None,
        };
        let storage_history = self.durable_storage_history_receipt(topic, part, group)?;
        let mut history = RetainedHistoryIdentity {
            version: if storage_history.is_some() { 2 } else { 1 },
            storage_history,
            id: [0; 32],
            message_digest: log_digest(messages)?,
            event_digest: log_digest(events)?,
            snapshot_digest,
            message_head: messages.head_offset(),
            message_next: messages.next_offset(),
            event_head: events.head_offset(),
            event_next: events.next_offset(),
        };
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
        assert_eq!(ticket.resolve().unwrap().msg_log().current_epoch(), 7);
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
