//! Explicit storage binding for a fresh history and its initial writer session.
//! Consensus authorization belongs to the caller. Existing data needs a verified
//! baseline; a new label must never turn it into an accepted history.

use super::*;
use std::io::{Read, Write};

const MAGIC: &[u8; 8] = b"SHIST\0\0\x01";
const MAX_BYTES: u64 = 65_536;

/// IDs from an authorized initial-history decision. This local receipt alone
/// proves neither a write quorum nor authority to replace an existing history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageHistoryBinding {
    pub resource_incarnation: [u8; 16],
    pub accepted_history: [u8; 16],
    pub writer_session: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    topic: String,
    partition: u32,
    group: Option<String>,
    stream: bool,
    binding: StorageHistoryBinding,
    // Private to this Stroma instance; callers cannot replay this across restart.
    storage_session: [u8; 16],
}

fn validate(binding: &StorageHistoryBinding) -> Result<()> {
    if [
        binding.resource_incarnation,
        binding.accepted_history,
        binding.writer_session,
    ]
    .contains(&[0; 16])
    {
        return Err(StromaError::InvalidArgument(
            "history IDs must be nonzero".into(),
        ));
    }
    Ok(())
}

fn read(path: &Path) -> Result<Option<Receipt>> {
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io_err(e)),
    };
    let mut bytes = Vec::new();
    file.take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(io_err)?;
    if bytes.len() < 12 || bytes.len() as u64 > MAX_BYTES || &bytes[..8] != MAGIC {
        return Err(StromaError::Corruption(
            "invalid storage history header or size".into(),
        ));
    }
    let end = bytes.len() - 4;
    if crc32c::crc32c(&bytes[..end]) != u32::from_be_bytes(bytes[end..].try_into().unwrap()) {
        return Err(StromaError::Corruption(
            "storage history checksum mismatch".into(),
        ));
    }
    let receipt: Receipt = rmp_serde::from_slice(&bytes[8..end])
        .map_err(|e| StromaError::Corruption(format!("invalid storage history: {e}")))?;
    validate(&receipt.binding).map_err(|e| StromaError::Corruption(e.to_string()))?;
    if receipt.storage_session == [0; 16] {
        return Err(StromaError::Corruption("invalid storage session".into()));
    }
    Ok(Some(receipt))
}

fn persist(path: &Path, receipt: &Receipt) -> Result<()> {
    let parent = path.parent().expect("receipt parent");
    fs::create_dir_all(parent).map_err(io_err)?;
    if let Some(existing) = read(path)? {
        if existing != *receipt {
            return Err(StromaError::InvalidArgument(
                "conflicting storage history binding".into(),
            ));
        }
    } else {
        let mut bytes = MAGIC.to_vec();
        bytes.extend(rmp_serde::to_vec_named(receipt).map_err(encode_err)?);
        bytes.extend(crc32c::crc32c(&bytes).to_be_bytes());
        if bytes.len() as u64 > MAX_BYTES {
            return Err(StromaError::InvalidArgument(
                "storage history exceeds size limit".into(),
            ));
        }
        let temp = parent.join(format!("storage.history.{}.pending", uuid::Uuid::now_v7()));
        let result = (|| {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)
                .map_err(io_err)?;
            file.write_all(&bytes).map_err(io_err)?;
            file.sync_all().map_err(io_err)?;
            // Atomic create, never overwrite another initializer's receipt.
            fs::hard_link(&temp, path).map_err(io_err)
        })();
        let _ = fs::remove_file(&temp);
        result?;
    }
    fs::File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(io_err)?;
    recovery_seal::sync_directories(parent)
}

impl Stroma {
    fn storage_history_path(&self, topic: &str, part: u32, group: Option<&str>) -> PathBuf {
        self.snap_dir(topic, part, group).join("storage.history")
    }

    fn checked_storage_history(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Option<Receipt>> {
        let receipt = read(&self.storage_history_path(topic, part, group))?;
        if let Some(receipt) = &receipt {
            if receipt.topic != topic
                || receipt.partition != part
                || receipt.group.as_deref() != group
            {
                return Err(StromaError::Corruption(
                    "storage history resource mismatch".into(),
                ));
            }
            let expected = if receipt.stream {
                PartitionKind::Stream
            } else {
                PartitionKind::Queue
            };
            // Bound resources always have an explicit marker. Missing/corrupt
            // markers must not silently reinterpret a stream as a queue.
            if self.partition_kind_marker(topic, part, group) != Some(expected) {
                return Err(StromaError::Corruption(
                    "storage history kind mismatch".into(),
                ));
            }
        }
        Ok(receipt)
    }

    /// Read the durable binding without opening logs or granting writer admission.
    pub fn storage_history_binding(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Option<StorageHistoryBinding>> {
        self.checked_storage_history(topic, part, normalize_group(group))
            .map(|r| r.map(|r| r.binding))
    }

    pub(super) fn ensure_storage_history_admitted(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<()> {
        let group = normalize_group(group);
        let key = (Box::<str>::from(topic), part, group.map(Box::<str>::from));
        let receipt = self.checked_storage_history(topic, part, group)?;
        match receipt {
            Some(receipt)
                if receipt.storage_session == self.storage_session
                    && self
                        .admitted_histories
                        .get(&key)
                        .is_some_and(|b| *b == receipt.binding) =>
            {
                Ok(())
            }
            Some(_) => Err(StromaError::HistoryAdmissionRequired {
                topic: topic.into(),
                partition: part,
                group: group.map(str::to_owned),
            }),
            None if self.admitted_histories.contains_key(&key) => Err(StromaError::Corruption(
                "admitted storage history receipt is missing".into(),
            )),
            None => Ok(()), // Legacy/unbound operation; no accepted-history claim.
        }
    }

    pub(super) fn require_unbound_storage_history(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<()> {
        if self
            .checked_storage_history(topic, part, normalize_group(group))?
            .is_some()
        {
            return Err(StromaError::HistoryAdmissionRequired {
                topic: topic.into(),
                partition: part,
                group: normalize_group(group).map(str::to_owned),
            });
        }
        Ok(())
    }

    /// Explicitly bind previously nonexistent storage before its first write.
    /// Call only after consensus authorizes this resource/history/writer session.
    /// This is not automatically enabled by the broker. Retries within this
    /// storage instance are idempotent; another instance requires recovery even
    /// if it supplies exactly the same IDs and assignment epoch.
    ///
    /// Any preexisting partition directory is unverified, even if empty or fully
    /// compacted. There is no replacement/reset API: installing another history
    /// must eventually carry its validated baseline and quorum authorization.
    pub async fn initialize_empty_storage_history(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        kind: PartitionKind,
        binding: StorageHistoryBinding,
    ) -> Result<()> {
        if !cfg!(unix) {
            return Err(StromaError::Unsupported(
                "durable storage history requires directory sync".into(),
            ));
        }
        validate(&binding)?;
        let group = normalize_group(group).map(str::to_owned);
        if kind == PartitionKind::Stream && group.is_some() {
            return Err(StromaError::InvalidArgument(
                "stream history cannot have a group".into(),
            ));
        }
        let stroma = self.clone();
        let topic = topic.to_owned();
        // Own all admitted work and its lifecycle lock through persistence and
        // log shutdown, even if the caller drops its future.
        tokio::spawn(async move {
            stroma
                .initialize_history_inner(&topic, part, group.as_deref(), kind, binding)
                .await
        })
        .await
        .map_err(io_err)?
    }

    async fn initialize_history_inner(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        kind: PartitionKind,
        binding: StorageHistoryBinding,
    ) -> Result<()> {
        let _lifecycle = self.lock_partition_lifecycle(topic, part, group).await;
        let path = self.storage_history_path(topic, part, group);
        let receipt = Receipt {
            topic: topic.into(),
            partition: part,
            group: group.map(str::to_owned),
            stream: kind == PartitionKind::Stream,
            binding: binding.clone(),
            storage_session: self.storage_session,
        };
        if let Some(existing) = self.checked_storage_history(topic, part, group)? {
            if existing != receipt {
                return Err(StromaError::HistoryAdmissionRequired {
                    topic: topic.into(),
                    partition: part,
                    group: group.map(str::to_owned),
                });
            }
            let saved = receipt.clone();
            tokio::task::spawn_blocking(move || persist(&path, &saved))
                .await
                .map_err(io_err)??;
        } else {
            let live = {
                let registry = self.queue_handles.load();
                slot_lookup_no_alloc(&registry, topic, part, group)
                    .is_some_and(|slot| slot.handle.get().is_some())
            };
            if live {
                return Err(StromaError::InvalidArgument(
                    "existing storage needs a verified history baseline".into(),
                ));
            }
            for dir in [
                self.msg_tp_part_dir(topic, part, group),
                self.tp_part_dir(topic, part, group),
                self.snap_dir(topic, part, group),
            ] {
                if dir.try_exists().map_err(io_err)? {
                    return Err(StromaError::InvalidArgument(
                        "existing storage needs a verified history baseline".into(),
                    ));
                }
            }
            // Take both Keratin locks before persisting. Another storage instance
            // cannot write these logs concurrently with initial binding.
            let messages = self.msg_log_init(topic, part, group).await?;
            let events = match self.event_log_init(topic, part, group).await {
                Ok(events) => events,
                Err(error) => {
                    let _ = messages.shutdown().await;
                    return Err(error);
                }
            };
            let result = async {
                if messages.next_offset() != 0
                    || messages.head_offset() != 0
                    || events.next_offset() != 0
                    || events.head_offset() != 0
                    || messages.current_epoch() != 0
                    || events.current_epoch() != 0
                {
                    return Err(StromaError::InvalidArgument(
                        "initial history logs are not pristine".into(),
                    ));
                }
                let stroma = self.clone();
                let saved = receipt.clone();
                tokio::task::spawn_blocking(move || {
                    stroma.write_partition_kind(
                        &saved.topic,
                        saved.partition,
                        saved.group.as_deref(),
                        kind,
                    )?;
                    persist(&path, &saved)
                })
                .await
                .map_err(io_err)?
            }
            .await;
            let (message_close, event_close) = tokio::join!(messages.shutdown(), events.shutdown());
            result?;
            message_close.map_err(io_err)?;
            event_close.map_err(io_err)?;
        }
        // Admission is published only after successful fsync, directory sync and
        // log shutdown. A visible file after an I/O failure is insufficient.
        self.admitted_histories
            .insert((topic.into(), part, group.map(Into::into)), binding);
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn binding() -> StorageHistoryBinding {
        StorageHistoryBinding {
            resource_incarnation: [1; 16],
            accepted_history: [2; 16],
            writer_session: [3; 16],
        }
    }

    async fn open(root: &Path) -> Stroma {
        Stroma::open(
            root,
            StromaKeratinConfig::from_message_log(KeratinConfig::default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap()
    }

    async fn initialize(stroma: &Stroma) -> Result<()> {
        stroma
            .initialize_empty_storage_history("q", 0, None, PartitionKind::Queue, binding())
            .await
    }

    #[tokio::test]
    async fn restart_same_epoch_cannot_reuse_bound_writer_but_can_seal() {
        let dir = keratin_log::test_dir!("history_restart");
        let stroma = open(&dir.root).await;
        initialize(&stroma).await.unwrap();
        initialize(&stroma.clone()).await.unwrap();
        stroma
            .become_queue_follower_with_epoch("q", 0, None, 7)
            .await
            .unwrap();
        stroma
            .apply_replicated_queue_batch(
                "q",
                0,
                None,
                Some(ReplicatedMessageBatch {
                    epoch: 7,
                    first_offset: 0,
                    durability: Some(KDurability::AfterFsync),
                    records: vec![Message {
                        flags: 0,
                        headers: vec![],
                        payload: b"retained".to_vec(),
                    }],
                }),
                Some(ReplicatedEventBatch {
                    epoch: 7,
                    first_offset: 0,
                    durability: Some(KDurability::AfterFsync),
                    events: vec![StromaEvent::EnqueueMany {
                        reqs: vec![EnqueueEventMeta {
                            off: 0,
                            retries: 0,
                            expire_at: None,
                        }],
                    }],
                }),
            )
            .await
            .unwrap();
        stroma
            .become_queue_owner_with_epoch("q", 0, None, 7)
            .await
            .unwrap();
        let receipt = fs::read(stroma.storage_history_path("q", 0, None)).unwrap();
        stroma.shutdown().await.unwrap();
        drop(stroma);
        let reopened = open(&dir.root).await;
        assert_eq!(
            reopened.storage_history_binding("q", 0, None).unwrap(),
            Some(binding())
        );
        assert!(matches!(
            initialize(&reopened).await,
            Err(StromaError::HistoryAdmissionRequired { .. })
        ));
        for epoch in [7, 8] {
            assert!(matches!(
                reopened
                    .ensure_queue_owner_epoch("q", 0, None, Some(epoch))
                    .await,
                Err(StromaError::HistoryAdmissionRequired { .. })
            ));
            assert!(matches!(
                reopened
                    .become_queue_owner_with_epoch("q", 0, None, epoch)
                    .await,
                Err(StromaError::HistoryAdmissionRequired { .. })
            ));
        }
        assert_eq!(
            fs::read(reopened.storage_history_path("q", 0, None)).unwrap(),
            receipt
        );
        let sealed = reopened
            .seal_replica_for_recovery(
                "q",
                0,
                None,
                RecoverySealRequest {
                    transition: [9; 32],
                    fence_epoch: 8,
                },
            )
            .await
            .unwrap();
        assert_eq!((sealed.message_next, sealed.event_next), (1, 1));
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn crash_child() {
        let Some(root) = std::env::var_os("STROMA_HISTORY_CRASH_CHILD_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let stroma = open(&root).await;
        initialize(&stroma).await.unwrap();
        stroma
            .ensure_queue_owner_epoch("q", 0, None, Some(7))
            .await
            .unwrap();
        let (completion, rx) = KeratinAppendCompletion::pair();
        let headers = MessageHeaders {
            published: 0,
            publish_received: 0,
            content_type: None,
            extra: Default::default(),
        };
        stroma
            .append_message("q", 0, None, &headers, b"before-crash".to_vec(), completion)
            .await
            .unwrap();
        rx.await.unwrap().unwrap();
        fs::write(root.join("crash.ready"), b"ready").unwrap();
        std::future::pending::<()>().await;
    }

    #[tokio::test]
    async fn sigkill_preserves_binding_and_blocks_same_epoch_writer() {
        let dir = keratin_log::test_dir!("history_sigkill");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "stroma::storage_history::tests::crash_child",
                "--nocapture",
            ])
            .env("STROMA_HISTORY_CRASH_CHILD_ROOT", &dir.root)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let reached = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if dir.root.join("crash.ready").exists() {
                    return true;
                }
                if child.try_wait().unwrap().is_some() {
                    return false;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        let _ = child.kill();
        child.wait().unwrap();
        assert!(
            matches!(reached, Ok(true)),
            "child must finish its durable write"
        );
        let reopened = open(&dir.root).await;
        assert_eq!(
            reopened.storage_history_binding("q", 0, None).unwrap(),
            Some(binding())
        );
        assert!(matches!(
            reopened
                .ensure_queue_owner_epoch("q", 0, None, Some(7))
                .await,
            Err(StromaError::HistoryAdmissionRequired { .. })
        ));
        let sealed = reopened
            .seal_replica_for_recovery(
                "q",
                0,
                None,
                RecoverySealRequest {
                    transition: [9; 32],
                    fence_epoch: 8,
                },
            )
            .await
            .unwrap();
        assert_eq!(sealed.message_next, 1);
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn changed_history_or_session_cannot_relabel_existing_storage() {
        let dir = keratin_log::test_dir!("history_relabel");
        let stroma = open(&dir.root).await;
        initialize(&stroma).await.unwrap();
        let before = fs::read(stroma.storage_history_path("q", 0, None)).unwrap();
        for index in 0..3 {
            let mut different = binding();
            match index {
                0 => different.resource_incarnation = [4; 16],
                1 => different.accepted_history = [4; 16],
                _ => different.writer_session = [4; 16],
            }
            assert!(matches!(
                stroma
                    .initialize_empty_storage_history("q", 0, None, PartitionKind::Queue, different)
                    .await,
                Err(StromaError::HistoryAdmissionRequired { .. })
            ));
            assert_eq!(
                fs::read(stroma.storage_history_path("q", 0, None)).unwrap(),
                before
            );
        }
        stroma.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn competing_initializations_keep_one_binding() {
        let dir = keratin_log::test_dir!("history_competing");
        let stroma = open(&dir.root).await;
        let mut other = binding();
        other.resource_incarnation = [8; 16];
        let (left, right) = tokio::join!(
            initialize(&stroma),
            stroma.initialize_empty_storage_history(
                "q",
                0,
                None,
                PartitionKind::Queue,
                other.clone()
            )
        );
        assert_ne!(left.is_ok(), right.is_ok());
        let chosen = stroma
            .storage_history_binding("q", 0, None)
            .unwrap()
            .unwrap();
        assert_eq!(chosen, if left.is_ok() { binding() } else { other });
        stroma
            .ensure_queue_owner_epoch("q", 0, None, Some(7))
            .await
            .unwrap();
        stroma.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn empty_existing_storage_is_not_a_fresh_history() {
        for materialized in [false, true] {
            let dir = keratin_log::test_dir!("history_legacy");
            let stroma = open(&dir.root).await;
            if materialized {
                stroma.queue_handle("q", 0, None).await.unwrap();
            } else {
                fs::create_dir_all(stroma.msg_tp_part_dir("q", 0, None)).unwrap();
            }
            assert!(initialize(&stroma).await.is_err());
            assert_eq!(stroma.storage_history_binding("q", 0, None).unwrap(), None);
            stroma.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn cancellation_keeps_admitted_initialization_owned() {
        let dir = keratin_log::test_dir!("history_cancel");
        let stroma = open(&dir.root).await;
        let guard = stroma.lock_partition_lifecycle("q", 0, None).await;
        let mut future = Box::pin(initialize(&stroma));
        assert!(futures::poll!(&mut future).is_pending());
        drop(future);
        drop(guard);
        tokio::time::timeout(Duration::from_secs(10), async {
            while stroma.admitted_histories.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        stroma
            .ensure_queue_owner_epoch("q", 0, None, Some(7))
            .await
            .unwrap();
        initialize(&stroma).await.unwrap();
        stroma.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn visible_receipt_without_completed_admission_requires_retry() {
        let dir = keratin_log::test_dir!("history_unfinished_sync");
        let stroma = open(&dir.root).await;
        stroma
            .write_partition_kind("q", 0, None, PartitionKind::Queue)
            .unwrap();
        let receipt = Receipt {
            topic: "q".into(),
            partition: 0,
            group: None,
            stream: false,
            binding: binding(),
            storage_session: stroma.storage_session,
        };
        // Simulates a visible receipt before the admitted operation reported
        // successful directory sync/log shutdown. File presence is insufficient.
        persist(&stroma.storage_history_path("q", 0, None), &receipt).unwrap();
        assert!(matches!(
            stroma.queue_handle("q", 0, None).await,
            Err(StromaError::HistoryAdmissionRequired { .. })
        ));
        initialize(&stroma).await.unwrap();
        stroma.queue_handle("q", 0, None).await.unwrap();
        stroma.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn corruption_and_cross_resource_receipts_fail_closed() {
        let dir = keratin_log::test_dir!("history_corruption");
        let stroma = open(&dir.root).await;
        initialize(&stroma).await.unwrap();
        let path = stroma.storage_history_path("q", 0, None);
        let original = fs::read(&path).unwrap();
        for bytes in [
            vec![],
            original[..10].to_vec(),
            vec![0; MAX_BYTES as usize + 1],
            {
                let mut bytes = original.clone();
                bytes[20] ^= 1;
                bytes
            },
        ] {
            fs::write(&path, bytes).unwrap();
            assert!(matches!(
                stroma.queue_handle("q", 0, None).await,
                Err(StromaError::Corruption(_))
            ));
        }
        fs::write(&path, &original).unwrap();
        stroma
            .write_partition_kind("other", 0, None, PartitionKind::Queue)
            .unwrap();
        fs::write(stroma.storage_history_path("other", 0, None), &original).unwrap();
        assert!(matches!(
            stroma.queue_handle("other", 0, None).await,
            Err(StromaError::Corruption(_))
        ));
        fs::remove_file(stroma.kind_marker_file("q", 0, None)).unwrap();
        assert!(matches!(
            stroma.queue_handle("q", 0, None).await,
            Err(StromaError::Corruption(_))
        ));
        stroma.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_replacement_cannot_change_bound_history() {
        let dir = keratin_log::test_dir!("history_checkpoint");
        let stroma = open(&dir.root).await;
        initialize(&stroma).await.unwrap();
        stroma
            .become_queue_follower_with_epoch("q", 0, None, 7)
            .await
            .unwrap();
        let state = crate::state::QueueInternalState::new("q".into(), 0);
        let install = FollowerStateCheckpointInstall {
            message_epoch: 7,
            event_epoch: 7,
            message_next_offset: 0,
            event_next_offset: 0,
            applied_event_offset: 0,
            state_snapshot: state.encode_snapshot(0),
        };
        assert!(matches!(
            stroma
                .install_follower_state_checkpoint("q", 0, None, install)
                .await,
            Err(StromaError::HistoryAdmissionRequired { .. })
        ));
        assert!(
            !stroma
                .snap_dir("q", 0, None)
                .join("checkpoint.install")
                .exists()
        );
        assert_eq!(
            stroma.storage_history_binding("q", 0, None).unwrap(),
            Some(binding())
        );
        stroma.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stream_binding_preserves_kind_across_restart() {
        let dir = keratin_log::test_dir!("history_stream");
        let stroma = open(&dir.root).await;
        stroma
            .initialize_empty_storage_history("s", 0, None, PartitionKind::Stream, binding())
            .await
            .unwrap();
        assert_eq!(stroma.partition_kind("s", 0, None), PartitionKind::Stream);
        stroma.create_stream("s", 0, None).await.unwrap();
        stroma.shutdown().await.unwrap();
        drop(stroma);
        let reopened = open(&dir.root).await;
        assert!(matches!(
            reopened.create_stream("s", 0, None).await,
            Err(StromaError::HistoryAdmissionRequired { .. })
        ));
        assert_eq!(reopened.partition_kind("s", 0, None), PartitionKind::Stream);
        reopened.shutdown().await.unwrap();
    }
}
