//! Durable local fencing for the forthcoming coordinated recovery protocol.
//!
//! A seal preserves evidence; it does not certify a history or grant ownership.
//! Only a coordinator-validated pending transition may authorize this API.

use super::*;
use std::io::Write;

const MAGIC: &[u8; 8] = b"RSEAL\0\0\x01";

/// Identifies the exact committed transition requesting a replica seal.
/// Authentication and old-replica membership validation belong to the caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoverySealRequest {
    /// Hash of the complete persisted transition, including old/new policies
    /// and replica sets. This is not an installed-data-history identifier.
    pub transition: [u8; 32],
    pub fence_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SealIntent {
    topic: String,
    partition: u32,
    group: Option<String>,
    request: RecoverySealRequest,
    // These are fencing epochs only. Do not use them to rank data histories.
    previous_message_epoch: u64,
    previous_event_epoch: u64,
}

/// Stable local durable log bounds after sealing both writers. These bounds
/// include potentially incomplete/unconfirmed work. They are not a promotion
/// certificate: history, checkpoint and payload/event validation must follow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedReplicaFrontiers {
    pub request: RecoverySealRequest,
    pub history: RetainedHistoryIdentity,
    pub message_head: u64,
    pub message_next: u64,
    pub event_head: u64,
    pub event_next: u64,
}

fn read_intent(path: &Path) -> Result<Option<SealIntent>> {
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
            "invalid recovery seal header".into(),
        ));
    }
    let end = bytes.len() - 4;
    let crc = u32::from_be_bytes(bytes[end..].try_into().unwrap());
    if crc32c::crc32c(&bytes[..end]) != crc {
        return Err(StromaError::Corruption(
            "recovery seal checksum mismatch".into(),
        ));
    }
    let intent: SealIntent = rmp_serde::from_slice(&bytes[8..end])
        .map_err(|err| StromaError::Corruption(format!("invalid recovery seal: {err}")))?;
    if intent.request.fence_epoch <= intent.previous_message_epoch
        || intent.request.fence_epoch <= intent.previous_event_epoch
    {
        return Err(StromaError::Corruption(
            "recovery seal does not advance both epochs".into(),
        ));
    }
    Ok(Some(intent))
}

// Sync the whole ancestor chain: the snapshot path (or storage root itself)
// may have been created without a durable parent entry. Retrying this also
// completes a previous rename whose directory sync failed.
#[cfg(unix)]
pub(super) fn sync_directories(path: &Path) -> Result<()> {
    for ancestor in fs::canonicalize(path).map_err(io_err)?.ancestors() {
        fs::File::open(ancestor)
            .and_then(|file| file.sync_all())
            .map_err(io_err)?;
    }
    Ok(())
}

#[cfg(not(unix))]
pub(super) fn sync_directories(_path: &Path) -> Result<()> {
    Err(StromaError::Unsupported(
        "durable recovery seals require directory-sync support on this platform".into(),
    ))
}

fn persist_intent(path: &Path, intent: &SealIntent) -> Result<()> {
    let parent = path.parent().expect("seal path has a parent");
    fs::create_dir_all(parent).map_err(io_err)?;
    if let Some(existing) = read_intent(path)? {
        if &existing != intent {
            return Err(StromaError::InvalidArgument(
                "conflicting recovery seal".into(),
            ));
        }
        fs::File::open(path)
            .and_then(|file| file.sync_all())
            .map_err(io_err)?;
        return sync_directories(parent);
    }
    let mut bytes = MAGIC.to_vec();
    bytes.extend(rmp_serde::to_vec_named(intent).map_err(encode_err)?);
    bytes.extend(crc32c::crc32c(&bytes).to_be_bytes());
    let temp = parent.join(format!("recovery.seal.{}.pending", uuid::Uuid::now_v7()));
    let result = (|| {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .map_err(io_err)?;
        file.write_all(&bytes).map_err(io_err)?;
        file.sync_all().map_err(io_err)?;
        fs::rename(&temp, path).map_err(io_err)?;
        sync_directories(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}

impl Stroma {
    pub(super) fn recovery_seal_path(&self, topic: &str, part: u32, group: Option<&str>) -> PathBuf {
        self.snap_dir(topic, part, group).join("recovery.seal")
    }

    pub(super) fn require_matching_recovery_seal(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        request: &RecoverySealRequest,
    ) -> Result<()> {
        let intent =
            read_intent(&self.recovery_seal_path(topic, part, group))?.ok_or_else(|| {
                StromaError::InvalidArgument("replica has no durable recovery seal".into())
            })?;
        if intent.topic != topic
            || intent.partition != part
            || intent.group.as_deref() != group
            || &intent.request != request
        {
            return Err(StromaError::InvalidArgument(
                "recovery read does not match durable seal".into(),
            ));
        }
        Ok(())
    }

    // Ordinary lifecycle mutation also requires admission to a bound history.
    // Explicit recovery sealing bypasses writer admission so evidence remains accessible.
    pub(super) fn ensure_partition_not_sealed(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<()> {
        if read_intent(&self.recovery_seal_path(topic, part, group))?.is_some() {
            return Err(StromaError::RecoverySealed {
                topic: topic.to_owned(),
                partition: part,
                group: normalize_group(group).map(str::to_owned),
            });
        }
        self.ensure_storage_history_admitted(topic, part, group)
    }

    /// Seal a replica for a committed recovery transition. Retries must provide
    /// the identical request. Once started, caller cancellation does not cancel
    /// the local operation. A failed attempt never returns usable evidence.
    ///
    /// Ordinary admission, checkpoint replacement, compaction and startup
    /// replay cannot reopen a sealed partition. There is deliberately no public
    /// unseal method: release requires the future installation/activation proof.
    /// This primitive is not yet wired into automatic broker failover.
    pub async fn seal_replica_for_recovery(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        request: RecoverySealRequest,
    ) -> Result<SealedReplicaFrontiers> {
        self.seal_replica_for_recovery_checked(
            topic,
            part,
            group,
            self.read_partition_kind(topic, part, group),
            request,
        )
        .await
    }

    /// Bind the protocol resource kind under the lifecycle lock, before any
    /// file is opened or fenced. Queues and streams share storage paths.
    pub async fn seal_replica_for_recovery_checked(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        expected_kind: PartitionKind,
        request: RecoverySealRequest,
    ) -> Result<SealedReplicaFrontiers> {
        if !cfg!(unix) {
            return Err(StromaError::Unsupported(
                "durable recovery seals require directory-sync support on this platform".into(),
            ));
        }
        let stroma = self.clone();
        let topic = topic.to_owned();
        let group = normalize_group(group).map(str::to_owned);
        tokio::spawn(async move {
            stroma
                .seal_replica_inner(&topic, part, group.as_deref(), expected_kind, request)
                .await
        })
        .await
        .map_err(io_err)?
    }

    async fn seal_replica_inner(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        expected_kind: PartitionKind,
        request: RecoverySealRequest,
    ) -> Result<SealedReplicaFrontiers> {
        let _lifecycle = self.lock_partition_lifecycle(topic, part, group).await;
        let actual = self.read_partition_kind(topic, part, group);
        if actual != expected_kind {
            return Err(StromaError::WrongPartitionKind {
                expected: expected_kind,
                actual,
            });
        }
        self.ensure_checkpoint_not_pending(topic, part, group)?;
        let path = self.recovery_seal_path(topic, part, group);
        let previous = read_intent(&path)?;
        if let Some(intent) = &previous {
            if intent.topic != topic
                || intent.partition != part
                || intent.group.as_deref() != group
                || intent.request != request
            {
                return Err(StromaError::InvalidArgument(
                    "conflicting recovery seal request".into(),
                ));
            }
        }
        let handle = {
            let current = self.queue_handles.load();
            slot_lookup_no_alloc(&current, topic, part, group)
                .and_then(|slot| slot.handle.get().cloned())
        };
        // Also serializes with snapshot/compaction and role changes. Drain the
        // owner separately: its leases span durable writes and actor application.
        let _apply = match &handle {
            Some(handle) => Some(handle.follower_apply_state().await),
            None => None,
        };
        let (messages, events) = match &handle {
            Some(handle) => (handle.msg_log(), handle.event_log()),
            None => {
                let messages = self.msg_log_init(topic, part, group).await?;
                match self.event_log_init(topic, part, group).await {
                    Ok(events) => (messages, events),
                    Err(err) => {
                        let _ = messages.shutdown().await;
                        return Err(err);
                    }
                }
            }
        };
        let result = async {
            let max_epoch = messages.current_epoch().max(events.current_epoch());
            if max_epoch > request.fence_epoch
                || (previous.is_none() && max_epoch == request.fence_epoch)
            {
                return Err(StromaError::InvalidArgument(
                    "recovery fence must advance both existing epochs".into(),
                ));
            }
            if let Some(handle) = &handle {
                handle.begin_recovery_seal();
                handle.quiesce_for_teardown().await;
            }
            let intent = previous.unwrap_or_else(|| SealIntent {
                topic: topic.to_owned(),
                partition: part,
                group: group.map(str::to_owned),
                request: request.clone(),
                previous_message_epoch: messages.current_epoch(),
                previous_event_epoch: events.current_epoch(),
            });
            if intent.previous_message_epoch >= request.fence_epoch
                || intent.previous_event_epoch >= request.fence_epoch
            {
                return Err(StromaError::InvalidArgument(
                    "recovery fence overtaken while draining".into(),
                ));
            }
            let io_handle = handle.clone();
            tokio::task::spawn_blocking(move || {
                let _snapshot_io = io_handle
                    .as_ref()
                    .map(|h| h.recovery_gate.snapshot_io.lock());
                persist_intent(&path, &intent)
            })
            .await
            .map_err(io_err)??;
            tracing::info!(topic, partition = part, group, fence_epoch = request.fence_epoch,
                transition = %blake3::Hash::from_bytes(request.transition),
                "recovery seal intent durable; fencing both logs");
            // The durable intent gates restart BEFORE either log fence changes.
            messages.freeze();
            events.freeze();
            let (message_result, event_result) = tokio::join!(
                messages.advance_epoch(request.fence_epoch),
                events.advance_epoch(request.fence_epoch),
            );
            message_result.map_err(io_err)?;
            event_result.map_err(io_err)?;
            let stroma = self.clone();
            let topic_owned = topic.to_owned();
            let group_owned = group.map(str::to_owned);
            let request_owned = request.clone();
            let message_copy = messages.clone();
            let event_copy = events.clone();
            let history = tokio::task::spawn_blocking(move || {
                stroma.persist_retained_history(
                    &topic_owned,
                    part,
                    group_owned.as_deref(),
                    request_owned,
                    &message_copy,
                    &event_copy,
                )
            })
            .await
            .map_err(io_err)??;
            tracing::info!(
                topic,
                partition = part,
                group,
                fence_epoch = request.fence_epoch,
                message_next = messages.next_offset(),
                event_next = events.next_offset(),
                history = %blake3::Hash::from_bytes(history.id),
                "replica sealed; local bounds require recovery history validation"
            );
            Ok(SealedReplicaFrontiers {
                request,
                history,
                message_head: messages.head_offset(),
                message_next: messages.next_offset(),
                event_head: events.head_offset(),
                event_next: events.next_offset(),
            })
        }
        .await;
        if handle.is_none() {
            let (message_close, event_close) = tokio::join!(messages.shutdown(), events.shutdown());
            message_close.map_err(io_err)?;
            event_close.map_err(io_err)?;
        }
        result
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::EnqueueEventMeta;
    use keratin_log::test_dir;

    fn request() -> RecoverySealRequest {
        RecoverySealRequest {
            transition: [19; 32],
            fence_epoch: 8,
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

    async fn populated(stroma: &Stroma) -> QueueHandle {
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
                        payload: b"confirmed".to_vec(),
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
        stroma.queue_handle("q", 0, None).await.unwrap()
    }

    #[tokio::test]
    async fn seal_retains_data_and_blocks_reopening_through_eviction_and_restart() {
        let dir = test_dir!("recovery_seal_restart");
        let stroma = open(&dir.root).await;
        let qh = populated(&stroma).await;
        let h = qh.resolve().unwrap();
        let sealed = stroma
            .seal_replica_for_recovery("q", 0, None, request())
            .await
            .unwrap();
        assert_eq!((sealed.message_next, sealed.event_next), (1, 1));
        assert_eq!(
            (h.msg_log().current_epoch(), h.event_log().current_epoch()),
            (8, 8)
        );
        h.become_owner();
        assert_eq!(h.role(), QueueRole::Frozen);
        assert!(h.begin_owner_operation().await.is_err());
        h.become_follower();
        assert_eq!(h.role(), QueueRole::Frozen);
        assert!(stroma.become_queue_owner("q", 0, None).await.is_err());
        assert!(stroma.become_queue_follower("q", 0, None).await.is_err());
        assert!(
            stroma
                .promote_queue_follower_to_local_tail("q", 0, None, 9)
                .await
                .is_err()
        );
        assert!(stroma.advance_queue_epoch("q", 0, None, 9).await.is_err());
        assert!(stroma.snapshot_partition("q", 0, None).await.is_err());
        assert!(
            stroma
                .truncate_messages_before("q", 0, None, 1)
                .await
                .is_err()
        );
        assert!(stroma.truncate_partition_log(qh.clone(), 1).await.is_err());
        assert!(stroma.destroy_partition("q", 0, None).await.is_err());
        assert_eq!(
            stroma
                .seal_replica_for_recovery("q", 0, None, request())
                .await
                .unwrap(),
            sealed
        );
        drop(h);
        assert_eq!(
            stroma.evict("q", 0, None).await.unwrap(),
            EvictOutcome::Evicted
        );
        assert!(stroma.materialize("q", 0, None).await.is_err());
        assert_eq!(
            stroma
                .seal_replica_for_recovery("q", 0, None, request())
                .await
                .unwrap(),
            sealed
        );
        stroma.shutdown().await.unwrap();
        drop(stroma);
        let reopened = open(&dir.root).await;
        assert!(matches!(
            reopened.queue_handle("q", 0, None).await,
            Err(StromaError::RecoverySealed { .. })
        ));
        assert!(
            reopened
                .become_queue_owner_with_epoch("q", 0, None, 9)
                .await
                .is_err()
        );
        assert_eq!(
            reopened
                .seal_replica_for_recovery("q", 0, None, request())
                .await
                .unwrap(),
            sealed
        );
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn seal_resumes_after_intent_or_only_one_log_fence_persisted() {
        for fence_message_log in [false, true] {
            let dir = test_dir!("recovery_seal_partial_fence");
            let stroma = open(&dir.root).await;
            let qh = populated(&stroma).await;
            let h = qh.resolve().unwrap();
            persist_intent(
                &stroma.recovery_seal_path("q", 0, None),
                &SealIntent {
                    topic: "q".into(),
                    partition: 0,
                    group: None,
                    request: request(),
                    previous_message_epoch: 7,
                    previous_event_epoch: 7,
                },
            )
            .unwrap();
            h.begin_recovery_seal();
            if fence_message_log {
                h.msg_log().advance_epoch(8).await.unwrap();
            }
            drop(h);
            stroma.shutdown().await.unwrap();
            drop(stroma);
            let reopened = open(&dir.root).await;
            assert!(reopened.queue_handle("q", 0, None).await.is_err());
            let sealed = reopened
                .seal_replica_for_recovery("q", 0, None, request())
                .await
                .unwrap();
            assert_eq!((sealed.message_next, sealed.event_next), (1, 1));
            let messages = reopened.msg_log_init("q", 0, None).await.unwrap();
            let events = reopened.event_log_init("q", 0, None).await.unwrap();
            assert_eq!((messages.current_epoch(), events.current_epoch()), (8, 8));
            messages.shutdown().await.unwrap();
            events.shutdown().await.unwrap();
            reopened.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn seal_caller_cancellation_drains_accepted_owner_work_and_finishes() {
        let dir = test_dir!("recovery_seal_cancel");
        let stroma = open(&dir.root).await;
        let qh = populated(&stroma).await;
        stroma.become_queue_owner("q", 0, None).await.unwrap();
        let h = qh.resolve().unwrap();
        let lease = h.begin_owner_operation().await.unwrap();
        let worker = stroma.clone();
        let task = tokio::spawn(async move {
            worker
                .seal_replica_for_recovery("q", 0, None, request())
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while h.role() != QueueRole::Frozen {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!task.is_finished());
        assert_eq!(h.msg_log().current_epoch(), 7);
        task.abort();
        let _ = task.await;
        drop(lease);
        let sealed = tokio::time::timeout(
            Duration::from_secs(5),
            stroma.seal_replica_for_recovery("q", 0, None, request()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!((sealed.message_next, sealed.event_next), (1, 1));
        drop(h);
        stroma.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn seal_drains_other_fence_when_one_writer_fails_and_retries_after_restart() {
        let dir = test_dir!("recovery_seal_failed_writer");
        let stroma = open(&dir.root).await;
        let qh = populated(&stroma).await;
        let h = qh.resolve().unwrap();
        h.msg_log().shutdown().await.unwrap();
        assert!(
            stroma
                .seal_replica_for_recovery("q", 0, None, request())
                .await
                .is_err()
        );
        assert_eq!(h.event_log().current_epoch(), 8);
        assert!(
            read_intent(&stroma.recovery_seal_path("q", 0, None))
                .unwrap()
                .is_some()
        );
        assert!(h.ensure_owner().is_err());
        drop(h);
        stroma.shutdown().await.unwrap();
        drop(stroma);
        let reopened = open(&dir.root).await;
        let sealed = reopened
            .seal_replica_for_recovery("q", 0, None, request())
            .await
            .unwrap();
        assert_eq!((sealed.message_next, sealed.event_next), (1, 1));
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn seal_rejects_conflicting_request_and_corrupt_marker_without_reopening() {
        let dir = test_dir!("recovery_seal_conflict");
        let stroma = open(&dir.root).await;
        populated(&stroma).await;
        stroma
            .seal_replica_for_recovery("q", 0, None, request())
            .await
            .unwrap();
        let path = stroma.recovery_seal_path("q", 0, None);
        let original = fs::read(&path).unwrap();
        for other in [
            RecoverySealRequest {
                transition: [20; 32],
                ..request()
            },
            RecoverySealRequest {
                fence_epoch: 9,
                ..request()
            },
        ] {
            assert!(
                stroma
                    .seal_replica_for_recovery("q", 0, None, other)
                    .await
                    .is_err()
            );
            assert_eq!(fs::read(&path).unwrap(), original);
        }
        stroma.shutdown().await.unwrap();
        drop(stroma);
        for bytes in [
            &b""[..],
            &b"bad marker"[..],
            &original[..original.len() - 1],
        ] {
            fs::write(&path, bytes).unwrap();
            let reopened = open(&dir.root).await;
            assert!(matches!(
                reopened.queue_handle("q", 0, None).await,
                Err(StromaError::Corruption(_))
            ));
            assert!(
                reopened
                    .seal_replica_for_recovery("q", 0, None, request())
                    .await
                    .is_err()
            );
            reopened.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn seal_blocks_startup_suffix_repair_so_evidence_remains_available() {
        let dir = test_dir!("recovery_seal_partial_data");
        let stroma = open(&dir.root).await;
        stroma
            .become_queue_follower_with_epoch("q", 0, None, 7)
            .await
            .unwrap();
        let qh = stroma.queue_handle("q", 0, None).await.unwrap();
        let h = qh.resolve().unwrap();
        let event = event_msg(&StromaEvent::EnqueueMany {
            reqs: vec![EnqueueEventMeta {
                off: 0,
                retries: 0,
                expire_at: None,
            }],
        })
        .unwrap();
        h.event_log()
            .append_replicated_batch(7, 0, vec![event], Some(KDurability::AfterFsync))
            .await
            .unwrap();
        let sealed = stroma
            .seal_replica_for_recovery("q", 0, None, request())
            .await
            .unwrap();
        assert_eq!((sealed.message_next, sealed.event_next), (0, 1));
        drop(h);
        stroma.shutdown().await.unwrap();
        drop(stroma);
        let reopened = open(&dir.root).await;
        assert!(reopened.queue_handle("q", 0, None).await.is_err());
        assert_eq!(
            reopened
                .seal_replica_for_recovery("q", 0, None, request())
                .await
                .unwrap(),
            sealed
        );
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn seal_waits_for_snapshot_io_even_after_its_async_caller_is_gone() {
        let dir = test_dir!("recovery_seal_snapshot_io");
        let stroma = open(&dir.root).await;
        let qh = populated(&stroma).await;
        let h = qh.resolve().unwrap();
        let gate = h.recovery_gate.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        // Model a blocking filesystem job which outlives its async caller.
        let filesystem_job = tokio::task::spawn_blocking(move || {
            let _io = gate.snapshot_io.lock();
            started.send(()).unwrap();
            released.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        ready.await.unwrap();
        let worker = stroma.clone();
        let seal = tokio::spawn(async move {
            worker
                .seal_replica_for_recovery("q", 0, None, request())
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while h.role() != QueueRole::Frozen {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!seal.is_finished());
        assert!(
            read_intent(&stroma.recovery_seal_path("q", 0, None))
                .unwrap()
                .is_none()
        );
        release.send(()).unwrap();
        filesystem_job.await.unwrap();
        seal.await.unwrap().unwrap();
        assert!(h.recovery_gate.ensure_open("q", 0, None).is_err());
        drop(h);
        stroma.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn seal_covers_streams_and_empty_replicas_without_inventing_history() {
        let dir = test_dir!("recovery_seal_stream");
        let stroma = open(&dir.root).await;
        stroma.create_stream("stream", 0, None).await.unwrap();
        stroma
            .append_stream_record(
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
        let stream = stroma
            .seal_replica_for_recovery("stream", 0, None, request())
            .await
            .unwrap();
        assert_eq!((stream.message_next, stream.event_next), (1, 0));
        assert!(
            stroma
                .become_stream_owner_with_epoch("stream", 0, 9)
                .await
                .is_err()
        );
        assert!(
            stroma
                .become_stream_follower_with_epoch("stream", 0, 9)
                .await
                .is_err()
        );
        assert!(stroma.create_stream("stream", 0, None).await.is_err());
        let empty = stroma
            .seal_replica_for_recovery("empty", 0, Some("default"), request())
            .await
            .unwrap();
        assert_eq!((empty.message_next, empty.event_next), (0, 0));
        assert_eq!(
            stroma
                .seal_replica_for_recovery("empty", 0, None, request())
                .await
                .unwrap(),
            empty
        );
        stroma.shutdown().await.unwrap();
        drop(stroma);
        let reopened = open(&dir.root).await;
        assert!(reopened.create_stream("stream", 0, None).await.is_err());
        assert_eq!(
            reopened
                .seal_replica_for_recovery("stream", 0, None, request())
                .await
                .unwrap(),
            stream
        );
        let debug = reopened.debug_snapshot().await.unwrap();
        assert!(
            debug
                .queues
                .iter()
                .all(|queue| queue.role == QueueRole::Frozen)
        );
        reopened.shutdown().await.unwrap();
    }
}
