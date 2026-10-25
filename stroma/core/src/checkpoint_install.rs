//! Recoverable queue checkpoint replacement. A pending record owns the reset.
//! a durable completion receipt prevents retries from erasing later backfill.

use super::*;
use crate::QueueInternalState;
use std::io::Write;

// Version 2 journals hold a single-log QueueStateCheckpoint.
const MAGIC: &[u8; 8] = b"CINST\0\0\x02";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Journal {
    topic: String,
    partition: u32,
    group: Option<String>,
    phase: Phase,
    #[serde(default)]
    history: Option<StorageHistoryBinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Phase {
    Pending(QueueStateCheckpoint),
    Complete {
        digest: [u8; 32],
        backfill: Option<Box<QueueStateCheckpoint>>,
    },
}

fn digest(install: &QueueStateCheckpoint) -> Result<[u8; 32]> {
    Ok(*blake3::hash(&rmp_serde::to_vec_named(install).map_err(encode_err)?).as_bytes())
}

fn validate(
    topic: &str,
    part: u32,
    install: &QueueStateCheckpoint,
) -> Result<(QueueInternalState, crate::SnapshotMeta)> {
    if install.start_offset > install.next_offset {
        return Err(StromaError::InvalidArgument(format!(
            "checkpoint start {} is past its applied boundary {}",
            install.start_offset, install.next_offset,
        )));
    }
    // The snapshot covers every record below the boundary.
    let applied = install.next_offset.saturating_sub(1);
    let mut state = QueueInternalState::new(topic.to_owned(), part);
    let meta = state
        .load_snapshot(&install.state_snapshot)
        .map_err(decode_err)?;
    if meta.last_snapshot_event_offset != applied {
        return Err(StromaError::InvalidArgument(format!(
            "checkpoint snapshot event offset {} does not match applied boundary {}",
            meta.last_snapshot_event_offset, install.next_offset,
        )));
    }
    // Backfill must start at or below every payload the state can still
    // reference, and the state cannot reference records past the boundary.
    if install.start_offset > state.lowest_not_settled_offset()
        || state.required_message_next() > install.next_offset
    {
        return Err(StromaError::InvalidArgument(format!(
            "checkpoint payload range {}..{} does not cover state-referenced messages from {} through {}",
            install.start_offset,
            install.next_offset,
            state.lowest_not_settled_offset(),
            state.required_message_next(),
        )));
    }
    Ok((state, meta))
}

fn read_journal(path: &Path) -> Result<Option<Journal>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(io_err(err)),
    };
    if bytes.len() < 12 || &bytes[..8] != MAGIC {
        return Err(StromaError::Corruption(
            "invalid checkpoint installation header".into(),
        ));
    }
    let end = bytes.len() - 4;
    let crc = u32::from_be_bytes(bytes[end..].try_into().unwrap());
    if crc32c::crc32c(&bytes[..end]) != crc {
        return Err(StromaError::Corruption(
            "checkpoint installation checksum mismatch".into(),
        ));
    }
    rmp_serde::from_slice(&bytes[8..end])
        .map(Some)
        .map_err(|err| StromaError::Corruption(format!("invalid checkpoint installation: {err}")))
}

fn persist_journal(path: &Path, journal: &Journal) -> Result<()> {
    let parent = path.parent().expect("journal has a parent");
    fs::create_dir_all(parent).map_err(io_err)?;
    let mut bytes = MAGIC.to_vec();
    bytes.extend(rmp_serde::to_vec_named(journal).map_err(encode_err)?);
    bytes.extend(crc32c::crc32c(&bytes).to_be_bytes());
    let temp = parent.join(format!("checkpoint.{}.pending", uuid::Uuid::now_v7()));
    let result = (|| {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)
            .map_err(io_err)?;
        file.write_all(&bytes).map_err(io_err)?;
        file.sync_all().map_err(io_err)?;
        drop(file);
        fs::rename(&temp, path).map_err(io_err)?;
        recovery_seal::sync_directories(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}

impl Stroma {
    fn checkpoint_journal_path(&self, topic: &str, part: u32, group: Option<&str>) -> PathBuf {
        self.snap_dir(topic, part, group).join("checkpoint.install")
    }

    fn checkpoint_journal(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Option<Journal>> {
        let journal = read_journal(&self.checkpoint_journal_path(topic, part, group))?;
        if let Some(journal) = &journal {
            if journal.topic != topic
                || journal.partition != part
                || journal.group.as_deref() != normalize_group(group)
                || journal.history != self.storage_history_binding(topic, part, group)?
            {
                return Err(StromaError::Corruption(
                    "checkpoint installation identity mismatch".into(),
                ));
            }
        }
        Ok(journal)
    }

    pub(super) fn ensure_checkpoint_not_pending(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<()> {
        if matches!(
            self.checkpoint_journal(topic, part, group)?
                .map(|j| j.phase),
            Some(Phase::Pending(_))
        ) {
            return Err(StromaError::Io(
                "checkpoint installation must finish before sealing or destruction".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn checkpoint_boundary(&self, _boundary: &'static str) -> Result<()> {
        #[cfg(test)]
        {
            if std::env::var("STROMA_CHECKPOINT_CRASH_BOUNDARY").as_deref() == Ok(_boundary) {
                let ready = std::env::var_os("STROMA_CHECKPOINT_CRASH_READY").unwrap();
                fs::write(ready, _boundary).unwrap();
                loop {
                    std::thread::park();
                }
            }
            let mut fault = self.checkpoint_fault.lock().unwrap();
            if *fault == Some(_boundary) {
                *fault = None;
                return Err(StromaError::Io(format!(
                    "injected checkpoint failure after {_boundary}"
                )));
            }
        }
        Ok(())
    }

    /// The caller must freshly authorize this exact non-voting learner before
    /// each installation. Ordinary bound replicas cannot use checkpoint reset.
    pub async fn install_queue_learner_checkpoint(
        &self,
        receipt: PreparedStorageHistory,
        install: QueueStateCheckpoint,
    ) -> Result<QueueStateCheckpointInstallOutcome> {
        if receipt.stream {
            return Err(StromaError::Unsupported(
                "learner checkpoint requires queue storage".into(),
            ));
        }
        let (_, meta) = validate(&receipt.topic, receipt.partition, &install)?;
        let st = self.clone();
        tokio::spawn(async move {
            st.install_checkpoint_inner(
                &receipt.topic,
                receipt.partition,
                receipt.group.as_deref(),
                &install,
                &receipt,
            )
            .await?;
            Ok(QueueStateCheckpointInstallOutcome {
                next_offset: install.start_offset,
                snapshot_meta: meta,
            })
        })
        .await
        .map_err(io_err)?
    }

    async fn install_checkpoint_inner(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        install: &QueueStateCheckpoint,
        learner: &PreparedStorageHistory,
    ) -> Result<()> {
        loop {
            self.queue_handle(topic, part, group).await?;
            let _lifecycle = self.lock_partition_lifecycle(topic, part, group).await;
            // Replacing a bound history needs a lineage-aware installation receipt.
            self.verify_admitted_storage_history(learner)?;
            let h = {
                let current = self.queue_handles.load();
                slot_lookup_no_alloc(&current, topic, part, group)
                    .and_then(|slot| slot.handle.get().cloned())
            };
            let Some(h) = h else {
                continue;
            };
            let mut apply = h.follower_apply_state().await;
            h.ensure_not_recovery_sealed()?;
            if h.role() != QueueRole::Follower {
                return Err(StromaError::WrongQueueRole {
                    expected: QueueRole::Follower,
                    actual: h.role(),
                });
            }
            h.work_queue()?;
            Self::checkpoint_epoch(&h, install)?;
            let hash = digest(install)?;
            if matches!(self.checkpoint_journal(topic, part, group)?.map(|j| j.phase), Some(Phase::Complete { digest, .. }) if digest == hash)
            {
                // Already committed. In particular, do not wipe later backfill.
                return Ok(());
            }
            let old_apply = *apply;
            *apply = true;
            h.recovery_gate
                .checkpoint_pending
                .store(true, Ordering::Release);
            h.recovery_gate
                .checkpoint_generation
                .fetch_add(1, Ordering::AcqRel);
            let journal = Journal {
                topic: topic.to_owned(),
                partition: part,
                group: group.map(str::to_owned),
                phase: Phase::Pending(install.clone()),
                history: Some(learner.binding.clone()),
            };
            let path = self.checkpoint_journal_path(topic, part, group);
            let gate = h.recovery_gate.clone();
            let write = tokio::task::spawn_blocking(move || {
                let _io = gate.snapshot_io.lock();
                persist_journal(&path, &journal)
            })
            .await
            .map_err(io_err)?;
            if let Err(err) = write {
                // No destructive work has started. If the new intent was never
                // published, restore the original eligibility/partial-apply state.
                if matches!(
                    self.checkpoint_journal(topic, part, group),
                    Ok(None)
                        | Ok(Some(Journal {
                            phase: Phase::Complete { .. },
                            ..
                        }))
                ) {
                    h.recovery_gate
                        .checkpoint_pending
                        .store(false, Ordering::Release);
                    *apply = old_apply;
                }
                return Err(err);
            }
            self.checkpoint_boundary("intent")?;
            tracing::info!(topic, partition = part, group,
                checkpoint = %blake3::Hash::from_bytes(hash),
                epoch = install.epoch, start = install.start_offset, next = install.next_offset,
                "checkpoint intent durable; replacing follower logs and state");
            self.finish_live_checkpoint(topic, part, group, &h, &mut apply)
                .await?;
            return Ok(());
        }
    }

    fn checkpoint_epoch(h: &QueueHandleInner, install: &QueueStateCheckpoint) -> Result<()> {
        let local = h.event_log().current_epoch();
        if local != install.epoch {
            return Err(StromaError::InvalidArgument(format!(
                "checkpoint epoch {} does not match local epoch {local}",
                install.epoch,
            )));
        }
        Ok(())
    }

    pub(super) async fn resume_live_checkpoint(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        h: &Arc<QueueHandleInner>,
    ) -> Result<()> {
        let stroma = self.clone();
        let topic = topic.to_owned();
        let group = group.map(str::to_owned);
        let h = h.clone();
        tokio::spawn(async move {
            let _lifecycle = stroma
                .lock_partition_lifecycle(&topic, part, group.as_deref())
                .await;
            let current_handle = {
                let current = stroma.queue_handles.load();
                slot_lookup_no_alloc(&current, &topic, part, group.as_deref())
                    .and_then(|slot| slot.handle.get().cloned())
            };
            if !current_handle
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &h))
            {
                return Ok(());
            }
            let mut apply = h.follower_apply_state().await;
            if h.recovery_gate.checkpoint_pending.load(Ordering::Acquire) {
                stroma
                    .finish_live_checkpoint(&topic, part, group.as_deref(), &h, &mut apply)
                    .await?;
            }
            Ok(())
        })
        .await
        .map_err(io_err)?
    }

    async fn finish_live_checkpoint(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        h: &QueueHandleInner,
        apply: &mut bool,
    ) -> Result<()> {
        let journal = self
            .checkpoint_journal(topic, part, group)?
            .ok_or_else(|| StromaError::Corruption("pending checkpoint has no journal".into()))?;
        match &journal.phase {
            Phase::Pending(install) => {
                let (state, meta) = validate(topic, part, install)?;
                Self::checkpoint_epoch(h, install)?;
                for log in h.physical_logs() {
                    log.become_follower();
                }
                self.checkpoint_boundary("follower")?;
                h.event_log()
                    .destructive_reset_to_checkpoint_at_epoch(install.start_offset, install.epoch)
                    .await
                    .map_err(io_err)?;
                self.checkpoint_boundary("log_reset")?;
                h.work_queue()?
                    .install_snapshot_state(state, meta)
                    .await
                    .map_err(io_err)?;
                h.applied_upto()
                    .store(install.next_offset.saturating_sub(1), Ordering::Release);
                h.reset_ordered_apply_after_recovery(install.next_offset);
                self.checkpoint_boundary("state")?;
                let gate = h.recovery_gate.clone();
                let stroma = self.clone();
                tokio::task::spawn_blocking(move || {
                    let _io = gate.snapshot_io.lock();
                    stroma.publish_checkpoint_snapshot_and_receipt(journal)
                })
                .await
                .map_err(io_err)??;
            }
            Phase::Complete { .. } => {
                // The receipt may have been renamed before its directory sync
                // failed. Finish durability before permitting any new backfill.
                let path = self.checkpoint_journal_path(topic, part, group);
                tokio::task::spawn_blocking(move || {
                    fs::File::open(&path)
                        .and_then(|f| f.sync_all())
                        .map_err(io_err)?;
                    recovery_seal::sync_directories(path.parent().unwrap())
                })
                .await
                .map_err(io_err)??;
            }
        }
        h.set_dirty_snapshot(false);
        *apply = false;
        h.recovery_gate
            .checkpoint_generation
            .fetch_add(1, Ordering::AcqRel);
        h.recovery_gate
            .checkpoint_pending
            .store(false, Ordering::Release);
        tracing::info!(
            topic,
            partition = part,
            group,
            "checkpoint installation complete; payload backfill remains promotion-gated"
        );
        Ok(())
    }

    pub(super) fn permits_combined_checkpoint_backfill(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        next: u64,
        snapshot: &[u8],
        log: &Keratin,
    ) -> Result<bool> {
        if !self
            .snap_dir(topic, part, group)
            .join("storage.learner")
            .try_exists()
            .map_err(io_err)?
        {
            return Ok(false);
        }
        let Some(journal) = self.checkpoint_journal(topic, part, group)? else {
            return Ok(false);
        };
        let Phase::Complete {
            digest: expected,
            backfill: Some(install),
        } = journal.phase
        else {
            return Ok(false);
        };
        validate(topic, part, &install)?;
        Ok(digest(&install)? == expected
            && journal.history.is_some()
            && journal.history == self.storage_history_binding(topic, part, group)?
            && install.epoch == log.current_epoch()
            && install.next_offset == next
            && install.state_snapshot == snapshot
            && install.start_offset == log.head_offset()
            && log.next_offset() >= install.start_offset
            && log.next_offset() < next)
    }

    fn publish_checkpoint_snapshot_and_receipt(&self, mut journal: Journal) -> Result<()> {
        let Phase::Pending(install) = &journal.phase else {
            return Ok(());
        };
        self.write_queue_snapshot_envelope(
            &journal.topic,
            journal.partition,
            journal.group.as_deref(),
            2,
            install.next_offset,
            &install.state_snapshot,
        )?;
        recovery_seal::sync_directories(&self.snap_dir(
            &journal.topic,
            journal.partition,
            journal.group.as_deref(),
        ))?;
        recovery_seal::sync_directories(&self.root.join("tmp"))?;
        self.checkpoint_boundary("snapshot")?;
        let hash = digest(install)?;
        let backfill = Some(Box::new(install.clone()));
        journal.phase = Phase::Complete {
            digest: hash,
            backfill,
        };
        persist_journal(
            &self.checkpoint_journal_path(
                &journal.topic,
                journal.partition,
                journal.group.as_deref(),
            ),
            &journal,
        )?;
        self.checkpoint_boundary("complete")
    }

    pub(super) async fn resume_cold_checkpoint(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        lifecycle: tokio::sync::OwnedMutexGuard<()>,
    ) -> Result<tokio::sync::OwnedMutexGuard<()>> {
        self.resume_cold_checkpoint_authorized(topic, part, group, lifecycle, false)
            .await
    }

    pub(super) async fn resume_cold_checkpoint_authorized(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        lifecycle: tokio::sync::OwnedMutexGuard<()>,
        excluded_learner: bool,
    ) -> Result<tokio::sync::OwnedMutexGuard<()>> {
        let Some(journal) = self.checkpoint_journal(topic, part, group)? else {
            return Ok(lifecycle);
        };
        if excluded_learner {
            // The caller has checked the exact binding under this lifecycle
            // lock. Journal identity is bound to that same history above.
            if journal.history.is_none()
                || self
                    .recovery_seal_path(topic, part, group)
                    .try_exists()
                    .map_err(io_err)?
            {
                return Err(StromaError::InvalidArgument(
                    "learner checkpoint has no bound authority or is sealed".into(),
                ));
            }
        } else {
            self.ensure_partition_not_sealed(topic, part, group)?;
        }
        let stroma = self.clone();
        // Own the lifecycle guard in the task: a cancelled materialization must
        // not release it while detached recovery I/O is still deleting files.
        let (lifecycle, result) = tokio::spawn(async move {
            let result = stroma.resume_cold_checkpoint_inner(journal).await;
            (lifecycle, result)
        })
        .await
        .map_err(io_err)?;
        result?;
        Ok(lifecycle)
    }

    async fn resume_cold_checkpoint_inner(&self, journal: Journal) -> Result<()> {
        match &journal.phase {
            Phase::Pending(install) => {
                validate(&journal.topic, journal.partition, install)?;
                tracing::info!(topic = journal.topic, partition = journal.partition,
                    group = journal.group, checkpoint = %blake3::Hash::from_bytes(digest(install)?),
                    epoch = install.epoch, start = install.start_offset, next = install.next_offset,
                    "resuming interrupted checkpoint before ordinary log recovery");
                let logs = vec![(
                    self.tp_part_dir(&journal.topic, journal.partition, journal.group.as_deref()),
                    install.start_offset,
                    install.epoch,
                )];
                Keratin::rebuild_for_checkpoint_recovery(logs)
                    .await
                    .map_err(io_err)?;
                self.checkpoint_boundary("cold_logs")?;
                let stroma = self.clone();
                tokio::task::spawn_blocking(move || {
                    stroma.publish_checkpoint_snapshot_and_receipt(journal)
                })
                .await
                .map_err(io_err)??;
            }
            Phase::Complete { .. } => {
                let path = self.checkpoint_journal_path(
                    &journal.topic,
                    journal.partition,
                    journal.group.as_deref(),
                );
                tokio::task::spawn_blocking(move || {
                    fs::File::open(&path)
                        .and_then(|f| f.sync_all())
                        .map_err(io_err)?;
                    recovery_seal::sync_directories(path.parent().unwrap())
                })
                .await
                .map_err(io_err)??;
            }
        }
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use keratin_log::test_dir;

    async fn open(root: &Path) -> Stroma {
        Stroma::open(
            root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap()
    }

    fn checkpoint(count: u64) -> QueueStateCheckpoint {
        let mut state = QueueInternalState::new("q".into(), 0);
        for offset in 0..count {
            state.enqueue(offset, 0, None);
        }
        QueueStateCheckpoint {
            epoch: 7,
            start_offset: 0,
            next_offset: count,
            state_snapshot: state.encode_snapshot(count.saturating_sub(1)),
        }
    }

    fn binding() -> StorageHistoryBinding {
        StorageHistoryBinding {
            resource_incarnation: [1; 16],
            accepted_history: [2; 16],
            writer_session: [3; 16],
        }
    }

    async fn admit(stroma: &Stroma) {
        stroma
            .prepare_queue_learner_storage("q", 0, None, binding(), [4; 32])
            .await
            .unwrap();
    }

    async fn follower(stroma: &Stroma) {
        admit(stroma).await;
        stroma
            .become_queue_follower_with_epoch("q", 0, None, 7)
            .await
            .unwrap();
    }

    async fn install(stroma: &Stroma, checkpoint: QueueStateCheckpoint) -> Result<()> {
        stroma
            .install_queue_learner_checkpoint(
                PreparedStorageHistory {
                    topic: "q".into(),
                    partition: 0,
                    group: None,
                    stream: false,
                    binding: binding(),
                    storage_instance: stroma.storage_session,
                },
                checkpoint,
            )
            .await
            .map(|_| ())
    }

    fn payload(bytes: &[u8]) -> StromaEvent {
        StromaEvent::PayloadEnqueue {
            not_before: None,
            expire_at: None,
            headers: vec![],
            payload: bytes.to_vec(),
        }
    }

    async fn assert_installed(stroma: &Stroma, count: u64) {
        admit(stroma).await;
        let ticket = stroma.queue_handle("q", 0, None).await.unwrap();
        let h = ticket.resolve().unwrap();
        assert!(h.optional_message_log().is_none());
        assert_eq!(h.event_log().next_offset(), 0);
        assert_eq!(h.full_debug_info().await.state.ready_count, count as usize);
        if count > 0 {
            assert_eq!(h.role(), QueueRole::Follower);
            assert!(stroma.become_queue_owner("q", 0, None).await.is_err());
        }
        assert!(matches!(
            stroma
                .checkpoint_journal("q", 0, None)
                .unwrap()
                .unwrap()
                .phase,
            Phase::Complete { .. }
        ));
    }

    #[tokio::test]
    async fn learner_checkpoint_restart_retains_authority_and_requires_payload_backfill() {
        for count in [0, 1] {
            for boundary in [
                "intent",
                "follower",
                "log_reset",
                "state",
                "snapshot",
                "complete",
            ] {
                let dir = test_dir!("learner_checkpoint_restart");
                let st = open(&dir.root).await;
                let binding = StorageHistoryBinding {
                    resource_incarnation: [1; 16],
                    accepted_history: [2; 16],
                    writer_session: [3; 16],
                };
                let receipt = st
                    .prepare_queue_learner_storage("q", 0, None, binding.clone(), [4; 32])
                    .await
                    .unwrap();
                follower(&st).await;
                let mut wrong = receipt.clone();
                wrong.storage_instance = [9; 16];
                assert!(
                    st.install_queue_learner_checkpoint(wrong, checkpoint(count))
                        .await
                        .is_err()
                );
                *st.checkpoint_fault.lock().unwrap() = Some(boundary);
                let error = st
                    .install_queue_learner_checkpoint(receipt.clone(), checkpoint(count))
                    .await
                    .unwrap_err();
                assert!(error.to_string().contains(boundary), "{error}");
                st.shutdown().await.unwrap();
                drop(st);
                let st = open(&dir.root).await;
                // Ordinary access still requires fresh metadata admission.
                assert!(st.queue_handle("q", 0, None).await.is_err());
                let renewed = st
                    .prepare_queue_learner_storage("q", 0, None, binding, [5; 32])
                    .await
                    .unwrap();
                assert_ne!(renewed.storage_instance, receipt.storage_instance);
                follower(&st).await;
                assert_eq!(
                    st.queue_replication_next_offset("q", 0, None)
                        .await
                        .unwrap(),
                    0
                );
                if count > 0 {
                    assert!(
                        st.verify_queue_learner_caught_up("q", 0, None, 7, count)
                            .await
                            .is_err()
                    );
                    st.apply_replicated_queue_batch(
                        "q",
                        0,
                        None,
                        ReplicatedEventBatch {
                            epoch: 7,
                            first_offset: 0,
                            durability: Some(KDurability::AfterFsync),
                            events: vec![payload(b"backfill")],
                        },
                    )
                    .await
                    .unwrap();
                }
                st.verify_queue_learner_caught_up("q", 0, None, 7, count)
                    .await
                    .unwrap();
                st.install_queue_learner_checkpoint(renewed, checkpoint(count))
                    .await
                    .unwrap();
                assert_eq!(
                    st.queue_replication_next_offset("q", 0, None)
                        .await
                        .unwrap(),
                    count
                );
                st.shutdown().await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn checkpoint_each_interruption_resumes_on_live_access() {
        for boundary in [
            "intent",
            "follower",
            "log_reset",
            "state",
            "snapshot",
            "complete",
        ] {
            let dir = test_dir!("checkpoint_live_boundaries");
            let stroma = open(&dir.root).await;
            follower(&stroma).await;
            *stroma.checkpoint_fault.lock().unwrap() = Some(boundary);
            let err = install(&stroma, checkpoint(2)).await.unwrap_err();
            assert!(err.to_string().contains(boundary), "{err}");
            let h = slot_lookup_no_alloc(&stroma.queue_handles.load(), "q", 0, None)
                .unwrap()
                .handle
                .get()
                .unwrap()
                .clone();
            assert_eq!(h.role(), QueueRole::Frozen);
            assert!(h.ensure_not_recovery_sealed().is_err());
            assert_installed(&stroma, 2).await;
            stroma.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn checkpoint_each_interruption_resumes_after_restart_including_zero() {
        for count in [0, 1, 2] {
            for boundary in [
                "intent",
                "follower",
                "log_reset",
                "state",
                "snapshot",
                "complete",
            ] {
                let dir = test_dir!("checkpoint_restart_boundaries");
                let stroma = open(&dir.root).await;
                follower(&stroma).await;
                *stroma.checkpoint_fault.lock().unwrap() = Some(boundary);
                install(&stroma, checkpoint(count)).await.unwrap_err();
                stroma.shutdown().await.unwrap();
                drop(stroma);
                let stroma = open(&dir.root).await;
                assert_installed(&stroma, count).await;
                stroma.shutdown().await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn checkpoint_completed_retry_and_restart_preserve_later_backfill() {
        let dir = test_dir!("checkpoint_receipt_preserves_backfill");
        let stroma = open(&dir.root).await;
        follower(&stroma).await;
        let target = checkpoint(2);
        install(&stroma, target.clone()).await.unwrap();
        stroma
            .apply_replicated_queue_batch(
                "q",
                0,
                None,
                ReplicatedEventBatch {
                    epoch: 7,
                    first_offset: 0,
                    events: vec![payload(b"a"), payload(b"b")],
                    durability: Some(KDurability::AfterFsync),
                },
            )
            .await
            .unwrap();
        install(&stroma, target.clone()).await.unwrap();
        let ticket = stroma.queue_handle("q", 0, None).await.unwrap();
        assert_eq!(ticket.resolve().unwrap().event_log().next_offset(), 2);
        stroma.shutdown().await.unwrap();
        drop(stroma);
        let stroma = open(&dir.root).await;
        // Renew admission without resetting storage, then inspect before a role transition.
        admit(&stroma).await;
        let ticket = stroma.queue_handle("q", 0, None).await.unwrap();
        assert_eq!(ticket.resolve().unwrap().event_log().next_offset(), 2);
        follower(&stroma).await;
        install(&stroma, target).await.unwrap();
        let records = ticket
            .resolve()
            .unwrap()
            .event_log()
            .reader()
            .scan_from(0, 10)
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].payload, payload(b"b").encode().unwrap());
        stroma.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_restart_recovers_partial_files_and_repeated_interruption() {
        let dir = test_dir!("checkpoint_partial_files");
        let stroma = open(&dir.root).await;
        follower(&stroma).await;
        *stroma.checkpoint_fault.lock().unwrap() = Some("intent");
        install(&stroma, checkpoint(2)).await.unwrap_err();
        let events = stroma.tp_part_dir("q", 0, None);
        stroma.shutdown().await.unwrap();
        drop(stroma);
        // Model interruption inside reset, before valid new segment headers exist.
        for entry in fs::read_dir(events.join("segments")).unwrap() {
            fs::write(entry.unwrap().path(), b"partial").unwrap();
        }
        let stroma = open(&dir.root).await;
        *stroma.checkpoint_fault.lock().unwrap() = Some("cold_logs");
        let error = stroma
            .prepare_queue_learner_storage("q", 0, None, binding(), [4; 32])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("cold_logs"), "{error}");
        assert!(stroma.queue_handle("q", 0, None).await.is_err());
        stroma.shutdown().await.unwrap();
        drop(stroma);
        let stroma = open(&dir.root).await;
        assert_installed(&stroma, 2).await;
        stroma.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_cancellation_keeps_installation_serialized() {
        let dir = test_dir!("checkpoint_cancel");
        let stroma = open(&dir.root).await;
        follower(&stroma).await;
        let h = slot_lookup_no_alloc(&stroma.queue_handles.load(), "q", 0, None)
            .unwrap()
            .handle
            .get()
            .unwrap()
            .clone();
        let io = h.recovery_gate.snapshot_io.lock();
        let other = stroma.clone();
        let install = tokio::spawn(async move { install(&other, checkpoint(2)).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !h.recovery_gate.checkpoint_pending.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        install.abort();
        assert!(install.await.unwrap_err().is_cancelled());
        drop(io);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            assert_installed(&stroma, 2),
        )
        .await
        .unwrap();
        stroma.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_pending_blocks_seal_and_destructive_maintenance() {
        let dir = test_dir!("checkpoint_pending_maintenance");
        let stroma = open(&dir.root).await;
        follower(&stroma).await;
        *stroma.checkpoint_fault.lock().unwrap() = Some("intent");
        install(&stroma, checkpoint(2)).await.unwrap_err();
        assert!(stroma.ensure_checkpoint_not_pending("q", 0, None).is_err());
        assert!(stroma.destroy_partition("q", 0, None).await.is_err());
        assert!(
            stroma
                .seal_replica_for_recovery(
                    "q",
                    0,
                    None,
                    RecoverySealRequest {
                        transition: [3; 32],
                        fence_epoch: 8,
                    }
                )
                .await
                .is_err()
        );
        assert_installed(&stroma, 2).await;
        stroma.shutdown().await.unwrap();
    }

    #[test]
    fn checkpoint_journal_rejects_corruption() {
        let dir = test_dir!("checkpoint_journal_validation");
        let path = dir.root.join("install");
        let journal = Journal {
            history: None,
            topic: "q".into(),
            partition: 0,
            group: None,
            phase: Phase::Pending(checkpoint(2)),
        };
        persist_journal(&path, &journal).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        bytes[13] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            read_journal(&path),
            Err(StromaError::Corruption(_))
        ));
    }
    #[tokio::test]
    async fn checkpoint_wrong_identity_and_corruption_block_cold_open() {
        let dir = test_dir!("checkpoint_wrong_identity");
        let stroma = open(&dir.root).await;
        follower(&stroma).await;
        let path = stroma.checkpoint_journal_path("q", 0, None);
        stroma.shutdown().await.unwrap();
        drop(stroma);
        let journal = Journal {
            history: Some(binding()),
            topic: "another-queue".into(),
            partition: 0,
            group: None,
            phase: Phase::Pending(checkpoint(2)),
        };
        persist_journal(&path, &journal).unwrap();
        let stroma = open(&dir.root).await;
        assert!(matches!(
            stroma
                .prepare_queue_learner_storage("q", 0, None, binding(), [4; 32])
                .await,
            Err(StromaError::Corruption(_))
        ));
        assert!(stroma.queue_handle("q", 0, None).await.is_err());
        assert!(stroma.admitted_histories.is_empty());
        fs::write(path, b"damaged").unwrap();
        assert!(matches!(
            stroma
                .prepare_queue_learner_storage("q", 0, None, binding(), [4; 32])
                .await,
            Err(StromaError::Corruption(_))
        ));
        stroma.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_rejects_old_snapshot_after_install_and_eviction() {
        let dir = test_dir!("checkpoint_stale_snapshot");
        let stroma = open(&dir.root).await;
        follower(&stroma).await;
        let h = slot_lookup_no_alloc(&stroma.queue_handles.load(), "q", 0, None)
            .unwrap()
            .handle
            .get()
            .unwrap()
            .clone();
        let gate = h.recovery_gate.clone();
        let generation = gate.checkpoint_generation.load(Ordering::Acquire);
        let blob = QueueInternalState::new("q".into(), 0).encode_snapshot(0);
        install(&stroma, checkpoint(2)).await.unwrap();
        // Resume the blocking half of a snapshot prepared before installation.
        let write = || stroma.write_queue_snapshot("q", 0, None, 0, &blob);
        let installed = fs::read(stroma.snap_file("q", 0, None)).unwrap();
        assert!(
            gate.write_snapshot(generation, "q", 0, None, write)
                .is_err()
        );
        assert_eq!(fs::read(stroma.snap_file("q", 0, None)).unwrap(), installed);
        let generation = gate.checkpoint_generation.load(Ordering::Acquire);
        stroma.evict("q", 0, None).await.unwrap();
        stroma.queue_handle("q", 0, None).await.unwrap();
        assert!(
            gate.write_snapshot(generation, "q", 0, None, write)
                .is_err()
        );
        assert_eq!(fs::read(stroma.snap_file("q", 0, None)).unwrap(), installed);
        stroma.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn checkpoint_crash_child() {
        let Some(root) = std::env::var_os("STROMA_CHECKPOINT_CRASH_ROOT") else {
            return;
        };
        let stroma = open(Path::new(&root)).await;
        follower(&stroma).await;
        install(&stroma, checkpoint(2)).await.unwrap();
        panic!("child should have stopped at a checkpoint boundary");
    }

    #[tokio::test]
    async fn checkpoint_sigkill_at_each_installation_boundary_recovers() {
        struct KillOnDrop(std::process::Child);
        impl Drop for KillOnDrop {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        for boundary in [
            "intent",
            "follower",
            "log_reset",
            "state",
            "snapshot",
            "complete",
        ] {
            let dir = test_dir!("checkpoint_sigkill");
            let ready = dir.root.join("tmp/child-ready");
            let mut child = KillOnDrop(
                std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "stroma::checkpoint_install::tests::checkpoint_crash_child",
                        "--nocapture",
                    ])
                    .env("STROMA_CHECKPOINT_CRASH_ROOT", &dir.root)
                    .env("STROMA_CHECKPOINT_CRASH_READY", &ready)
                    .env("STROMA_CHECKPOINT_CRASH_BOUNDARY", boundary)
                    .stdout(std::process::Stdio::null())
                    .spawn()
                    .unwrap(),
            );
            tokio::time::timeout(std::time::Duration::from_secs(20), async {
                while !ready.exists() {
                    assert!(
                        child.0.try_wait().unwrap().is_none(),
                        "child exited before {boundary}"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            child.0.kill().unwrap();
            let status = child.0.wait().unwrap();
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(9));
            let stroma = open(&dir.root).await;
            assert_installed(&stroma, 2).await;
            stroma.shutdown().await.unwrap();
        }
    }
}
