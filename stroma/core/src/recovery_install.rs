//! Crash-safe recovery installation: build a complete generation, then switch
//! the partition's `current` to it. Sealed generations remain available for
//! recovery reads.
use super::recovery_stage::{persist_exact, read_bounded};
use super::*;
use keratin_log::lock_existing_log;

const MAX_META: usize = 65_536;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedQueueRecovery {
    pub spec: QueueRecoveryStageSpec,
    pub storage: PreparedStorageHistory,
}

/// Durable pointer to a generation kept as recovery evidence, stored in the
/// partition's `recovery/` directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GenerationRecord {
    topic: String,
    partition: u32,
    group: Option<String>,
    generation: String,
}
fn invalid(message: impl Into<String>) -> StromaError {
    StromaError::InvalidArgument(message.into())
}
fn key(topic: &str, part: u32, group: Option<&str>) -> (Box<str>, u32, Option<Box<str>>) {
    (topic.into(), part, normalize_group(group).map(Into::into))
}
fn read_meta<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    read_bounded(path, MAX_META)?
        .map(|b| rmp_serde::from_slice(&b).map_err(decode_err))
        .transpose()
}
fn save_meta<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    persist_exact(path, &rmp_serde::to_vec_named(value).map_err(encode_err)?)
}
fn recovery_generation(plan: [u8; 32], instance: [u8; 16]) -> layout::Generation {
    layout::Generation::Recovery { plan, instance }
}
impl GenerationRecord {
    fn new(topic: &str, part: u32, group: Option<&str>, generation: layout::Generation) -> Self {
        Self {
            topic: topic.to_owned(),
            partition: part,
            group: normalize_group(group).map(str::to_owned),
            generation: generation.name(),
        }
    }
    fn generation_for(&self, topic: &str, part: u32, group: Option<&str>) -> Result<layout::Generation> {
        if self.topic != topic || self.partition != part || self.group.as_deref() != normalize_group(group) {
            return Err(StromaError::Corruption(
                "retained generation resource mismatch".into(),
            ));
        }
        layout::Generation::parse(&self.generation)
            .ok_or_else(|| StromaError::Corruption("invalid retained generation name".into()))
    }
}
impl Stroma {
    /// Requires external revocation of this replica's write-quorum membership.
    /// The live generation remains untouched throughout copying and is still usable
    /// as recovery evidence if the owner disappears.
    pub async fn prepare_queue_reseed_storage(
        &self,
        receipt: PreparedStorageHistory,
        intent: [u8; 32],
    ) -> Result<Self> {
        if intent == [0; 32] || receipt.stream {
            return Err(invalid("queue reseed requires an exact queue intent"));
        }
        self.verify_admitted_storage_history(&receipt)?;
        let generation = recovery_generation(intent, self.storage_session);
        let (topic, part, group) = (&receipt.topic, receipt.partition, receipt.group.as_deref());
        if self.partition_generation(topic, part, group) == Some(generation) {
            return Err(invalid(
                "reseed already installed, resume admission without resetting storage",
            ));
        }
        let view = self
            .reseed_views
            .entry(intent)
            .or_insert_with(|| self.generation_view(topic, part, group, generation))
            .clone();
        let prepared = view
            .resume_unaccepted_storage_history(
                &receipt.topic,
                receipt.partition,
                receipt.group.as_deref(),
                receipt.binding.clone(),
            )
            .await?;
        if prepared != receipt {
            return Err(invalid("reseed storage identity changed"));
        }
        Ok(view)
    }

    /// A lost admission reply must resume from the installed cut, never reset
    /// the replacement that now carries the original replica's evidence.
    pub fn installed_queue_reseed_cut(
        &self,
        receipt: &PreparedStorageHistory,
        intent: [u8; 32],
    ) -> Result<Option<(u64, u64, u64)>> {
        self.verify_admitted_storage_history(receipt)?;
        let generation = recovery_generation(intent, self.storage_session);
        if self.partition_generation(&receipt.topic, receipt.partition, receipt.group.as_deref())
            != Some(generation)
        {
            return Ok(None);
        }
        let saved: Option<(PreparedStorageHistory, u64, u64, u64)> = read_meta(
            &self
                .recovery_dir(&receipt.topic, receipt.partition, receipt.group.as_deref())
                .join(format!(
                    "reseed-complete-{}",
                    blake3::Hash::from_bytes(intent)
                )),
        )?;
        let Some((saved, epoch, messages, events)) = saved else {
            return Err(invalid("installed reseed completion receipt absent"));
        };
        if saved != *receipt {
            return Err(invalid("installed reseed receipt differs"));
        }
        Ok(Some((epoch, messages, events)))
    }

    /// Publish only a complete replacement that covers the original durable
    /// tails. Caller must hold fresh consensus authorization for this intent.
    /// Sealing and the `current` switch serialize on the same lifecycle lock.
    pub async fn install_queue_reseed_storage(
        &self,
        receipt: PreparedStorageHistory,
        intent: [u8; 32],
        epoch: u64,
        message_target: u64,
        event_target: u64,
    ) -> Result<()> {
        let st = self.clone();
        tokio::spawn(async move {
            let topic = &receipt.topic;
            let part = receipt.partition;
            let group = receipt.group.as_deref();
            st.verify_admitted_storage_history(&receipt)?;
            let generation = recovery_generation(intent, st.storage_session);
            let resource = key(topic, part, group);
            if st.partition_generation(topic, part, group) == Some(generation) {
                st.verify_queue_learner_caught_up(
                    topic,
                    part,
                    group,
                    epoch,
                    message_target,
                    event_target,
                )
                .await?;
                return Ok(());
            }
            let view = st
                .reseed_views
                .get(&intent)
                .ok_or_else(|| invalid("reseed stage absent"))?
                .clone();
            view.verify_admitted_storage_history(&receipt)?;
            let old_ticket = st.queue_handle(topic, part, group).await?;
            let old = old_ticket.resolve()?;
            let ready = view
                .verify_queue_learner_caught_up(
                    topic,
                    part,
                    group,
                    epoch,
                    message_target,
                    event_target,
                )
                .await?;
            let replacement_ticket = view.queue_handle(topic, part, group).await?;
            let replacement = replacement_ticket.resolve()?;
            let _lifecycle = st.lock_partition_lifecycle(topic, part, group).await;
            for (storage, expected) in [(&st, &*old), (&view, &*replacement)] {
                let registry=storage.queue_handles.load();
                if slot_lookup_no_alloc(&registry,topic,part,group)
                    .and_then(|slot|slot.handle.get())
                    .is_none_or(|handle|!std::ptr::eq(handle.as_ref(),expected)) {
                    return Err(invalid("reseed incarnation changed during verification"));
                }
            }
            let _old_apply = old.follower_apply_state().await;
            let new_apply = replacement.follower_apply_state().await;
            st.verify_admitted_storage_history(&receipt)?;
            st.ensure_partition_not_sealed(topic, part, group)?;
            old.ensure_not_recovery_sealed()?;
            replacement.ensure_not_recovery_sealed()?;
            if *new_apply
                || replacement.role()!=QueueRole::Follower
                || replacement.message_lane_epoch()!=epoch
                || replacement.event_log().current_epoch()!=epoch
                || replacement.ordered_applied_next()?!=Some(ready.event_next)
                || !matches!(old.role(), QueueRole::Follower | QueueRole::Frozen)
                || old.message_lane_epoch() != epoch
                || old.event_log().current_epoch() != epoch
                || ready.message_next < old.message_lane_next()
                || ready.event_next < old.event_log().next_offset()
                || ready.message_next != replacement.message_lane_next()
                || ready.event_next != replacement.event_log().next_offset()
            {
                return Err(invalid(
                    "reseed replacement does not cover the unchanged original history",
                ));
            }
            if let Some(wq)=replacement.as_work_queue() {
                if wq.required_message_next().await.map_err(io_err)?>replacement.payload_log().next_offset() {
                    return Err(invalid("reseed payload dependency changed during verification"));
                }
            }
            let route_dir = st.lock_recovery_dir(topic, part, group)?;
            let _lock = lock_existing_log(&route_dir).map_err(io_err)?;
            let cached = st.generations.get(&resource).map(|r| *r);
            let disk = layout::read_current(&st.partition_dir(topic, part, group))?;
            if disk != cached {
                return Err(invalid("reseed generation changed in another process"));
            }
            let original = cached.ok_or_else(|| invalid("reseed requires an existing generation"))?;
            // Keep an explicit durable pointer to the original generation. No
            // original payload, event or checkpoint file is overwritten.
            save_meta(
                &route_dir.join(format!(
                    "reseed-original-{}",
                    blake3::Hash::from_bytes(intent)
                )),
                &GenerationRecord::new(topic, part, group, original),
            )?;
            let complete_path = route_dir.join(format!(
                "reseed-complete-{}",
                blake3::Hash::from_bytes(intent)
            ));
            if let Some((saved, saved_epoch, messages, events)) =
                read_meta::<(PreparedStorageHistory, u64, u64, u64)>(&complete_path)?
            {
                if saved != receipt
                    || saved_epoch != epoch
                    || messages > ready.message_next
                    || events > ready.event_next
                {
                    return Err(invalid(
                        "reseed no longer covers its prepared completion cut",
                    ));
                }
            } else {
                save_meta(
                    &complete_path,
                    &(receipt.clone(), epoch, ready.message_next, ready.event_next),
                )?;
            }
            boundary("reseed_before_switch");
            // Cancelled callers can still hold a staging view. Revoke that
            // view under the lifecycle lock before its files become live.
            view.retired_view.store(true, Ordering::Release);
            view.admitted_histories.remove(&resource);
            for handle in [&old, &replacement] {
                handle.begin_recovery_seal();
                handle.quiesce_for_teardown().await;
                handle.cancel_background_tasks();
                handle.recovery_gate.retire_snapshots().await?;
                if let Some(wq) = handle.as_work_queue() {
                    wq.shutdown().await;
                }
                for log in handle.physical_logs() { log.shutdown().await.map_err(io_err)?; }
            }
            st.remove_queue(topic, part, group);
            view.remove_queue(topic, part, group);
            boundary("reseed_retired");
            layout::write_current(&st.partition_dir(topic, part, group), &generation)?;
            st.generations.insert(resource, generation);
            st.reseed_views.remove(&intent);
            boundary("reseed_switched");
            Ok(())
        })
        .await
        .map_err(io_err)?
    }

    /// Create the partition's recovery directory and its lock file. The
    /// returned directory can be locked with `lock_existing_log`.
    fn lock_recovery_dir(&self, topic: &str, part: u32, group: Option<&str>) -> Result<PathBuf> {
        let dir = self.recovery_dir(topic, part, group);
        layout::create_dir_within(&self.root, &dir)?;
        fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join(".keratin.lock"))
            .map_err(io_err)?;
        Ok(dir)
    }
    pub(super) fn has_recovery_installation(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<bool> {
        // A view builds or reads one generation. Installation bookkeeping
        // belongs to the partition, so it is visible only to the base storage.
        if self.view_generation.is_some() {
            return Ok(false);
        }
        self.recovery_dir(topic, part, group)
            .join("started")
            .try_exists()
            .map_err(io_err)
    }
    /// A private path-only view of one partition generation. It resolves no
    /// other partition, shares lifecycle serialization, and cannot resolve
    /// live actors or publish ordinary admission for the original storage.
    fn generation_view(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        generation: layout::Generation,
    ) -> Self {
        let mut view = self.clone();
        view.view_generation = Some(Arc::new((key(topic, part, group), generation)));
        view.generations = Arc::new(DashMap::new());
        view.reseed_views = Arc::new(DashMap::new());
        view.retired_view = Arc::new(AtomicBool::new(false));
        view.quarantined = Arc::new(DashMap::new());
        view.queue_handles = Arc::new(ArcSwap::new(Arc::new(hashbrown::HashMap::new())));
        view.admitted_histories = Arc::new(DashMap::new());
        view
    }
    pub(super) fn retained_recovery_view(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        seal: &RecoverySealRequest,
    ) -> Result<Option<Self>> {
        if self.view_generation.is_some() {
            return Ok(None);
        }
        let path = self.recovery_dir(topic, part, group).join(format!(
            "retained-{}",
            blake3::Hash::from_bytes(seal.transition)
        ));
        let Some(record) = read_meta::<GenerationRecord>(&path)? else {
            return Ok(None);
        };
        let generation = record.generation_for(topic, part, group)?;
        Ok(Some(self.generation_view(topic, part, group, generation)))
    }

    /// Fresh consensus must identify this exact local replica as excluded from
    /// the current write set. Retain and fence its old history, then publish a
    /// separate empty learner generation. This grants no broker serving role.
    pub async fn prepare_queue_learner_storage(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        binding: StorageHistoryBinding,
        intent: [u8; 32],
    ) -> Result<PreparedStorageHistory> {
        if intent == [0; 32] {
            return Err(invalid("learner generation requires an exact intent"));
        }
        let st = self.clone();
        let topic = topic.to_owned();
        let group = normalize_group(group).map(str::to_owned);
        tokio::spawn(async move {
            let current = st.storage_history_binding(&topic, part, group.as_deref())?;
            let sealed = st
                .recovery_seal_path(&topic, part, group.as_deref())
                .try_exists()
                .map_err(io_err)?;
            if current.is_none() || (current.as_ref() == Some(&binding) && !sealed) {
                return st
                    .resume_unaccepted_storage_history(&topic, part, group.as_deref(), binding)
                    .await;
            }
            if !sealed {
                let lifecycle = st
                    .lock_partition_lifecycle(&topic, part, group.as_deref())
                    .await;
                let handle = {
                    let registry = st.queue_handles.load();
                    slot_lookup_no_alloc(&registry, &topic, part, group.as_deref())
                        .and_then(|s| s.handle.get().cloned())
                };
                if let Some(handle) = handle {
                    drop(lifecycle);
                    st.resume_live_checkpoint(&topic, part, group.as_deref(), &handle)
                        .await?;
                } else {
                    let _lifecycle = st
                        .resume_cold_checkpoint_authorized(
                            &topic,
                            part,
                            group.as_deref(),
                            lifecycle,
                            true,
                        )
                        .await?;
                }
            }
            // This also drains any surviving local actor and freezes the queue log.
            let sealed = st
                .seal_replica_inner(
                    &topic,
                    part,
                    group.as_deref(),
                    PartitionKind::Queue,
                    RecoverySealRequest {
                        transition: intent,
                        fence_epoch: 0,
                    },
                    true,
                )
                .await?;
            let generation = recovery_generation(intent, st.storage_session);
            let view = st.generation_view(&topic, part, group.as_deref(), generation);
            // Preparation owns the lifecycle lock itself. The old generation
            // is durably sealed before this lock is temporarily released.
            let receipt = view
                .resume_unaccepted_storage_history(&topic, part, group.as_deref(), binding.clone())
                .await?;
            boundary("learner_prepared");
            let _lifecycle = st
                .lock_partition_lifecycle(&topic, part, group.as_deref())
                .await;
            let route_dir = st.lock_recovery_dir(&topic, part, group.as_deref())?;
            let _lock = lock_existing_log(&route_dir).map_err(io_err)?;
            let part_dir = st.partition_dir(&topic, part, group.as_deref());
            let disk = layout::read_current(&part_dir)?;
            let resource_key = key(&topic, part, group.as_deref());
            let cached = st.generations.get(&resource_key).map(|r| *r);
            if disk != cached && disk != Some(generation) {
                return Err(invalid(
                    "learner generation changed in another storage instance; reopen",
                ));
            }
            st.require_matching_recovery_seal(&topic, part, group.as_deref(), &sealed.request)?;
            if st.storage_history_binding(&topic, part, group.as_deref())? != current {
                return Err(invalid("learner source history changed during preparation"));
            }
            let old = cached.ok_or_else(|| invalid("learner requires an existing generation"))?;
            save_meta(
                &route_dir.join(format!(
                    "retained-{}",
                    blake3::Hash::from_bytes(sealed.request.transition)
                )),
                &GenerationRecord::new(&topic, part, group.as_deref(), old),
            )?;
            persist_exact(&route_dir.join("started"), b"recovery installation")?;
            let handle = {
                let registry = st.queue_handles.load();
                slot_lookup_no_alloc(&registry, &topic, part, group.as_deref())
                    .and_then(|s| s.handle.get().cloned())
            };
            if let Some(h) = handle {
                h.begin_recovery_seal();
                h.quiesce_for_teardown().await;
                let _apply = h.follower_apply_state().await;
                h.cancel_background_tasks();
                h.recovery_gate.retire_snapshots().await?;
                if let Some(wq) = h.as_work_queue() {
                    wq.shutdown().await;
                }
                for log in h.physical_logs() { log.shutdown().await.map_err(io_err)?; }
            }
            st.remove_queue(&topic, part, group.as_deref());
            st.admitted_histories.remove(&resource_key);
            boundary("learner_retired");
            view.verify_admitted_storage_history(&receipt)?;
            layout::write_current(&part_dir, &generation)?;
            boundary("learner_switched");
            st.generations.insert(resource_key.clone(), generation);
            st.quarantined.remove(&resource_key);
            st.admitted_histories.insert(resource_key, binding);
            st.index_recovered_resource(&topic, part, group.as_deref());
            st.verify_admitted_storage_history(&receipt)?;
            Ok(receipt)
        })
        .await
        .map_err(io_err)?
    }

    /// Install a verified stage after fresh external plan authorization. Old
    /// storage must already be sealed under `seal`. This grants no serving role.
    pub async fn install_queue_recovery_stage(
        &self,
        spec: QueueRecoveryStageSpec,
        seal: RecoverySealRequest,
        stage: &QueueRecoveryStage,
    ) -> Result<PreparedQueueRecovery> {
        self.validate_recovery_payload_layout(&spec)?;

        if seal.fence_epoch != spec.fence_epoch {
            return Err(invalid("installation fence differs from stage"));
        }
        let guard = stage
            .inner
            .clone()
            .try_lock_owned()
            .map_err(|_| invalid("recovery stage is busy"))?;
        if !guard.complete || guard.needs_reopen || guard.intent.spec != spec {
            return Err(invalid("installation requires the exact completed stage"));
        }
        let st = self.clone();
        tokio::spawn(async move { st.install_recovery_inner(spec, seal, guard).await })
            .await
            .map_err(io_err)?
    }

    async fn install_recovery_inner(
        &self,
        spec: QueueRecoveryStageSpec,
        seal: RecoverySealRequest,
        guard: tokio::sync::OwnedMutexGuard<recovery_stage::StageInner>,
    ) -> Result<PreparedQueueRecovery> {
        let st = self;
        let _lifecycle = st
            .lock_partition_lifecycle(&spec.topic, spec.partition, spec.group.as_deref())
            .await;
        let route_dir =
            st.lock_recovery_dir(&spec.topic, spec.partition, spec.group.as_deref())?;
        let _lock = lock_existing_log(&route_dir).map_err(io_err)?;
        // A durable anchor prevents a missing current generation from
        // becoming an empty queue after an interrupted first installation.
        let generation = recovery_generation(spec.plan, st.storage_session);
        let part_dir = st.partition_dir(&spec.topic, spec.partition, spec.group.as_deref());
        let disk_current = layout::read_current(&part_dir)?;
        let cached = st
            .generations
            .get(&key(&spec.topic, spec.partition, spec.group.as_deref()))
            .map(|r| *r);
        if disk_current != cached {
            let resource_key = key(&spec.topic, spec.partition, spec.group.as_deref());
            let live = {
                let registry = st.queue_handles.load();
                registry
                    .get(&resource_key)
                    .is_some_and(|slot| slot.handle.get().is_some())
            };
            if disk_current != Some(generation)
                || live
                || st.admitted_histories.contains_key(&resource_key)
            {
                return Err(invalid(
                    "recovery generation changed in another storage instance; reopen",
                ));
            }
            // A rename can become visible before its directory sync returns.
            // Retry that barrier for this exact unfinished process and plan.
            // Never adopt another instance's generation or reset its data.
            let view = st.generation_view(
                &spec.topic,
                spec.partition,
                spec.group.as_deref(),
                generation,
            );
            let saved: PreparedQueueRecovery = read_meta(
                &view
                    .snap_dir(&spec.topic, spec.partition, spec.group.as_deref())
                    .join("recovery.installed"),
            )?
            .ok_or_else(|| invalid("visible recovery generation lost its receipt"))?;
            if saved.spec != spec {
                return Err(invalid("visible recovery generation differs from plan"));
            }
            view.verify_prepared_storage_history(&saved.storage)?;
            layout::write_current(&part_dir, &generation)?;
            st.generations.insert(resource_key, generation);
        }
        if disk_current == Some(generation) {
            let prepared: PreparedQueueRecovery = read_meta(
                &st.snap_dir(&spec.topic, spec.partition, spec.group.as_deref())
                    .join("recovery.installed"),
            )?
            .ok_or_else(|| invalid("installed receipt is missing"))?;
            if prepared.spec != spec {
                return Err(invalid("installed baseline differs from plan"));
            }
            st.verify_prepared_queue_recovery(&prepared)?;
            st.index_recovered_partition(&spec);
            return Ok(prepared);
        }
        let same_plan = matches!(
            disk_current,
            Some(layout::Generation::Recovery { plan, .. }) if plan == spec.plan
        );
        let old_dirs = [
            st.msg_tp_part_dir(&spec.topic, spec.partition, spec.group.as_deref()),
            st.tp_part_dir(&spec.topic, spec.partition, spec.group.as_deref()),
            st.snap_dir(&spec.topic, spec.partition, spec.group.as_deref()),
        ];
        if !same_plan && old_dirs.iter().any(|p| p.exists()) {
            st.require_matching_recovery_seal(
                &spec.topic,
                spec.partition,
                spec.group.as_deref(),
                &seal,
            )?;
            // Record the source generation BEFORE switching `current`.
            let old = disk_current.ok_or_else(|| invalid("sealed data has no current generation"))?;
            save_meta(
                &route_dir.join(format!(
                    "retained-{}",
                    blake3::Hash::from_bytes(seal.transition)
                )),
                &GenerationRecord::new(&spec.topic, spec.partition, spec.group.as_deref(), old),
            )?;
        }
        persist_exact(&route_dir.join("started"), b"recovery installation")?;
        let handle = {
            let registry = st.queue_handles.load();
            slot_lookup_no_alloc(
                &registry,
                &spec.topic,
                spec.partition,
                spec.group.as_deref(),
            )
            .and_then(|slot| slot.handle.get().cloned())
        };
        if let Some(h) = &handle {
            h.begin_recovery_seal();
            h.quiesce_for_teardown().await;
            let _apply = h.follower_apply_state().await;
            h.cancel_background_tasks();
            h.recovery_gate.retire_snapshots().await?;
            if let Some(wq) = h.as_work_queue() {
                wq.shutdown().await;
            }
            for log in h.physical_logs() { log.shutdown().await.map_err(io_err)?; }
        }
        st.remove_queue(&spec.topic, spec.partition, spec.group.as_deref());
        st.admitted_histories
            .remove(&key(&spec.topic, spec.partition, spec.group.as_deref()));
        boundary("retired");
        let view = st.generation_view(
            &spec.topic,
            spec.partition,
            spec.group.as_deref(),
            generation,
        );
        let root = view.partition_root(&spec.topic, spec.partition, spec.group.as_deref());
        let receipt_path = view
            .snap_dir(&spec.topic, spec.partition, spec.group.as_deref())
            .join("recovery.installed");
        let prepared = PreparedQueueRecovery {
            spec: spec.clone(),
            storage: PreparedStorageHistory {
                topic: spec.topic.clone(),
                partition: spec.partition,
                group: spec.group.clone(),
                stream: false,
                binding: spec.binding.clone(),
                storage_instance: st.storage_session,
            },
        };
        let verification_intent = guard.intent.clone();
        let verification_limits = guard.limits;
        if let Some(saved) = read_meta::<PreparedQueueRecovery>(&receipt_path)? {
            if saved != prepared {
                return Err(invalid("conflicting installed recovery receipt"));
            }
        } else {
            // Only this unreferenced, unfinished generation may be rebuilt.
            // Completed stages and sealed old generations are immutable inputs.
            if root.exists() {
                fs::remove_dir_all(&root).map_err(io_err)?;
            }
            let target = view.recovery_payload_path(&spec);
            let (guard, target) = tokio::task::spawn_blocking(move || {
                recovery_stage::scan(&guard, true)?;
                if let Some(parent) = target.parent() { fs::create_dir_all(parent).map_err(io_err)?; }
                Ok::<_, StromaError>((guard, target))
            })
            .await
            .map_err(io_err)??;
            let stats = guard.messages.fork_frozen(target, spec.fence_epoch, spec.payload_log.head, spec.payload_log.next)
                .await.map_err(io_err)?;
            tracing::info!(topic = spec.topic, partition = spec.partition,
                shared_bytes = stats.shared_bytes, copied_bytes = stats.copied_bytes,
                shared_segments = stats.shared_segments, "recovery installation reused staged payloads");
            boundary("messages");
            let messages = view
                .open_keratin(
                    view.recovery_payload_path(&spec),
                    st.keratin_cfg_msg,
                    true,
                )
                .await
                .map_err(io_err)?;
            messages.freeze();
            let (messages, guard) = tokio::task::spawn_blocking(move || {
                recovery_stage::scan_parts(&messages, &guard.intent, guard.limits, true)?;
                Ok::<_, StromaError>((messages, guard))
            })
            .await
            .map_err(io_err)??;
            messages.shutdown().await.map_err(io_err)?;
            drop(messages);
            let writer = view.clone();
            let saved = prepared.clone();
            tokio::task::spawn_blocking(move || {
                let s = &saved.spec;
                writer.write_partition_kind(
                    &s.topic,
                    s.partition,
                    s.group.as_deref(),
                    PartitionKind::Queue,
                )?;
                writer.write_queue_snapshot_envelope(
                    &s.topic,
                    s.partition,
                    s.group.as_deref(),
                    2,
                    s.event_next,
                    &guard.intent.snapshot,
                )?;
                storage_history::persist(
                    &writer.storage_history_path(&s.topic, s.partition, s.group.as_deref()),
                    &storage_history::Receipt {
                        topic: s.topic.clone(),
                        partition: s.partition,
                        group: s.group.clone(),
                        stream: false,
                        binding: s.binding.clone(),
                        storage_session: saved.storage.storage_instance,
                    },
                )?;
                recovery_seal::sync_directories(&writer.tp_part_dir(
                    &s.topic,
                    s.partition,
                    s.group.as_deref(),
                ))?;
                save_meta(&receipt_path, &saved)?;
                boundary("prepared");
                Ok::<_, StromaError>(())
            })
            .await
            .map_err(io_err)??;
        }
        // Reopen and verify the whole baseline, including a completed
        // generation whose receipt survived an interrupted `current` switch.
        let messages = view
            .open_keratin(
                view.recovery_payload_path(&spec),
                st.keratin_cfg_msg,
                true,
            )
            .await
            .map_err(io_err)?;
        messages.freeze();
        let reader = view.clone();
        let messages = tokio::task::spawn_blocking(move || {
                recovery_stage::scan_parts(&messages, &verification_intent, verification_limits, true)?;
                let s = &verification_intent.spec;
                if !matches!(reader.read_queue_snapshot(&reader.snap_file(&s.topic, s.partition, s.group.as_deref()))?, Some((checkpoint_capture::SnapshotBoundary::ExactNext(next), ref blob)) if next == s.event_next && *blob == verification_intent.snapshot) {
                    return Err(invalid("installed snapshot differs from plan"));
                }
                Ok::<_, StromaError>(messages)
            }).await.map_err(io_err)??;
        messages.shutdown().await.map_err(io_err)?;
        drop(messages);
        // The complete generation is durable. Publish it as `current`.
        layout::write_current(&part_dir, &generation)?;
        boundary("switched");
        st.generations.insert(
            key(&spec.topic, spec.partition, spec.group.as_deref()),
            generation,
        );
        st.quarantined
            .remove(&key(&spec.topic, spec.partition, spec.group.as_deref()));
        st.verify_prepared_queue_recovery(&prepared)?;
        st.index_recovered_partition(&spec);
        Ok(prepared)
    }

    fn index_recovered_partition(&self, spec: &QueueRecoveryStageSpec) {
        self.index_recovered_resource(&spec.topic, spec.partition, spec.group.as_deref());
    }

    fn index_recovered_resource(&self, topic: &str, part: u32, group: Option<&str>) {
        let key = key(topic, part, group);
        loop {
            let current = self.queue_handles.load();
            if current
                .get(&key)
                .is_some_and(|slot| slot.exists_on_disk || slot.handle.get().is_some())
            {
                return;
            }
            let mut next = (**current).clone();
            next.insert(
                key.clone(),
                Arc::new(QueueSlot {
                    handle: OnceCell::new(),
                    exists_on_disk: true,
                    eviction_state: Arc::new(EvictionState::new()),
                }),
            );
            let previous = self
                .queue_handles
                .compare_and_swap(&current, Arc::new(next));
            if Arc::ptr_eq(&previous, &current) {
                return;
            }
        }
    }

    pub fn verify_prepared_queue_recovery(&self, prepared: &PreparedQueueRecovery) -> Result<()> {
        let s = &prepared.spec;
        self.validate_recovery_payload_layout(s)?;
        self.verify_prepared_storage_history(&prepared.storage)?;
        if prepared.storage.topic != s.topic
            || prepared.storage.partition != s.partition
            || prepared.storage.group != s.group
            || prepared.storage.stream
            || prepared.storage.binding != s.binding
        {
            return Err(invalid("recovery receipt resource or binding mismatch"));
        }
        let current = layout::read_current(&self.partition_dir(
            &s.topic,
            s.partition,
            s.group.as_deref(),
        ))?
        .ok_or_else(|| invalid("recovery generation missing"))?;
        if current != recovery_generation(s.plan, prepared.storage.storage_instance)
            || self.partition_generation(&s.topic, s.partition, s.group.as_deref())
                != Some(current)
            || read_meta::<PreparedQueueRecovery>(
                &self
                    .snap_dir(&s.topic, s.partition, s.group.as_deref())
                    .join("recovery.installed"),
            )?
            .as_ref()
                != Some(prepared)
        {
            return Err(invalid(
                "recovery receipt differs from active installed generation",
            ));
        }
        Ok(())
    }
    /// Fresh consensus activation is the caller's responsibility. Admission is
    /// exact-process only and never resets logs or rewrites the saved baseline.
    pub async fn admit_prepared_queue_recovery(
        &self,
        prepared: PreparedQueueRecovery,
    ) -> Result<()> {
        let st = self.clone();
        tokio::spawn(async move {
            let s = &prepared.spec;
            let _lifecycle = st
                .lock_partition_lifecycle(&s.topic, s.partition, s.group.as_deref())
                .await;
            let _lock = lock_existing_log(&st.recovery_dir(&s.topic, s.partition, s.group.as_deref()))
                .map_err(io_err)?;
            st.verify_prepared_queue_recovery(&prepared)?;
            if st
                .recovery_seal_path(&s.topic, s.partition, s.group.as_deref())
                .try_exists()
                .map_err(io_err)?
            {
                return Err(StromaError::RecoverySealed {
                    topic: s.topic.clone(),
                    partition: s.partition,
                    group: s.group.clone(),
                });
            }
            st.admitted_histories.insert(
                key(&s.topic, s.partition, s.group.as_deref()),
                s.binding.clone(),
            );
            Ok(())
        })
        .await
        .map_err(io_err)?
    }
}
fn boundary(_name: &str) {
    #[cfg(test)]
    if std::env::var("STROMA_INSTALL_CRASH_BOUNDARY").as_deref() == Ok(_name) {
        fs::write(
            std::env::var_os("STROMA_INSTALL_CRASH_READY").unwrap(),
            _name,
        )
        .unwrap();
        loop {
            std::thread::park();
        }
    }
}
