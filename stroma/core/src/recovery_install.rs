//! Crash-safe recovery installation: build a complete generation, then publish
//! one durable route. Sealed generations remain available for recovery reads.
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Route {
    pub topic: String,
    pub partition: u32,
    pub group: Option<String>,
    generation: Option<Generation>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Generation {
    plan: [u8; 32],
    instance: [u8; 16],
}
fn invalid(message: impl Into<String>) -> StromaError {
    StromaError::InvalidArgument(message.into())
}
fn key(topic: &str, part: u32, group: Option<&str>) -> (Box<str>, u32, Option<Box<str>>) {
    (topic.into(), part, normalize_group(group).map(Into::into))
}
fn route_id(topic: &str, part: u32, group: Option<&str>) -> String {
    let bytes =
        rmp_serde::to_vec(&(topic, part, normalize_group(group))).expect("resource serializes");
    blake3::hash(&bytes).to_hex().to_string()
}
fn read_meta<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    read_bounded(path, MAX_META)?
        .map(|b| rmp_serde::from_slice(&b).map_err(decode_err))
        .transpose()
}
fn save_meta<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    persist_exact(path, &rmp_serde::to_vec_named(value).map_err(encode_err)?)
}
impl Route {
    fn root(&self, root: &Path) -> PathBuf {
        match &self.generation {
            None => root.to_owned(),
            Some(g) => root
                .join("recovery-installed")
                .join(blake3::Hash::from_bytes(g.plan).to_hex().as_str())
                .join(uuid::Uuid::from_bytes(g.instance).to_string()),
        }
    }
    fn validate(&self) -> Result<()> {
        if self.topic.is_empty()
            || self.group.as_deref() != normalize_group(self.group.as_deref())
            || self
                .generation
                .as_ref()
                .is_some_and(|g| g.plan == [0; 32] || g.instance == [0; 16])
        {
            return Err(StromaError::Corruption(
                "invalid recovery route identity".into(),
            ));
        }
        Ok(())
    }
}
impl Stroma {
    fn route_dir(&self, topic: &str, part: u32, group: Option<&str>) -> PathBuf {
        self.root
            .join("recovery-routes")
            .join(route_id(topic, part, group))
    }
    pub(super) fn partition_root(&self, topic: &str, part: u32, group: Option<&str>) -> PathBuf {
        self.recovery_routes
            .get(&key(topic, part, group))
            .map_or_else(|| self.root.clone(), |r| r.root(&self.root))
    }
    pub(super) fn load_recovery_routes(&self) -> Result<()> {
        let dir = self.root.join("recovery-routes");
        if !dir.try_exists().map_err(io_err)? {
            return Ok(());
        }
        for entry in fs::read_dir(dir).map_err(io_err)? {
            let entry = entry.map_err(io_err)?;
            if !entry.file_type().map_err(io_err)?.is_dir() {
                return Err(invalid("unexpected recovery route entry"));
            }
            if let Some(route) = read_meta::<Route>(&entry.path().join("active"))? {
                route.validate()?;
                if route.generation.is_none()
                    || entry.file_name().to_string_lossy()
                        != route_id(&route.topic, route.partition, route.group.as_deref())
                {
                    return Err(StromaError::Corruption(
                        "recovery route resource mismatch".into(),
                    ));
                }
                self.recovery_routes.insert(
                    key(&route.topic, route.partition, route.group.as_deref()),
                    route,
                );
            }
        }
        Ok(())
    }
    pub(super) fn has_recovery_installation(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<bool> {
        self.route_dir(topic, part, group)
            .join("started")
            .try_exists()
            .map_err(io_err)
    }
    /// A private path-only view. It shares lifecycle serialization but cannot
    /// resolve live actors or publish ordinary admission for the original root.
    fn generation_view(&self, root: PathBuf) -> Self {
        let mut view = self.clone();
        view.root = root;
        view.recovery_routes = Arc::new(DashMap::new());
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
        let path = self.route_dir(topic, part, group).join(format!(
            "retained-{}",
            blake3::Hash::from_bytes(seal.transition)
        ));
        let Some(route) = read_meta::<Route>(&path)? else {
            return Ok(None);
        };
        route.validate()?;
        if route.topic != topic || route.partition != part || route.group.as_deref() != group {
            return Err(StromaError::Corruption(
                "retained route resource mismatch".into(),
            ));
        }
        Ok(Some(self.generation_view(route.root(&self.root))))
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
        if !cfg!(unix) || intent == [0; 32] {
            return Err(invalid(
                "learner generation requires durable directories and an exact intent",
            ));
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
            // This also drains any surviving local actor and freezes both logs.
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
            let route = Route {
                topic: topic.clone(),
                partition: part,
                group: group.clone(),
                generation: Some(Generation {
                    plan: intent,
                    instance: st.storage_session,
                }),
            };
            let view = st.generation_view(route.root(&st.root));
            // Preparation owns the lifecycle lock itself. The old generation
            // is durably sealed before this lock is temporarily released.
            let receipt = view
                .resume_unaccepted_storage_history(&topic, part, group.as_deref(), binding.clone())
                .await?;
            boundary("learner_prepared");
            let _lifecycle = st
                .lock_partition_lifecycle(&topic, part, group.as_deref())
                .await;
            let route_dir = st.route_dir(&topic, part, group.as_deref());
            fs::create_dir_all(&route_dir).map_err(io_err)?;
            fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(route_dir.join(".keratin.lock"))
                .map_err(io_err)?;
            let _lock = lock_existing_log(&route_dir).map_err(io_err)?;
            let disk: Option<Route> = read_meta(&route_dir.join("active"))?;
            let resource_key = key(&topic, part, group.as_deref());
            let cached = st.recovery_routes.get(&resource_key).map(|r| r.clone());
            if disk != cached && disk.as_ref() != Some(&route) {
                return Err(invalid(
                    "learner route changed in another storage instance; reopen",
                ));
            }
            st.require_matching_recovery_seal(&topic, part, group.as_deref(), &sealed.request)?;
            if st.storage_history_binding(&topic, part, group.as_deref())? != current {
                return Err(invalid("learner source history changed during preparation"));
            }
            let old = cached.unwrap_or(Route {
                topic: topic.clone(),
                partition: part,
                group: group.clone(),
                generation: None,
            });
            save_meta(
                &route_dir.join(format!(
                    "retained-{}",
                    blake3::Hash::from_bytes(sealed.request.transition)
                )),
                &old,
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
                h.msg_log().shutdown().await.map_err(io_err)?;
                h.event_log().shutdown().await.map_err(io_err)?;
            }
            st.remove_queue(&topic, part, group.as_deref());
            st.admitted_histories.remove(&resource_key);
            boundary("learner_retired");
            view.verify_admitted_storage_history(&receipt)?;
            let scratch = route_dir.join("next");
            if scratch.try_exists().map_err(io_err)? {
                fs::remove_file(&scratch).map_err(io_err)?;
            }
            save_meta(&scratch, &route)?;
            fs::rename(&scratch, route_dir.join("active")).map_err(io_err)?;
            recovery_seal::sync_directories(&route_dir)?;
            boundary("learner_switched");
            st.recovery_routes.insert(resource_key.clone(), route);
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
        let route_dir = st.route_dir(&spec.topic, spec.partition, spec.group.as_deref());
        fs::create_dir_all(&route_dir).map_err(io_err)?;
        fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(route_dir.join(".keratin.lock"))
            .map_err(io_err)?;
        let _lock = lock_existing_log(&route_dir).map_err(io_err)?;
        // A durable anchor prevents a missing active pointer from becoming
        // a legacy empty queue after an interrupted first installation.
        let route = Route {
            topic: spec.topic.clone(),
            partition: spec.partition,
            group: spec.group.clone(),
            generation: Some(Generation {
                plan: spec.plan,
                instance: st.storage_session,
            }),
        };
        let disk_route: Option<Route> = read_meta(&route_dir.join("active"))?;
        let cached = st
            .recovery_routes
            .get(&key(&spec.topic, spec.partition, spec.group.as_deref()))
            .map(|r| r.clone());
        if disk_route != cached {
            let resource_key = key(&spec.topic, spec.partition, spec.group.as_deref());
            let live = {
                let registry = st.queue_handles.load();
                registry
                    .get(&resource_key)
                    .is_some_and(|slot| slot.handle.get().is_some())
            };
            if disk_route.as_ref() != Some(&route)
                || live
                || st.admitted_histories.contains_key(&resource_key)
            {
                return Err(invalid(
                    "recovery route changed in another storage instance; reopen",
                ));
            }
            // A rename can become visible before its directory sync returns.
            // Retry that barrier for this exact unfinished process/plan;
            // never adopt another instance's route or reset its data.
            let view = st.generation_view(route.root(&st.root));
            let saved: PreparedQueueRecovery = read_meta(
                &view
                    .snap_dir(&spec.topic, spec.partition, spec.group.as_deref())
                    .join("recovery.installed"),
            )?
            .ok_or_else(|| invalid("visible recovery route lost its receipt"))?;
            if saved.spec != spec {
                return Err(invalid("visible recovery route differs from plan"));
            }
            view.verify_prepared_storage_history(&saved.storage)?;
            fs::File::open(route_dir.join("active"))
                .and_then(|f| f.sync_all())
                .map_err(io_err)?;
            recovery_seal::sync_directories(&route_dir)?;
            st.recovery_routes.insert(resource_key, route.clone());
        }
        if disk_route.as_ref() == Some(&route) {
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
        let same_plan = disk_route
            .as_ref()
            .and_then(|r| r.generation.as_ref())
            .is_some_and(|g| g.plan == spec.plan);
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
            // Record the source location BEFORE switching the active route.
            let old = disk_route.clone().unwrap_or(Route {
                topic: spec.topic.clone(),
                partition: spec.partition,
                group: spec.group.clone(),
                generation: None,
            });
            save_meta(
                &route_dir.join(format!(
                    "retained-{}",
                    blake3::Hash::from_bytes(seal.transition)
                )),
                &old,
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
            h.msg_log().shutdown().await.map_err(io_err)?;
            h.event_log().shutdown().await.map_err(io_err)?;
        }
        st.remove_queue(&spec.topic, spec.partition, spec.group.as_deref());
        st.admitted_histories
            .remove(&key(&spec.topic, spec.partition, spec.group.as_deref()));
        boundary("retired");
        let root = route.root(&st.root);
        let view = st.generation_view(root.clone());
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
            let target = view.msg_tp_part_dir(&spec.topic, spec.partition, spec.group.as_deref());
            let guard = tokio::task::spawn_blocking(move || {
                recovery_stage::scan(&guard, true)?;
                copy_log(&guard.root.join("messages"), &target)?;
                boundary("messages");
                Ok::<_, StromaError>(guard)
            })
            .await
            .map_err(io_err)??;
            let messages = view
                .open_keratin(
                    view.msg_tp_part_dir(&spec.topic, spec.partition, spec.group.as_deref()),
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
            let events = view
                .open_keratin(
                    view.tp_part_dir(&spec.topic, spec.partition, spec.group.as_deref()),
                    st.keratin_cfg_event,
                    false,
                )
                .await
                .map_err(io_err)?;
            events.become_follower();
            events
                .advance_epoch(spec.fence_epoch)
                .await
                .map_err(io_err)?;
            events
                .destructive_reset_to_checkpoint_at_epoch(spec.event_next, spec.fence_epoch)
                .await
                .map_err(io_err)?;
            events.sync().await.map_err(io_err)?;
            events.shutdown().await.map_err(io_err)?;
            drop(events);
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
                recovery_seal::sync_directories(&writer.msg_tp_part_dir(
                    &s.topic,
                    s.partition,
                    s.group.as_deref(),
                ))?;
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
        // generation whose receipt survived an interrupted route switch.
        let messages = view
            .open_keratin(
                view.msg_tp_part_dir(&spec.topic, spec.partition, spec.group.as_deref()),
                st.keratin_cfg_msg,
                true,
            )
            .await
            .map_err(io_err)?;
        messages.freeze();
        let events = view
            .open_keratin(
                view.tp_part_dir(&spec.topic, spec.partition, spec.group.as_deref()),
                st.keratin_cfg_event,
                true,
            )
            .await
            .map_err(io_err)?;
        if events.head_offset() != spec.event_next
            || events.next_offset() != spec.event_next
            || events.current_epoch() != spec.fence_epoch
        {
            return Err(invalid("installed event baseline differs from plan"));
        }
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
        events.shutdown().await.map_err(io_err)?;
        drop(messages);
        drop(events);
        // fsync the complete generation before atomically publishing its route.
        let scratch = route_dir.join("next");
        if scratch.exists() {
            fs::remove_file(&scratch).map_err(io_err)?;
        }
        save_meta(&scratch, &route)?;
        fs::rename(&scratch, route_dir.join("active")).map_err(io_err)?;
        recovery_seal::sync_directories(&route_dir)?;
        boundary("switched");
        st.recovery_routes.insert(
            key(&spec.topic, spec.partition, spec.group.as_deref()),
            route,
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
        self.verify_prepared_storage_history(&prepared.storage)?;
        if prepared.storage.topic != s.topic
            || prepared.storage.partition != s.partition
            || prepared.storage.group != s.group
            || prepared.storage.stream
            || prepared.storage.binding != s.binding
        {
            return Err(invalid("recovery receipt resource or binding mismatch"));
        }
        let route = read_meta::<Route>(
            &self
                .route_dir(&s.topic, s.partition, s.group.as_deref())
                .join("active"),
        )?
        .ok_or_else(|| invalid("recovery route missing"))?;
        if route.generation
            != Some(Generation {
                plan: s.plan,
                instance: prepared.storage.storage_instance,
            })
            || self
                .recovery_routes
                .get(&key(&s.topic, s.partition, s.group.as_deref()))
                .as_deref()
                != Some(&route)
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
            let _lock = lock_existing_log(&st.route_dir(&s.topic, s.partition, s.group.as_deref()))
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
fn copy_log(source: &Path, target: &Path) -> Result<()> {
    copy_log_depth(source, target, 0)
}
fn copy_log_depth(source: &Path, target: &Path, depth: usize) -> Result<()> {
    if depth > 8 {
        return Err(invalid("staged log directory nesting exceeds limit"));
    }
    fs::create_dir_all(target).map_err(io_err)?;
    for entry in fs::read_dir(source).map_err(io_err)? {
        let entry = entry.map_err(io_err)?;
        if entry.file_name() == ".keratin.lock" {
            continue;
        }
        let dest = target.join(entry.file_name());
        let kind = entry.file_type().map_err(io_err)?;
        if kind.is_dir() {
            copy_log_depth(&entry.path(), &dest, depth + 1)?;
            continue;
        }
        if !kind.is_file() {
            return Err(invalid("nonregular staged log entry"));
        }
        fs::copy(entry.path(), &dest).map_err(io_err)?;
        fs::File::open(dest)
            .and_then(|f| f.sync_all())
            .map_err(io_err)?;
    }
    recovery_seal::sync_directories(target)
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
