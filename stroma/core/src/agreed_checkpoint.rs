//! Durable replay bases and retention for background checkpoint agreement.
//! These records grant no history or serving authority. The caller must obtain
//! fresh consensus authorization before creating or releasing retention pins.
use super::*;
use crate::QueueInternalState;
use std::{
    collections::BTreeMap,
    io::{Read, Write},
};

const MAGIC: &[u8; 8] = b"QCPIN\0\0\x01";
const MAX_INDEX: usize = 65_536;
const MAX_SNAPSHOT: usize = 16 * 1024 * 1024;
const MAX_BASE: usize = 2 * MAX_SNAPSHOT + MAX_INDEX;
const MAX_PINS: usize = 2; // Last accepted checkpoint and one candidate.

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueCheckpointPin {
    pub attempt: [u8; 32],
    pub storage: PreparedStorageHistory,
    pub event_epoch: u64,
    pub message_epoch: u64,
    /// Fully applied, exclusive event boundary, including the empty cut at zero.
    pub event_next: u64,
    /// Conservative payload retention, including not-yet-applied enqueues.
    pub message_head: u64,
    pub message_next: u64,
    pub required_message_next: u64,
    pub snapshot_digest: [u8; 32],
    pub state_digest: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueCheckpointBase {
    pub pin: QueueCheckpointPin,
    pub snapshot: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Retention {
    version: u32,
    pins: BTreeMap<String, QueueCheckpointPin>,
    #[serde(default)]
    accepted: Option<AcceptedCheckpoint>,
}
impl Default for Retention {
    fn default() -> Self {
        Self {
            version: 1,
            pins: BTreeMap::new(),
            accepted: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AcceptedCheckpoint {
    certificate: [u8; 32],
    capsule: [u8; 32],
    attempt: [u8; 32],
    contents: QueueCheckpointContents,
}

fn invalid(message: impl Into<String>) -> StromaError {
    StromaError::InvalidArgument(message.into())
}
fn corrupt(message: impl Into<String>) -> StromaError {
    StromaError::Corruption(message.into())
}
fn name(attempt: [u8; 32]) -> String {
    blake3::Hash::from_bytes(attempt).to_string()
}
fn base_path(root: &Path, attempt: [u8; 32]) -> PathBuf {
    root.join(format!("{}.base", name(attempt)))
}

fn read<T: serde::de::DeserializeOwned>(path: &Path, max: usize) -> Result<Option<T>> {
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io_err(e)),
    };
    let mut bytes = Vec::new();
    file.take(max as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(io_err)?;
    if bytes.len() < 12 || bytes.len() > max || &bytes[..8] != MAGIC {
        return Err(corrupt("invalid checkpoint retention header or size"));
    }
    let end = bytes.len() - 4;
    if crc32c::crc32c(&bytes[..end]) != u32::from_be_bytes(bytes[end..].try_into().unwrap()) {
        return Err(corrupt("checkpoint retention checksum mismatch"));
    }
    rmp_serde::from_slice(&bytes[8..end])
        .map(Some)
        .map_err(|e| corrupt(e.to_string()))
}

/// Caller holds the partition lifecycle/apply locks, including after cancellation.
fn persist<T: Serialize>(path: &Path, value: &T, max: usize) -> Result<()> {
    let mut bytes = MAGIC.to_vec();
    bytes.extend(rmp_serde::to_vec_named(value).map_err(encode_err)?);
    bytes.extend(crc32c::crc32c(&bytes).to_be_bytes());
    if bytes.len() > max {
        return Err(invalid("checkpoint retention metadata exceeds limit"));
    }
    let root = path.parent().unwrap();
    fs::create_dir_all(root).map_err(io_err)?;
    let scratch = root.join("retention.writing");
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&scratch)
        .map_err(io_err)?;
    file.write_all(&bytes).map_err(io_err)?;
    file.sync_all().map_err(io_err)?;
    drop(file);
    fs::rename(&scratch, path).map_err(io_err)?;
    recovery_seal::sync_directories(root)
}

/// Reclaim only canonical capsule/base files whose durable pins are gone.
/// Bounded work also cleans unpublished files left by interruption.
fn reclaim_unpinned(root: &Path, index: &Retention) -> Result<()> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(io_err(e)),
    };
    let mut removed = false;
    for entry in entries.take(128) {
        let entry = entry.map_err(io_err)?;
        let file = entry.file_name();
        let Some(file) = file.to_str() else {
            continue;
        };
        let Some((id, extension)) = file.rsplit_once('.') else {
            continue;
        };
        if !matches!(extension, "base" | "capsule")
            || id.len() != 64
            || !id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || index.pins.contains_key(id)
        {
            continue;
        }
        fs::remove_file(entry.path()).map_err(io_err)?;
        removed = true;
    }
    if removed {
        recovery_seal::sync_directories(root)?;
    }
    Ok(())
}

fn validate_pin(pin: &QueueCheckpointPin) -> Result<()> {
    let s = &pin.storage;
    if pin.attempt == [0; 32]
        || s.stream
        || s.topic.is_empty()
        || s.group.as_deref() != normalize_group(s.group.as_deref())
        || [
            s.storage_instance,
            s.binding.resource_incarnation,
            s.binding.accepted_history,
            s.binding.writer_session,
        ]
        .contains(&[0; 16])
        || pin.message_head > pin.message_next
        || pin.required_message_next > pin.message_next
    {
        return Err(corrupt("invalid checkpoint pin identity or boundaries"));
    }
    Ok(())
}
fn load_index(root: &Path, topic: &str, part: u32, group: Option<&str>) -> Result<Retention> {
    let index: Retention = read(&root.join("retention"), MAX_INDEX)?.unwrap_or_default();
    if index.version != 1 || index.pins.len() > MAX_PINS {
        return Err(corrupt(
            "unsupported checkpoint retention version or pin count",
        ));
    }
    for (key, pin) in &index.pins {
        validate_pin(pin)?;
        if *key != name(pin.attempt)
            || pin.storage.topic != topic
            || pin.storage.partition != part
            || pin.storage.group.as_deref() != group
        {
            return Err(corrupt("checkpoint pin belongs to another resource"));
        }
    }
    if let Some(accepted) = &index.accepted {
        accepted.contents.validate().map_err(corrupt)?;
        if accepted.certificate == [0; 32]
            || accepted.capsule == [0; 32]
            || !index.pins.contains_key(&name(accepted.attempt))
        {
            return Err(corrupt("accepted checkpoint has no retained identity"));
        }
    }
    Ok(index)
}
fn load_base(root: &Path, pin: &QueueCheckpointPin) -> Result<QueueCheckpointBase> {
    let base: QueueCheckpointBase = read(&base_path(root, pin.attempt), MAX_BASE)?
        .ok_or_else(|| corrupt("checkpoint retention lost its replay base"))?;
    if base.pin != *pin
        || base.snapshot.len() > MAX_SNAPSHOT
        || *blake3::hash(&base.snapshot).as_bytes() != pin.snapshot_digest
    {
        return Err(corrupt(
            "checkpoint base differs from durable retention record",
        ));
    }
    let mut state = QueueInternalState::new(pin.storage.topic.clone(), pin.storage.partition);
    let meta = state
        .load_snapshot(&base.snapshot)
        .map_err(|e| corrupt(e.to_string()))?;
    if meta.last_snapshot_event_offset != pin.event_next.saturating_sub(1)
        || state.required_message_next() != pin.required_message_next
        || state.recovery_lease_normalized_digest() != pin.state_digest
        || state
            .recovery_live_ranges()
            .iter()
            .any(|r| r.start < pin.message_head || r.end > pin.message_next)
    {
        return Err(corrupt(
            "checkpoint base state or payload coverage is invalid",
        ));
    }
    Ok(base)
}

impl Stroma {
    fn checkpoint_pin_root(&self, topic: &str, part: u32, group: Option<&str>) -> PathBuf {
        self.snap_dir(topic, part, group).join("agreed-checkpoints")
    }

    /// Capture one local replay starting point and durably pin its dependencies.
    /// It is not yet a common-cut capsule or an agreement receipt. Ordinary
    /// snapshots may advance; compaction respects the separate retained base.
    pub async fn begin_queue_checkpoint_pin(
        &self,
        storage: PreparedStorageHistory,
        attempt: [u8; 32],
    ) -> Result<QueueCheckpointBase> {
        if !cfg!(unix) {
            return Err(StromaError::Unsupported(
                "checkpoint retention requires Unix durable metadata".into(),
            ));
        }
        if storage.stream || attempt == [0; 32] {
            return Err(invalid("queue checkpoint pin requires a nonzero attempt"));
        }
        let stroma = self.clone();
        tokio::spawn(async move {
            loop {
                stroma.verify_admitted_storage_history(&storage)?;
                let ticket = stroma
                    .queue_handle(&storage.topic, storage.partition, storage.group.as_deref())
                    .await?;
                let _lifecycle = stroma
                    .lock_partition_lifecycle(
                        &storage.topic,
                        storage.partition,
                        storage.group.as_deref(),
                    )
                    .await;
                let h = ticket.resolve()?;
                let current = stroma.queue_handles.load();
                if !slot_lookup_no_alloc(
                    &current,
                    &storage.topic,
                    storage.partition,
                    storage.group.as_deref(),
                )
                .and_then(|slot| slot.handle.get())
                .is_some_and(|inner| std::ptr::eq(inner.as_ref(), &*h))
                {
                    continue;
                }
                drop(current);
                let apply = h.follower_apply_state().await;
                stroma.verify_admitted_storage_history(&storage)?;
                stroma.verify_prepared_storage_history(&storage)?;
                h.ensure_not_recovery_sealed()?;
                if *apply {
                    return Err(invalid("checkpoint pin blocked by interrupted application"));
                }
                h.work_queue()?;
                if h.role() == QueueRole::Follower {
                    h.align_follower_checkpoint_boundary()?;
                }
                let root = stroma.checkpoint_pin_root(
                    &storage.topic,
                    storage.partition,
                    storage.group.as_deref(),
                );
                let root_read = root.clone();
                let lookup = storage.clone();
                let (mut index, existing) = tokio::task::spawn_blocking(move || {
                    let index = load_index(
                        &root_read,
                        &lookup.topic,
                        lookup.partition,
                        lookup.group.as_deref(),
                    )?;
                    let existing = index
                        .pins
                        .get(&name(attempt))
                        .map(|pin| load_base(&root_read, pin))
                        .transpose()?;
                    Ok::<_, StromaError>((index, existing))
                })
                .await
                .map_err(io_err)??;
                if let Some(base) = existing {
                    if base.pin.storage != storage
                        || base.pin.event_epoch != h.event_log().current_epoch()
                        || base.pin.message_epoch != h.msg_log().current_epoch()
                        || base.pin.event_next < h.event_log().head_offset()
                        || base.pin.message_head < h.msg_log().head_offset()
                    {
                        return Err(invalid("checkpoint pin no longer matches local history"));
                    }
                    // Retry a possibly published index after failed directory sync.
                    tokio::task::spawn_blocking(move || {
                        fs::File::open(base_path(&root, attempt))
                            .and_then(|f| f.sync_all())
                            .map_err(io_err)?;
                        persist(&root.join("retention"), &index, MAX_INDEX)?;
                        Ok::<_, StromaError>(())
                    })
                    .await
                    .map_err(io_err)??;
                    return Ok(base);
                }
                if index.pins.len() >= MAX_PINS {
                    return Err(invalid("checkpoint retention already has two pins"));
                }
                // Drain queued mutations whose callers may have been cancelled
                // after sending a truncate but before observing its completion.
                let messages = h.msg_log();
                let events = h.event_log();
                tokio::try_join!(messages.sync(), events.sync()).map_err(io_err)?;
                let captured = h.capture_exact_checkpoint(false).await?;
                let pin = QueueCheckpointPin {
                    attempt,
                    storage: storage.clone(),
                    event_epoch: h.event_log().current_epoch(),
                    message_epoch: h.msg_log().current_epoch(),
                    event_next: captured.event_next,
                    message_head: h.msg_log().head_offset(),
                    message_next: h.msg_log().next_offset(),
                    required_message_next: captured.state.required_message_next(),
                    snapshot_digest: [0; 32],
                    state_digest: [0; 32],
                };
                let writer = stroma.clone();
                return tokio::task::spawn_blocking(move || {
                    let mut pin = pin;
                    pin.state_digest = captured.state.recovery_lease_normalized_digest();
                    let snapshot = captured.state.into_recovery_snapshot(pin.event_next);
                    if snapshot.len() > MAX_SNAPSHOT {
                        return Err(invalid("checkpoint snapshot exceeds limit"));
                    }
                    pin.snapshot_digest = *blake3::hash(&snapshot).as_bytes();
                    validate_pin(&pin)?;
                    let base = QueueCheckpointBase {
                        pin: pin.clone(),
                        snapshot,
                    };
                    // Unpublished scratch from an earlier interruption is replaceable.
                    // An indexed base was handled above and is never overwritten.
                    persist(&base_path(&root, attempt), &base, MAX_BASE)?;
                    writer.checkpoint_boundary("agreement_base")?;
                    index.pins.insert(name(attempt), pin);
                    persist(&root.join("retention"), &index, MAX_INDEX)?;
                    writer.checkpoint_boundary("agreement_pin")?;
                    load_base(&root, &base.pin)
                })
                .await
                .map_err(io_err)?;
            }
        })
        .await
        .map_err(io_err)?
    }

    /// Read retained evidence without reopening or admitting a writer. Restarted
    /// replicas may inspect it under their original durable storage identity.
    pub async fn read_queue_checkpoint_base(
        &self,
        pin: QueueCheckpointPin,
    ) -> Result<QueueCheckpointBase> {
        let stroma = self.clone();
        tokio::spawn(async move {
            let s = &pin.storage;
            let _lifecycle = stroma
                .lock_partition_lifecycle(&s.topic, s.partition, s.group.as_deref())
                .await;
            if stroma
                .durable_storage_history_receipt(&s.topic, s.partition, s.group.as_deref())?
                .as_ref()
                != Some(s)
            {
                return Err(invalid("checkpoint base storage identity changed"));
            }
            let root = stroma.checkpoint_pin_root(&s.topic, s.partition, s.group.as_deref());
            tokio::task::spawn_blocking(move || {
                let s = &pin.storage;
                let index = load_index(&root, &s.topic, s.partition, s.group.as_deref())?;
                if index.pins.get(&name(pin.attempt)) != Some(&pin) {
                    return Err(invalid("checkpoint base is not durably retained"));
                }
                load_base(&root, &pin)
            })
            .await
            .map_err(io_err)?
        })
        .await
        .map_err(io_err)?
    }

    /// Remove a pin only after fresh external authority proves it is abandoned
    /// or superseded by a durable accepted checkpoint. This method cannot infer
    /// that from time, liveness or matching offsets. Base files remain inert for
    /// future reclamation; only the durable retention obligation is removed.
    pub async fn release_queue_checkpoint_pin(&self, pin: QueueCheckpointPin) -> Result<()> {
        let stroma = self.clone();
        tokio::spawn(async move {
            let s = &pin.storage;
            stroma.verify_admitted_storage_history(s)?;
            let ticket = stroma
                .queue_handle(&s.topic, s.partition, s.group.as_deref())
                .await?;
            let _lifecycle = stroma
                .lock_partition_lifecycle(&s.topic, s.partition, s.group.as_deref())
                .await;
            let h = ticket.resolve()?;
            let current = stroma.queue_handles.load();
            if !slot_lookup_no_alloc(&current, &s.topic, s.partition, s.group.as_deref())
                .and_then(|slot| slot.handle.get())
                .is_some_and(|inner| std::ptr::eq(inner.as_ref(), &*h))
            {
                return Err(invalid("checkpoint pin handle was retired; retry"));
            }
            drop(current);
            let _apply = h.follower_apply_state().await;
            stroma.verify_admitted_storage_history(s)?;
            stroma.verify_prepared_storage_history(s)?;
            h.ensure_not_recovery_sealed()?;
            let root = stroma.checkpoint_pin_root(&s.topic, s.partition, s.group.as_deref());
            tokio::task::spawn_blocking(move || {
                let s = &pin.storage;
                let mut index = load_index(&root, &s.topic, s.partition, s.group.as_deref())?;
                if index
                    .accepted
                    .as_ref()
                    .is_some_and(|a| a.attempt == pin.attempt)
                {
                    return Err(invalid(
                        "accepted checkpoint can only be released by replacement",
                    ));
                }
                let key = name(pin.attempt);
                match index.pins.get(&key) {
                    Some(current) if current == &pin => {
                        index.pins.remove(&key);
                    }
                    Some(_) => {
                        return Err(invalid("checkpoint release conflicts with retained pin"));
                    }
                    None => return Ok(()),
                }
                persist(&root.join("retention"), &index, MAX_INDEX)?;
                reclaim_unpinned(&root, &index)
            })
            .await
            .map_err(io_err)?
        })
        .await
        .map_err(io_err)?
    }

    /// Caller holds follower_apply through the eventual truncate commands. Read
    /// the small durable index only at compaction, with no append-path atomics.
    /// Corrupt/unreadable retention blocks compaction rather than dropping pins.
    pub(super) async fn checkpoint_retention_limits(
        &self,
        h: &QueueHandleInner,
        message: u64,
        event: u64,
    ) -> Result<(u64, u64)> {
        if h.as_work_queue().is_none() {
            return Ok((message, event));
        }
        let root = self.checkpoint_pin_root(h.topic(), h.partition(), h.group());
        let (topic, part, group) = (
            h.topic().to_owned(),
            h.partition(),
            h.group().map(str::to_owned),
        );
        tokio::task::spawn_blocking(move || {
            let index = load_index(&root, &topic, part, group.as_deref())?;
            Ok(index.pins.values().fold((message, event), |(m, e), pin| {
                let accepted = index.accepted.as_ref().filter(|a| a.attempt == pin.attempt);
                (
                    m.min(accepted.map_or(pin.message_head, |a| a.contents.message_head)),
                    e.min(accepted.map_or(pin.event_next, |a| a.contents.event_next)),
                )
            }))
        })
        .await
        .map_err(io_err)?
    }
}

#[cfg(all(test, unix, feature = "ordered-queue-apply"))]
#[path = "agreed_checkpoint_tests.rs"]
mod tests;

/// The common cut chosen after all replicas have pinned replay bases. No field
/// is a serving/confirmation authorization; offsets use independent log spaces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueCheckpointTarget {
    pub event_next: u64,
    pub message_head: u64,
    pub message_next: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueCheckpointContents {
    pub event_next: u64,
    pub message_head: u64,
    pub message_next: u64,
    pub required_message_next: u64,
    pub snapshot_digest: [u8; 32],
    pub state_digest: [u8; 32],
    pub message_digest: [u8; 32],
    pub live_payload_digest: [u8; 32],
}
impl QueueCheckpointContents {
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.message_head > self.message_next || self.required_message_next > self.message_next {
            return Err("checkpoint payload bounds do not cover state dependencies".into());
        }
        Ok(())
    }
    pub fn target(&self) -> QueueCheckpointTarget {
        QueueCheckpointTarget {
            event_next: self.event_next,
            message_head: self.message_head,
            message_next: self.message_next,
        }
    }
}

/// A locally durable, verified same-cut capsule. Fresh metadata and authenticated
/// unanimous receipts are still required before it becomes an agreed checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueCheckpointCapsule {
    version: u32,
    pub pin: QueueCheckpointPin,
    pub contents: QueueCheckpointContents,
    pub snapshot: Vec<u8>,
}
impl QueueCheckpointCapsule {
    pub fn digest(&self) -> Result<[u8; 32]> {
        let mut h = blake3::Hasher::new();
        h.update(b"stroma-queue-checkpoint-capsule-v1\0");
        h.update(&rmp_serde::to_vec_named(self).map_err(encode_err)?);
        Ok(*h.finalize().as_bytes())
    }
    fn validate(&self) -> Result<()> {
        validate_pin(&self.pin)?;
        let c = &self.contents;
        c.validate().map_err(invalid)?;
        if self.version != 1
            || c.event_next < self.pin.event_next
            || c.message_head < self.pin.message_head
            || c.message_next < self.pin.message_next
            || self.snapshot.len() > MAX_SNAPSHOT
            || *blake3::hash(&self.snapshot).as_bytes() != c.snapshot_digest
        {
            return Err(corrupt(
                "checkpoint capsule bounds or snapshot identity mismatch",
            ));
        }
        let mut state =
            QueueInternalState::new(self.pin.storage.topic.clone(), self.pin.storage.partition);
        let meta = state
            .load_snapshot(&self.snapshot)
            .map_err(|e| corrupt(e.to_string()))?;
        if meta.last_snapshot_event_offset != c.event_next.saturating_sub(1)
            || state.required_message_next() != c.required_message_next
            || state.recovery_state_digest() != c.state_digest
            || state
                .recovery_live_ranges()
                .iter()
                .any(|r| r.start < c.message_head || r.end > c.message_next)
        {
            return Err(corrupt(
                "checkpoint capsule state or payload dependencies mismatch",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct QueueCheckpointBuildLimits {
    pub records: u64,
    pub bytes: u64,
    pub elapsed: Duration,
}
impl Default for QueueCheckpointBuildLimits {
    fn default() -> Self {
        Self {
            records: 1_000_000,
            bytes: 256 * 1024 * 1024,
            elapsed: Duration::from_secs(5),
        }
    }
}
fn capsule_path(root: &Path, attempt: [u8; 32]) -> PathBuf {
    root.join(format!("{}.capsule", name(attempt)))
}

impl Stroma {
    /// Choose a complete owner cut after peer base capture. Only local owner
    /// operations drain; no peer wait occurs under the pause or application lock.
    pub async fn queue_checkpoint_target(
        &self,
        storage: PreparedStorageHistory,
        minimum_event_next: u64,
        minimum_message_head: u64,
        minimum_message_next: u64,
    ) -> Result<QueueCheckpointTarget> {
        let stroma = self.clone();
        tokio::spawn(async move {
            stroma.verify_admitted_storage_history(&storage)?;
            let ticket = stroma
                .queue_handle(&storage.topic, storage.partition, storage.group.as_deref())
                .await?;
            let _lifecycle = stroma
                .lock_partition_lifecycle(
                    &storage.topic,
                    storage.partition,
                    storage.group.as_deref(),
                )
                .await;
            let h = ticket.resolve()?;
            let current = stroma.queue_handles.load();
            if !slot_lookup_no_alloc(
                &current,
                &storage.topic,
                storage.partition,
                storage.group.as_deref(),
            )
            .and_then(|slot| slot.handle.get())
            .is_some_and(|inner| std::ptr::eq(inner.as_ref(), &*h))
            {
                return Err(invalid("checkpoint target handle was retired; retry"));
            }
            drop(current);
            stroma.verify_prepared_storage_history(&storage)?;
            h.ensure_not_recovery_sealed()?;
            let apply = h.follower_apply_state().await;
            if *apply {
                return Err(invalid(
                    "checkpoint target blocked by incomplete application",
                ));
            }
            stroma.verify_admitted_storage_history(&storage)?;
            h.ensure_owner()?;
            let _paused = h.pause_owner_operations_and_wait().await?;
            let captured = h.capture_exact_checkpoint(false).await?;
            let message_next = h.msg_log().next_offset();
            if captured.event_next < minimum_event_next
                || message_next < minimum_message_next
                || captured.event_next != h.event_log().next_offset()
            {
                return Err(invalid("owner has not reached every pinned replay base"));
            }
            let message_head = minimum_message_head
                .max(captured.message_floor)
                .max(h.msg_log().head_offset());
            if message_head > message_next {
                return Err(invalid("checkpoint payload floor exceeds owner tail"));
            }
            Ok(QueueCheckpointTarget {
                event_next: captured.event_next,
                message_head,
                message_next,
            })
        })
        .await
        .map_err(io_err)?
    }

    /// Replay to a fixed cut and verify physical payload bytes while publishing
    /// and follower application continue. The lifecycle lock keeps pins and log
    /// generations stable; the application lock is released during the scan.
    pub async fn build_queue_checkpoint_capsule(
        &self,
        pin: QueueCheckpointPin,
        target: QueueCheckpointTarget,
        limits: QueueCheckpointBuildLimits,
    ) -> Result<QueueCheckpointCapsule> {
        if limits.records == 0
            || limits.records > 1_000_000
            || limits.bytes == 0
            || limits.bytes > 1024 * 1024 * 1024
            || limits.elapsed.is_zero()
            || limits.elapsed > Duration::from_secs(30)
        {
            return Err(invalid("invalid checkpoint build limits"));
        }
        let stroma = self.clone();
        tokio::spawn(async move {
            let s = &pin.storage;
            stroma.verify_admitted_storage_history(s)?;
            let ticket = stroma
                .queue_handle(&s.topic, s.partition, s.group.as_deref())
                .await?;
            let _lifecycle = stroma
                .lock_partition_lifecycle(&s.topic, s.partition, s.group.as_deref())
                .await;
            let h = ticket.resolve()?;
            let current = stroma.queue_handles.load();
            if !slot_lookup_no_alloc(&current, &s.topic, s.partition, s.group.as_deref())
                .and_then(|slot| slot.handle.get())
                .is_some_and(|inner| std::ptr::eq(inner.as_ref(), &*h))
            {
                return Err(invalid("checkpoint handle was retired; retry"));
            }
            drop(current);
            let apply = h.follower_apply_state().await;
            h.ensure_not_recovery_sealed()?;
            stroma.verify_admitted_storage_history(s)?;
            stroma.verify_prepared_storage_history(s)?;
            if *apply
                || h.ordered_applied_next()?
                    .is_none_or(|n| n < target.event_next)
            {
                return Err(invalid(
                    "replica has not fully applied the checkpoint target",
                ));
            }
            let messages = h.msg_log();
            let events = h.event_log();
            if pin.event_epoch != events.current_epoch()
                || pin.message_epoch != messages.current_epoch()
                || target.event_next < pin.event_next
                || target.message_head < pin.message_head
                || target.message_next < pin.message_next
                || target.message_head > target.message_next
                || messages.head_offset() > target.message_head
                || messages.next_offset() < target.message_next
                || events.head_offset() > pin.event_next
                || target.event_next - pin.event_next > limits.records
                || target.message_next - target.message_head
                    > limits.records - (target.event_next - pin.event_next)
            {
                return Err(invalid(
                    "checkpoint target has incompatible bounds, epoch or record budget",
                ));
            }
            let root = stroma.checkpoint_pin_root(&s.topic, s.partition, s.group.as_deref());
            drop(apply);
            let scan_root = root.clone();
            let input_pin = pin.clone();
            let capsule = tokio::task::spawn_blocking(move || {
                let s = &input_pin.storage;
                let index = load_index(&scan_root, &s.topic, s.partition, s.group.as_deref())?;
                if index.pins.get(&name(input_pin.attempt)) != Some(&input_pin) {
                    return Err(invalid("checkpoint replay base is not retained"));
                }
                let path = capsule_path(&scan_root, input_pin.attempt);
                if let Some(existing) = read::<QueueCheckpointCapsule>(&path, MAX_BASE)? {
                    existing.validate()?;
                    if existing.pin != input_pin || existing.contents.target() != target {
                        return Err(invalid("conflicting checkpoint capsule target"));
                    }
                    return Ok(existing);
                }
                let base = load_base(&scan_root, &input_pin)?;
                build_capsule(base, target, messages, events, limits)
            })
            .await
            .map_err(io_err)??;
            let apply = h.follower_apply_state().await;
            h.ensure_not_recovery_sealed()?;
            stroma.verify_admitted_storage_history(&pin.storage)?;
            if *apply
                || h.event_log().current_epoch() != pin.event_epoch
                || h.msg_log().current_epoch() != pin.message_epoch
            {
                return Err(invalid("checkpoint history changed during verification"));
            }
            let writer = stroma.clone();
            tokio::task::spawn_blocking(move || {
                capsule.validate()?;
                persist(&capsule_path(&root, pin.attempt), &capsule, MAX_BASE)?;
                writer.checkpoint_boundary("agreement_capsule")?;
                Ok(capsule)
            })
            .await
            .map_err(io_err)?
        })
        .await
        .map_err(io_err)?
    }
}

fn build_capsule(
    base: QueueCheckpointBase,
    target: QueueCheckpointTarget,
    messages: Arc<Keratin>,
    events: Arc<Keratin>,
    limits: QueueCheckpointBuildLimits,
) -> Result<QueueCheckpointCapsule> {
    use crate::{
        recovery_inspection::hash_record,
        recovery_replay::{QueueReplay, RecoveryReplayLimits},
    };
    let started = Instant::now();
    let envelope = Stroma::queue_snapshot_envelope(2, base.pin.event_next, &base.snapshot)?;
    // This private replay input is not a sealed-recovery receipt. Its output is
    // used only to construct a checkpoint capsule after both physical scans.
    let history = RetainedHistoryIdentity {
        version: 2,
        storage_history: Some(base.pin.storage.clone()),
        id: base.pin.attempt,
        message_digest: [0; 32],
        event_digest: [0; 32],
        snapshot_digest: Some(*blake3::hash(&envelope).as_bytes()),
        message_head: target.message_head,
        message_next: target.message_next,
        event_head: base.pin.event_next,
        event_next: target.event_next,
    };
    let mut replay = QueueReplay::from_checkpoint(
        &base.pin.storage.topic,
        base.pin.storage.partition,
        history,
        target.event_next,
        RecoveryReplayLimits {
            operations_per_replica: limits.bytes,
        },
        &envelope,
    )
    .map_err(invalid)?;
    let mut cursor = events
        .retained_durable_cursor(
            base.pin.event_epoch,
            base.pin.event_next,
            target.event_next,
            limits.bytes,
        )
        .map_err(io_err)?;
    while let Some(record) = cursor.next_record(MAX_SNAPSHOT).map_err(io_err)? {
        if started.elapsed() > limits.elapsed {
            return Err(invalid("checkpoint verification deadline exceeded"));
        }
        replay
            .apply(&RecoveryRecord {
                offset: record.offset,
                flags: record.flags,
                headers: record.headers.to_vec(),
                payload: record.payload.to_vec(),
            })
            .map_err(invalid)?;
    }
    let event_bytes = cursor.scanned_bytes();
    let remaining = limits
        .bytes
        .checked_sub(event_bytes)
        .filter(|n| *n > 0)
        .ok_or_else(|| invalid("checkpoint payload verification byte budget exhausted"))?;
    let mut cursor = messages
        .retained_durable_cursor(
            base.pin.message_epoch,
            target.message_head,
            target.message_next,
            remaining,
        )
        .map_err(io_err)?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"fibril-retained-log-v1\0");
    hash.update(&target.message_head.to_be_bytes());
    hash.update(&target.message_next.to_be_bytes());
    while let Some(record) = cursor.next_record(MAX_SNAPSHOT).map_err(io_err)? {
        if started.elapsed() > limits.elapsed {
            return Err(invalid("checkpoint verification deadline exceeded"));
        }
        let record = RecoveryRecord {
            offset: record.offset,
            flags: record.flags,
            headers: record.headers.to_vec(),
            payload: record.payload.to_vec(),
        };
        hash_record(&mut hash, &record);
        replay.message(&record).map_err(invalid)?;
    }
    let artifact = replay.finish_artifact(MAX_SNAPSHOT).map_err(invalid)?;
    if started.elapsed() > limits.elapsed {
        return Err(invalid("checkpoint verification deadline exceeded"));
    }
    let evidence = artifact.evidence();
    let capsule = QueueCheckpointCapsule {
        version: 1,
        pin: base.pin,
        contents: QueueCheckpointContents {
            event_next: target.event_next,
            message_head: target.message_head,
            message_next: target.message_next,
            required_message_next: evidence.required_message_next,
            snapshot_digest: artifact.snapshot_digest(),
            state_digest: evidence.lease_normalized_state_digest,
            message_digest: *hash.finalize().as_bytes(),
            live_payload_digest: evidence
                .live_payload_digest
                .ok_or_else(|| invalid("checkpoint did not verify live payloads"))?,
        },
        snapshot: artifact.state_snapshot().to_vec(),
    };
    capsule.validate()?;
    tracing::debug!(
        event_bytes,
        message_bytes = cursor.scanned_bytes(),
        elapsed_ms = started.elapsed().as_millis(),
        event_next = target.event_next,
        "verified local checkpoint capsule"
    );
    Ok(capsule)
}

impl Stroma {
    /// Install the local side of a freshly committed consensus certificate.
    /// The coordination layer must verify that `certificate` names this exact
    /// locally published capsule. This API does not establish consensus itself.
    pub async fn accept_queue_checkpoint(
        &self,
        pin: QueueCheckpointPin,
        certificate: [u8; 32],
        capsule_digest: [u8; 32],
        previous: Option<[u8; 32]>,
    ) -> Result<()> {
        if certificate == [0; 32] || capsule_digest == [0; 32] {
            return Err(invalid(
                "checkpoint acceptance requires certificate and capsule identities",
            ));
        }
        let stroma = self.clone();
        tokio::spawn(async move {
            let s = &pin.storage;
            stroma.verify_admitted_storage_history(s)?;
            let ticket = stroma
                .queue_handle(&s.topic, s.partition, s.group.as_deref())
                .await?;
            let _lifecycle = stroma
                .lock_partition_lifecycle(&s.topic, s.partition, s.group.as_deref())
                .await;
            let h = ticket.resolve()?;
            let current = stroma.queue_handles.load();
            if !slot_lookup_no_alloc(&current, &s.topic, s.partition, s.group.as_deref())
                .and_then(|slot| slot.handle.get())
                .is_some_and(|inner| std::ptr::eq(inner.as_ref(), &*h))
            {
                return Err(invalid("checkpoint acceptance handle was retired; retry"));
            }
            drop(current);
            let apply = h.follower_apply_state().await;
            h.ensure_not_recovery_sealed()?;
            stroma.verify_admitted_storage_history(s)?;
            stroma.verify_prepared_storage_history(s)?;
            if *apply
                || h.event_log().current_epoch() != pin.event_epoch
                || h.msg_log().current_epoch() != pin.message_epoch
            {
                return Err(invalid("checkpoint acceptance history changed"));
            }
            let root = stroma.checkpoint_pin_root(&s.topic, s.partition, s.group.as_deref());
            let writer = stroma.clone();
            let input = pin.clone();
            let gate = h.recovery_gate.clone();
            let generation = gate.checkpoint_generation.load(Ordering::Acquire);
            let applied = h
                .ordered_applied_next()?
                .ok_or_else(|| invalid("ordered apply required"))?;
            let target = tokio::task::spawn_blocking(move || {
                let s = &input.storage;
                let mut index = load_index(&root, &s.topic, s.partition, s.group.as_deref())?;
                if index.pins.get(&name(input.attempt)) != Some(&input) {
                    return Err(invalid(
                        "checkpoint acceptance requires its retained local pin",
                    ));
                }
                let capsule: QueueCheckpointCapsule =
                    read(&capsule_path(&root, input.attempt), MAX_BASE)?
                        .ok_or_else(|| corrupt("accepted checkpoint capsule missing"))?;
                capsule.validate()?;
                if capsule.pin != input
                    || capsule.digest()? != capsule_digest
                    || capsule.contents.event_next > applied
                {
                    return Err(invalid(
                        "accepted certificate differs from local capsule or applied boundary",
                    ));
                }
                let accepted = AcceptedCheckpoint {
                    certificate,
                    capsule: capsule_digest,
                    attempt: input.attempt,
                    contents: capsule.contents.clone(),
                };
                if index.accepted.as_ref() != Some(&accepted) {
                    // A node must acknowledge local installation before another
                    // candidate can start, so it cannot skip a predecessor.
                    if index.accepted.as_ref().map(|a| a.certificate) != previous {
                        return Err(invalid("checkpoint acceptance predecessor changed"));
                    }
                    if let Some(old) = &index.accepted {
                        if accepted.contents.event_next <= old.contents.event_next
                            || accepted.contents.message_head < old.contents.message_head
                            || accepted.contents.message_next < old.contents.message_next
                        {
                            return Err(invalid("checkpoint acceptance regresses retained state"));
                        }
                    }
                    // Ordinary restart also needs a snapshot covering the new
                    // floor. A newer periodic snapshot must never be overwritten.
                    gate.write_snapshot(
                        generation,
                        &s.topic,
                        s.partition,
                        s.group.as_deref(),
                        || {
                            let current = writer.read_queue_snapshot(&writer.snap_file(
                                &s.topic,
                                s.partition,
                                s.group.as_deref(),
                            ))?;
                            if current.as_ref().is_none_or(|(boundary, _)| {
                                boundary.event_next() < capsule.contents.event_next
                                    || boundary.ambiguous_zero()
                            }) {
                                writer.write_queue_snapshot_envelope(
                                    &s.topic,
                                    s.partition,
                                    s.group.as_deref(),
                                    2,
                                    capsule.contents.event_next,
                                    &capsule.snapshot,
                                )?;
                            }
                            Ok(())
                        },
                    )?;
                    writer.checkpoint_boundary("agreement_restart_snapshot")?;
                    if let Some(old) = index.accepted.take() {
                        index.pins.remove(&name(old.attempt));
                    }
                    index.accepted = Some(accepted);
                    persist(&root.join("retention"), &index, MAX_INDEX)?;
                    writer.checkpoint_boundary("agreement_acceptance")?;
                    reclaim_unpinned(&root, &index)?;
                }
                Ok(capsule.contents)
            })
            .await
            .map_err(io_err)??;
            // Durable accepted metadata and restart state precede both logical
            // floors. A crash between these commands leaves extra retained data.
            let (message, event) = stroma
                .checkpoint_retention_limits(&h, target.message_head, target.event_next)
                .await?;
            h.event_log()
                .advance_retained_head(event, pin.event_epoch)
                .await
                .map_err(io_err)?;
            stroma.checkpoint_boundary("agreement_event_floor")?;
            h.msg_log()
                .advance_retained_head(message, pin.message_epoch)
                .await
                .map_err(io_err)?;
            stroma.checkpoint_boundary("agreement_message_floor")?;
            Ok(())
        })
        .await
        .map_err(io_err)?
    }
}

impl Stroma {
    /// Reconcile retention with a freshly authorized coordination attempt.
    /// `accepted` identifies the only certificate still used by that protocol;
    /// a changed history/membership may abandon it and use ordinary snapshots.
    /// Logical floors never regress, and the restart snapshot must cover any
    /// accepted capsule before its special retention obligation is removed.
    pub async fn reconcile_queue_checkpoint_pins(
        &self,
        storage: PreparedStorageHistory,
        pending: Option<[u8; 32]>,
        accepted: Option<[u8; 32]>,
    ) -> Result<()> {
        let stroma = self.clone();
        tokio::spawn(async move {
            stroma.verify_admitted_storage_history(&storage)?;
            let ticket = stroma
                .queue_handle(&storage.topic, storage.partition, storage.group.as_deref())
                .await?;
            let _lifecycle = stroma
                .lock_partition_lifecycle(
                    &storage.topic,
                    storage.partition,
                    storage.group.as_deref(),
                )
                .await;
            let h = ticket.resolve()?;
            let current = stroma.queue_handles.load();
            if !slot_lookup_no_alloc(
                &current,
                &storage.topic,
                storage.partition,
                storage.group.as_deref(),
            )
            .and_then(|slot| slot.handle.get())
            .is_some_and(|inner| std::ptr::eq(inner.as_ref(), &*h))
            {
                return Err(invalid("checkpoint cleanup handle was retired"));
            }
            drop(current);
            let _apply = h.follower_apply_state().await;
            h.ensure_not_recovery_sealed()?;
            stroma.verify_admitted_storage_history(&storage)?;
            stroma.verify_prepared_storage_history(&storage)?;
            let writer = stroma.clone();
            tokio::task::spawn_blocking(move || {
                let s = storage;
                let root = writer.checkpoint_pin_root(&s.topic, s.partition, s.group.as_deref());
                let mut index = load_index(&root, &s.topic, s.partition, s.group.as_deref())?;
                let before = index.clone();
                if index
                    .accepted
                    .as_ref()
                    .is_some_and(|a| Some(a.certificate) != accepted)
                {
                    let old = index.accepted.as_ref().unwrap();
                    let snapshot = writer.read_queue_snapshot(&writer.snap_file(
                        &s.topic,
                        s.partition,
                        s.group.as_deref(),
                    ))?;
                    if snapshot.is_none_or(|(boundary, _)| {
                        boundary.ambiguous_zero() || boundary.event_next() < old.contents.event_next
                    }) {
                        return Err(corrupt(
                            "checkpoint abandonment lacks a covering restart snapshot",
                        ));
                    }
                    index.accepted = None;
                }
                let retained = index.accepted.as_ref().map(|a| a.attempt);
                index
                    .pins
                    .retain(|_, pin| Some(pin.attempt) == pending || Some(pin.attempt) == retained);
                if index != before {
                    persist(&root.join("retention"), &index, MAX_INDEX)?;
                }
                reclaim_unpinned(&root, &index)?;
                Ok(())
            })
            .await
            .map_err(io_err)?
        })
        .await
        .map_err(io_err)?
    }
}

impl Stroma {
    /// A locally accepted capsule is already a durable compaction checkpoint;
    /// using it changes the replay starting point, never recovery authority.
    /// The ordinary sealed-history protocol still verifies every retained byte
    /// and obtains fresh quorum/installation authority.
    pub(super) fn agreed_recovery_snapshot(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        storage: Option<&PreparedStorageHistory>,
        messages: &Keratin,
        events: &Keratin,
    ) -> Result<Option<Vec<u8>>> {
        let root = self.checkpoint_pin_root(topic, part, group);
        let index = load_index(&root, topic, part, group)?;
        let Some(accepted) = index.accepted else {
            return Ok(None);
        };
        let pin = &index.pins[&name(accepted.attempt)];
        // A new installed history cannot inherit an old checkpoint's authority.
        if Some(&pin.storage) != storage {
            return Ok(None);
        }
        let Some(capsule) =
            read::<QueueCheckpointCapsule>(&capsule_path(&root, accepted.attempt), MAX_BASE)?
        else {
            tracing::warn!(
                topic,
                partition = part,
                "accepted checkpoint capsule missing; using ordinary verified snapshot recovery"
            );
            return Ok(None);
        };
        capsule.validate()?;
        if capsule.pin != *pin
            || capsule.contents != accepted.contents
            || capsule.digest()? != accepted.capsule
        {
            return Err(corrupt(
                "accepted recovery checkpoint differs from durable receipt",
            ));
        }
        if events.head_offset() > capsule.contents.event_next
            || events.next_offset() < capsule.contents.event_next
            || messages.head_offset() > capsule.contents.message_head
            || messages.next_offset() < capsule.contents.message_next
        {
            return Err(corrupt(
                "accepted checkpoint dependencies no longer retained",
            ));
        }
        Ok(Some(Self::queue_snapshot_envelope(
            2,
            capsule.contents.event_next,
            &capsule.snapshot,
        )?))
    }
}
