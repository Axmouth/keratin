//! Durable tentative inspection pages. A journal is a download cache, never a
//! witness, verified digest, state artifact, source choice or activation receipt.
//! Every restored page must pass the ordinary sealed inspector again.
use keratin_log::util::fsync_dir;
use crate::{RecoveryReadPage, RecoveryReadRequest, RecoveryReadSource, SealedReplicaFrontiers};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

const MAGIC: &[u8; 8] = b"RIPAGE\0\x01";
const MAX_PAGE: u64 = 16 * 1024 * 1024;
// MessagePack encodes Vec<u8> as integers, at most two bytes per input byte.
const MAX_ENCODED: u64 = 2 * MAX_PAGE + 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryJournalIdentity {
    pub topic: String,
    pub partition: u32,
    pub group: Option<String>,
    pub stream: bool,
    pub replica: String,
    pub seal: SealedReplicaFrontiers,
}

/// Capacity admission is independent of total retained history. This reserve
/// protects other node work and is rechecked for every new durable cache frame.
#[derive(Debug, Clone, Copy)]
pub struct RecoveryJournalLimits {
    pub reserve_free_bytes: u64,
}
impl Default for RecoveryJournalLimits {
    fn default() -> Self {
        Self {
            reserve_free_bytes: 256 * 1024 * 1024,
        }
    }
}

pub struct RecoveryInspectionJournal {
    _namespace: crate::recovery_cache::RecoveryCacheLease,
    root: PathBuf,
    identity: RecoveryJournalIdentity,
    limits: RecoveryJournalLimits,
    // The lock follows owned blocking work through caller cancellation.
    _lock: File,
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn encode<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    rmp_serde::to_vec_named(value).map_err(io::Error::other)
}
impl RecoveryInspectionJournal {
    /// The parent belongs to the local node. Hash-derived child names never
    /// interpolate topic, group or replica names into filesystem paths.
    pub fn open(parent: &Path, identity: RecoveryJournalIdentity) -> io::Result<Self> {
        Self::open_with_limits(parent, identity, Default::default())
    }

    pub fn open_with_limits(
        parent: &Path,
        identity: RecoveryJournalIdentity,
        limits: RecoveryJournalLimits,
    ) -> io::Result<Self> {
        if identity.replica.is_empty() || identity.topic.is_empty() {
            return Err(invalid("empty inspection journal identity"));
        }
        // Reuse the maintained resource/transition/frontier validation. This
        // checks a receipt's shape and binding, not its content or authority.
        crate::recovery_inspection::RecoveryPairInspector::new(
            &identity.topic,
            identity.partition,
            identity.group.as_deref(),
            identity.stream,
            identity.seal.clone(),
            identity.seal.clone(),
            Default::default(),
        )
        .map_err(io::Error::other)?;
        let namespace = crate::recovery_cache::RecoveryCacheLease::acquire(parent)?;
        let encoded = encode(&identity)?;
        if encoded.len() > 65536 {
            return Err(invalid("inspection journal identity exceeds limit"));
        }
        let root = parent.join(blake3::hash(&encoded).to_hex().as_str());
        fs::create_dir_all(&root)?;
        crate::dir_sync::sync_dir_chain(&root)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join("lock"))?;
        lock.try_lock().map_err(io::Error::other)?;
        let journal = Self {
            root,
            identity,
            limits,
            _namespace: namespace,
            _lock: lock,
        };
        // Exact identity is also stored for diagnostics and collision/corruption
        // detection. An interrupted scratch file is never treated as committed.
        let path = journal.root.join("identity");
        let read_identity = || -> io::Result<Vec<u8>> {
            let mut bytes = Vec::new();
            File::open(&path)?.take(65537).read_to_end(&mut bytes)?;
            Ok(bytes)
        };
        match read_identity() {
            Ok(existing) if existing != encoded => {
                return Err(invalid("inspection journal identity mismatch"));
            }
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => journal.persist(&path, &encoded)?,
            Err(e) => return Err(e),
        }
        File::open(&path)?.sync_all()?;
        fsync_dir(&journal.root)?;
        Ok(journal)
    }

    fn request_path(&self, request: &RecoveryReadRequest) -> io::Result<PathBuf> {
        if request.seal != self.identity.seal.request
            || request.history_id != self.identity.seal.history.id
            || request.max_records == 0
            || request.max_records > 4096
            || request.max_bytes == 0
            || u64::from(request.max_bytes) > MAX_PAGE
        {
            return Err(invalid(
                "inspection journal request identity or page bound mismatch",
            ));
        }
        Ok(self
            .root
            .join(format!("{}.page", blake3::hash(&encode(request)?).to_hex())))
    }

    fn validate_page(
        &self,
        request: &RecoveryReadRequest,
        page: &RecoveryReadPage,
    ) -> io::Result<()> {
        self.request_path(request)?;
        if page.history_id != request.history_id
            || page.source != request.source
            || page.from != request.from
            || page.next < page.from
            || page.next > page.end
        {
            return Err(invalid(
                "inspection journal page identity or range mismatch",
            ));
        }
        let h = &self.identity.seal.history;
        let bytes = match page.source {
            RecoveryReadSource::Snapshot => {
                if h.snapshot_digest.is_none()
                    || !page.records.is_empty()
                    || page.next == page.from
                    || page.next - page.from != page.snapshot_bytes.len() as u64
                {
                    return Err(invalid("invalid inspection journal snapshot page"));
                }
                page.snapshot_bytes.len() as u64
            }
            source => {
                let (head, end) = if source == RecoveryReadSource::Events {
                    (h.event_head, h.event_next)
                } else {
                    (h.message_head, h.message_next)
                };
                if page.from < head
                    || page.end != end
                    || !page.snapshot_bytes.is_empty()
                    || page.next - page.from != page.records.len() as u64
                    || page.records.len() > request.max_records as usize
                    || (page.next == page.from && page.next != end)
                {
                    return Err(invalid("invalid inspection journal record page"));
                }
                let mut bytes = 0u64;
                for (n, record) in page.records.iter().enumerate() {
                    if record.offset != page.from + n as u64 {
                        return Err(invalid("noncontiguous inspection journal page"));
                    }
                    bytes = bytes
                        .checked_add(18)
                        .and_then(|n| n.checked_add(record.headers.len() as u64))
                        .and_then(|n| n.checked_add(record.payload.len() as u64))
                        .ok_or_else(|| invalid("inspection journal page size overflow"))?;
                }
                bytes
            }
        };
        if bytes > u64::from(request.max_bytes) {
            return Err(invalid("inspection journal page exceeds byte bound"));
        }
        Ok(())
    }

    /// Returns tentative bytes only. Torn scratch files are ignored. Corrupt
    /// committed pages fail closed so disk errors are visible to the operator.
    pub fn read(&self, request: &RecoveryReadRequest) -> io::Result<Option<RecoveryReadPage>> {
        let path = self.request_path(request)?;
        let file = match File::open(path) {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let mut bytes = Vec::new();
        file.take(MAX_ENCODED + 41).read_to_end(&mut bytes)?;
        if bytes.len() < 40 || bytes.len() as u64 > MAX_ENCODED + 40 || &bytes[..8] != MAGIC {
            return Err(invalid("invalid inspection journal frame size or version"));
        }
        let end = bytes.len() - 32;
        if blake3::hash(&bytes[..end]).as_bytes() != &bytes[end..] {
            return Err(invalid("inspection journal checksum mismatch"));
        }
        let (saved_request, page): (RecoveryReadRequest, RecoveryReadPage) =
            rmp_serde::from_slice(&bytes[8..end]).map_err(io::Error::other)?;
        if &saved_request != request {
            return Err(invalid(
                "inspection journal page belongs to another request",
            ));
        }
        self.validate_page(request, &page)?;
        Ok(Some(page))
    }

    /// Publish one bounded page only after data and directory persistence. A
    /// failed or cancelled caller may retry this exact append idempotently.
    pub fn append(&self, request: &RecoveryReadRequest, page: &RecoveryReadPage) -> io::Result<()> {
        self.validate_page(request, page)?;
        let path = self.request_path(request)?;
        if let Some(existing) = self.read(request)? {
            if &existing != page {
                return Err(invalid("conflicting inspection journal page"));
            }
            File::open(path)?.sync_all()?;
            return fsync_dir(&self.root);
        }
        let payload = encode(&(request, page))?;
        if payload.len() as u64 > MAX_ENCODED {
            return Err(invalid("encoded inspection journal page too large"));
        }
        let mut bytes = MAGIC.to_vec();
        bytes.extend(payload);
        let digest = blake3::hash(&bytes);
        bytes.extend(digest.as_bytes());
        self.persist(&path, &bytes)
    }

    /// Drop tentative page files after completed phase results or activation are
    /// durably recorded by the caller. Keeping the lock/identity avoids unlinking
    /// an active lock inode. Reusing this journal simply fetches missing pages.
    pub fn clear_pages(&self) -> io::Result<u64> {
        let mut removed = 0u64;
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            if entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "page")
            {
                removed = removed.saturating_add(entry.metadata()?.len());
                fs::remove_file(entry.path())?;
            }
        }
        fsync_dir(&self.root)?;
        Ok(removed)
    }

    fn persist(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        let free = fs2::available_space(&self.root)?;
        if free.saturating_sub(self.limits.reserve_free_bytes) < bytes.len() as u64 {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                format!(
                    "inspection journal disk reserve: free={free}, reserve={}, requested={}",
                    self.limits.reserve_free_bytes,
                    bytes.len()
                ),
            ));
        }
        let scratch = self.root.join("page.writing");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&scratch)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        #[cfg(test)]
        journal_crash_gate(path, "file_synced");
        drop(file);
        fs::rename(scratch, path)?;
        #[cfg(test)]
        journal_crash_gate(path, "renamed");
        fsync_dir(&self.root)?;
        #[cfg(test)]
        journal_crash_gate(path, "directory_synced");
        Ok(())
    }
}

#[cfg(test)]
fn journal_crash_gate(path: &Path, phase: &str) {
    if path
        .extension()
        .is_some_and(|extension| extension == "page")
        && std::env::var("STROMA_TEST_JOURNAL_CRASH_PHASE").as_deref() == Ok(phase)
    {
        let marker = std::env::var("STROMA_TEST_JOURNAL_CRASH_MARKER").unwrap();
        fs::write(marker, phase).unwrap();
        loop {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}
