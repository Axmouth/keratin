//! Resumable private snapshot transfer. Durable progress is tentative until the
//! complete artifact matches the digest and length in the authorized plan.
use keratin_log::util::fsync_dir;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

pub const SNAPSHOT_CHUNK_BYTES: usize = 1024 * 1024;
/// Retained state admission, independent of the wire page and history budgets.
pub const MAX_RECOVERY_SNAPSHOT_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoverySnapshotIdentity {
    pub plan: [u8; 32],
    pub digest: [u8; 32],
    pub bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct RecoverySnapshotLimits {
    pub reserve_free_bytes: u64,
    pub max_bytes: u64,
}
impl Default for RecoverySnapshotLimits {
    fn default() -> Self {
        Self {
            reserve_free_bytes: 256 * 1024 * 1024,
            max_bytes: MAX_RECOVERY_SNAPSHOT_BYTES as u64,
        }
    }
}

pub struct RecoverySnapshotFile {
    root: PathBuf,
    identity: RecoverySnapshotIdentity,
    limits: RecoverySnapshotLimits,
    next: u64,
    verified: bool,
    needs_reopen: bool,
    data: File,
    _lock: keratin_log::lock::FileLock,
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn bounded_read(path: &Path, limit: u64) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(invalid("snapshot metadata exceeds bound"));
    }
    Ok(bytes)
}
fn publish(root: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    let scratch = root.join("metadata.writing");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&scratch)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(scratch, root.join(name))?;
    fsync_dir(root)
}
impl RecoverySnapshotFile {
    /// The caller supplies fresh authority for this exact identity. This file
    /// cannot select a source, install state or grant serving authority.
    pub fn open(
        parent: &Path,
        identity: RecoverySnapshotIdentity,
        limits: RecoverySnapshotLimits,
    ) -> io::Result<Self> {
        if identity.plan == [0; 32]
            || identity.bytes == 0
            || identity.bytes > limits.max_bytes
            || limits.max_bytes > MAX_RECOVERY_SNAPSHOT_BYTES as u64
        {
            return Err(invalid(
                "invalid snapshot identity or retained-state byte bound",
            ));
        }
        let encoded = rmp_serde::to_vec_named(&identity).map_err(io::Error::other)?;
        let root = parent.join(blake3::hash(&encoded).to_hex().as_str());
        fs::create_dir_all(&root)?;
        let lock = keratin_log::lock::FileLock::try_exclusive_or_create(&root.join("lock"))?;
        crate::dir_sync::sync_dir_chain(&root)?;
        match bounded_read(&root.join("identity"), 1024) {
            Ok(saved) if saved != encoded => return Err(invalid("snapshot identity changed")),
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => publish(&root, "identity", &encoded)?,
            Err(e) => return Err(e),
        }
        let next = match bounded_read(&root.join("progress"), 12) {
            Ok(bytes)
                if bytes.len() == 12
                    && crc32c::crc32c(&bytes[..8])
                        == u32::from_be_bytes(bytes[8..].try_into().unwrap()) =>
            {
                u64::from_be_bytes(bytes[..8].try_into().unwrap())
            }
            Ok(_) => return Err(invalid("snapshot progress checksum or size mismatch")),
            Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
            Err(e) => return Err(e),
        };
        if next > identity.bytes {
            return Err(invalid("snapshot progress exceeds selected length"));
        }
        let data_path = root.join("data");
        // Once progress exists, missing data is corruption. Never recreate it.
        let data = OpenOptions::new()
            .create(next == 0)
            .truncate(false)
            .read(true)
            .write(true)
            .open(data_path)?;
        let actual = data.metadata()?.len();
        if actual < next {
            return Err(invalid("durable snapshot prefix was truncated"));
        }
        if actual > next {
            // Only unpublished private tail bytes can be discarded on resume.
            data.set_len(next)?;
            data.sync_all()?;
        }
        fsync_dir(&root)?;
        Ok(Self {
            root,
            identity,
            limits,
            next,
            data,
            verified: false,
            needs_reopen: false,
            _lock: lock,
        })
    }

    pub fn next_offset(&self) -> u64 {
        self.next
    }

    /// A matching retry is idempotent. Skips, overlaps and conflicting retries
    /// are rejected. Persist data before publishing the new durable frontier.
    pub fn append(&mut self, from: u64, bytes: &[u8]) -> io::Result<u64> {
        if self.needs_reopen {
            return Err(invalid("snapshot transfer requires reopen"));
        }
        let end = from
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| invalid("snapshot offset overflow"))?;
        if bytes.is_empty()
            || bytes.len() > SNAPSHOT_CHUNK_BYTES
            || end > self.identity.bytes
            || from > self.next
            || from < self.next && end > self.next
        {
            return Err(invalid(
                "snapshot chunk bounds differ from durable progress",
            ));
        }
        if end <= self.next {
            let mut saved = vec![0; bytes.len()];
            self.data.seek(SeekFrom::Start(from))?;
            self.data.read_exact(&mut saved)?;
            if saved != bytes {
                return Err(invalid("conflicting snapshot chunk retry"));
            }
            return Ok(self.next);
        }
        let free = fs2::available_space(&self.root)?;
        if free.saturating_sub(self.limits.reserve_free_bytes) < bytes.len() as u64 + 4096 {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                format!(
                    "snapshot disk reserve: free={free}, reserve={}, requested={}",
                    self.limits.reserve_free_bytes,
                    bytes.len()
                ),
            ));
        }
        self.needs_reopen = true;
        self.data.seek(SeekFrom::Start(from))?;
        self.data.write_all(bytes)?;
        self.data.sync_all()?;
        #[cfg(test)]
        self.crash_gate("data_synced");
        let mut progress = end.to_be_bytes().to_vec();
        progress.extend_from_slice(&crc32c::crc32c(&progress).to_be_bytes());
        publish(&self.root, "progress", &progress)?;
        #[cfg(test)]
        self.crash_gate("progress_synced");
        self.next = end;
        self.needs_reopen = false;
        Ok(end)
    }

    pub fn verify_complete(&mut self) -> io::Result<()> {
        if self.needs_reopen || self.next != self.identity.bytes {
            return Err(invalid("snapshot transfer is incomplete"));
        }
        if !self.verified {
            self.data.seek(SeekFrom::Start(0))?;
            let mut hash = blake3::Hasher::new();
            let mut buffer = vec![0; SNAPSHOT_CHUNK_BYTES];
            let mut left = self.identity.bytes;
            while left != 0 {
                let n = left.min(buffer.len() as u64) as usize;
                self.data.read_exact(&mut buffer[..n])?;
                hash.update(&buffer[..n]);
                left -= n as u64;
            }
            if hash.finalize().as_bytes() != &self.identity.digest {
                return Err(invalid("snapshot artifact digest mismatch"));
            }
            self.verified = true;
        }
        Ok(())
    }

    pub fn read_chunk(&mut self, from: u64, max_bytes: usize) -> io::Result<Vec<u8>> {
        self.verify_complete()?;
        if from >= self.identity.bytes || max_bytes == 0 || max_bytes > SNAPSHOT_CHUNK_BYTES {
            return Err(invalid("snapshot read chunk outside bounds"));
        }
        let mut bytes = vec![0; (self.identity.bytes - from).min(max_bytes as u64) as usize];
        self.data.seek(SeekFrom::Start(from))?;
        self.data.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    pub fn read_complete(&mut self) -> io::Result<Vec<u8>> {
        self.verify_complete()?;
        let mut bytes = vec![0; self.identity.bytes as usize];
        self.data.seek(SeekFrom::Start(0))?;
        self.data.read_exact(&mut bytes)?;
        // Validate the actual returned bytes, not an earlier read of the file.
        if blake3::hash(&bytes).as_bytes() != &self.identity.digest {
            return Err(invalid("snapshot changed during verified read"));
        }
        Ok(bytes)
    }

    #[cfg(test)]
    fn crash_gate(&self, phase: &str) {
        if std::env::var("STROMA_TEST_SNAPSHOT_CRASH_PHASE").as_deref() == Ok(phase) {
            fs::write(
                std::env::var("STROMA_TEST_SNAPSHOT_CRASH_MARKER").unwrap(),
                phase,
            )
            .unwrap();
            loop {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    }
}

#[cfg(test)]
#[path = "recovery_snapshot_tests.rs"]
mod tests;
