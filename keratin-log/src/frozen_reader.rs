//! Strict read-only scans for externally sealed logs. The caller must hold the
//! log's existing lock and prevent mutation for the scan's entire lifetime.

use crate::{
    manifest::Manifest,
    record::{DecodedRecord, RECORD_HEADER_LEN, decode_record_prefix},
    segment::LOG_HEADER_LEN,
};
use fs2::FileExt;
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{self, Read},
    path::{Path, PathBuf},
};

/// Hard recovery-scan allocation limit, independent of untrusted disk lengths.
pub const MAX_FROZEN_RECORD_BYTES: usize = 64 * 1024 * 1024;

pub struct FrozenLogReader {
    segments: BTreeMap<u64, PathBuf>,
    head: u64,
    next: u64,
    active: u64,
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// Acquire the existing log lock without creating/truncating any file. Live
/// callers already holding the writer's lock retain that owner instead.
pub fn lock_existing_log(root: &Path) -> io::Result<File> {
    let lock = File::open(root.join(".keratin.lock"))?;
    lock.try_lock_exclusive()?;
    Ok(lock)
}

impl FrozenLogReader {
    /// Does not acquire a lock, recover padding, repair indexes or open writers.
    /// The caller holds either `lock_existing_log` or a quiesced live log owner.
    pub fn open(root: &Path, epoch: u64, head: u64, next: u64) -> io::Result<Self> {
        let mut file = File::open(Manifest::path(root))?;
        if file.metadata()?.len() != 76 {
            return Err(invalid("invalid frozen manifest size"));
        }
        let manifest = Manifest::read_from(&mut file)?;
        if manifest.epoch != epoch
            || manifest.head_offset != head
            || manifest.next_offset != next
            || head > next
        {
            return Err(invalid(
                "frozen manifest does not match sealed bounds and epoch",
            ));
        }
        let mut segments = BTreeMap::new();
        for entry in fs::read_dir(root.join("segments"))? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(base) = name.strip_suffix(".log") else {
                continue;
            };
            let base: u64 = base
                .parse()
                .map_err(|_| invalid("invalid frozen segment name"))?;
            if name != format!("{base:020}.log") {
                return Err(invalid("noncanonical frozen segment name"));
            }
            if !entry.file_type()?.is_file() {
                return Err(invalid("frozen segment is not a regular file"));
            }
            segments.insert(base, entry.path());
        }
        Ok(Self {
            segments,
            head,
            next,
            active: manifest.active_base_offset,
        })
    }

    pub fn scan(
        &self,
        mut visit: impl FnMut(DecodedRecord<'_>) -> io::Result<()>,
    ) -> io::Result<()> {
        if self.head == self.next {
            let path = self
                .segments
                .get(&self.active)
                .ok_or_else(|| invalid("missing empty frozen segment"))?;
            read_segment_header(&mut File::open(path)?, self.active)?;
            return Ok(());
        }
        let first = *self
            .segments
            .range(..=self.head)
            .next_back()
            .ok_or_else(|| invalid("missing frozen head segment"))?
            .0;
        let mut expected = first;
        let entries: Vec<_> = self.segments.range(first..self.next).collect();
        for (index, (base, path)) in entries.iter().enumerate() {
            if **base != expected {
                return Err(invalid("gap between frozen segments"));
            }
            let end = entries
                .get(index + 1)
                .map(|(base, _)| **base)
                .unwrap_or(self.next);
            let mut file = File::open(path)?;
            read_segment_header(&mut file, **base)?;
            while expected < end {
                let mut fixed = [0u8; RECORD_HEADER_LEN];
                file.read_exact(&mut fixed)?;
                let headers = u32::from_be_bytes(fixed[8..12].try_into().unwrap()) as usize;
                let payload = u32::from_be_bytes(fixed[12..16].try_into().unwrap()) as usize;
                let total = RECORD_HEADER_LEN
                    .checked_add(headers)
                    .and_then(|n| n.checked_add(payload))
                    .and_then(|n| n.checked_add(4))
                    .ok_or_else(|| invalid("frozen record length overflow"))?;
                if total > MAX_FROZEN_RECORD_BYTES {
                    return Err(invalid("frozen record exceeds recovery allocation limit"));
                }
                let mut bytes = vec![0u8; total];
                bytes[..RECORD_HEADER_LEN].copy_from_slice(&fixed);
                file.read_exact(&mut bytes[RECORD_HEADER_LEN..])?;
                let (record, _) =
                    decode_record_prefix(&bytes).map_err(|_| invalid("corrupt frozen record"))?;
                if record.offset != expected {
                    return Err(invalid("noncontiguous frozen record"));
                }
                if expected >= self.head {
                    visit(record)?;
                }
                expected += 1;
            }
        }
        if expected != self.next {
            return Err(invalid("missing frozen suffix"));
        }
        Ok(())
    }
}

fn read_segment_header(file: &mut File, expected_base: u64) -> io::Result<()> {
    let mut bytes = [0u8; LOG_HEADER_LEN as usize];
    file.read_exact(&mut bytes)?;
    if &bytes[..8] != crate::segment::LOG_MAGIC
        || u16::from_be_bytes(bytes[8..10].try_into().unwrap()) != crate::segment::LOG_VERSION
        || u32::from_be_bytes(bytes[12..16].try_into().unwrap()) != LOG_HEADER_LEN
        || u64::from_be_bytes(bytes[16..24].try_into().unwrap()) != expected_base
        || crc32c::crc32c(&bytes[..bytes.len() - 4])
            != u32::from_be_bytes(bytes[bytes.len() - 4..].try_into().unwrap())
    {
        return Err(invalid("corrupt frozen segment header"));
    }
    Ok(())
}
