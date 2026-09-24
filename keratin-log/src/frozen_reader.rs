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
    io::{self, BufReader, Read},
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

/// Sequential, read-only cursor over an externally sealed log. The caller must
/// keep the log frozen and verify a full content digest before accepting a scan.
/// Retains one file buffer and bounded record scratch, never the full history.
pub struct FrozenLogCursor {
    entries: Vec<(u64, PathBuf)>,
    segment: usize,
    file: Option<BufReader<File>>,
    segment_end: u64,
    expected: u64,
    head: u64,
    next: u64,
    bytes: Vec<u8>,
    failed: bool,
}

impl FrozenLogCursor {
    /// A returned record borrows scratch storage until the next read. CRC and
    /// exact offsets are checked for every record, including a retained prefix
    /// before `head` in its first segment. No disk repair is performed.
    pub fn next_record(
        &mut self,
        max_record_bytes: usize,
    ) -> io::Result<Option<DecodedRecord<'_>>> {
        if self.failed {
            return Err(invalid("failed frozen cursor must be discarded"));
        }
        self.failed = true;
        if !self.advance(max_record_bytes)? {
            self.failed = false;
            return Ok(None);
        }
        let record = decode_record_prefix(&self.bytes)
            .map_err(|_| invalid("corrupt frozen record"))?
            .0;
        self.failed = false;
        Ok(Some(record))
    }

    fn advance(&mut self, max_record_bytes: usize) -> io::Result<bool> {
        loop {
            if self.expected == self.next {
                return Ok(false);
            }
            if self.file.is_none() || self.expected == self.segment_end {
                let (base, path) = self
                    .entries
                    .get(self.segment)
                    .ok_or_else(|| invalid("missing frozen suffix"))?;
                if *base != self.expected {
                    return Err(invalid("gap between frozen segments"));
                }
                let mut file = File::open(path)?;
                read_segment_header(&mut file, *base)?;
                self.segment += 1;
                self.segment_end = self
                    .entries
                    .get(self.segment)
                    .map(|(base, _)| *base)
                    .unwrap_or(self.next);
                self.file = Some(BufReader::with_capacity(64 * 1024, file));
            }
            let file = self.file.as_mut().unwrap();
            let mut fixed = [0u8; RECORD_HEADER_LEN];
            file.read_exact(&mut fixed)?;
            let headers = u32::from_be_bytes(fixed[8..12].try_into().unwrap()) as usize;
            let payload = u32::from_be_bytes(fixed[12..16].try_into().unwrap()) as usize;
            let total = RECORD_HEADER_LEN
                .checked_add(headers)
                .and_then(|n| n.checked_add(payload))
                .and_then(|n| n.checked_add(4))
                .ok_or_else(|| invalid("frozen record length overflow"))?;
            if total > MAX_FROZEN_RECORD_BYTES || total > max_record_bytes {
                return Err(invalid("frozen record exceeds recovery allocation limit"));
            }
            if total > self.bytes.capacity() {
                self.bytes.reserve_exact(total - self.bytes.len());
            }
            self.bytes.resize(total, 0);
            self.bytes[..RECORD_HEADER_LEN].copy_from_slice(&fixed);
            file.read_exact(&mut self.bytes[RECORD_HEADER_LEN..])?;
            let offset = u64::from_be_bytes(fixed[24..32].try_into().unwrap());
            if offset != self.expected {
                return Err(invalid("noncontiguous frozen record"));
            }
            self.expected += 1;
            if offset >= self.head {
                break;
            }
            decode_record_prefix(&self.bytes).map_err(|_| invalid("corrupt frozen record"))?;
        }
        Ok(true)
    }
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
        Self::open_inner(root, epoch, head, next, false)
    }

    // Only Keratin can supply its quiescent, fully durable live frontier. Cold
    // sealed readers continue to require an exact persisted manifest boundary.
    pub(crate) fn open_live(root: &Path, epoch: u64, head: u64, next: u64) -> io::Result<Self> {
        Self::open_inner(root, epoch, head, next, true)
    }

    fn open_inner(root: &Path, epoch: u64, head: u64, next: u64, live: bool) -> io::Result<Self> {
        if root.join(crate::suffix_repair::JOURNAL).try_exists()? {
            return Err(invalid("frozen read cannot bypass pending suffix repair"));
        }
        let mut file = File::open(Manifest::path(root))?;
        if !matches!(file.metadata()?.len(), 76 | 84) {
            return Err(invalid("invalid frozen manifest size"));
        }
        let manifest = Manifest::read_from(&mut file)?;
        if manifest.epoch != epoch
            || manifest.head_offset != head
            || if live {
                manifest.next_offset > next
            } else {
                manifest.next_offset != next
            }
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

    /// Start one sequential traversal of the exact frozen retained range.
    pub fn into_cursor(self) -> io::Result<FrozenLogCursor> {
        if self.head == self.next {
            let path = self
                .segments
                .get(&self.active)
                .ok_or_else(|| invalid("missing empty frozen segment"))?;
            read_segment_header(&mut File::open(path)?, self.active)?;
            return Ok(FrozenLogCursor {
                entries: vec![],
                segment: 0,
                file: None,
                segment_end: self.next,
                expected: self.next,
                head: self.head,
                next: self.next,
                bytes: vec![],
                failed: false,
            });
        }
        let first = *self
            .segments
            .range(..=self.head)
            .next_back()
            .ok_or_else(|| invalid("missing frozen head segment"))?
            .0;
        let entries = self
            .segments
            .into_iter()
            .filter(|(base, _)| *base >= first && *base < self.next)
            .collect();
        Ok(FrozenLogCursor {
            entries,
            segment: 0,
            file: None,
            segment_end: first,
            expected: first,
            head: self.head,
            next: self.next,
            bytes: vec![],
            failed: false,
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
        // Reuse bounded scratch storage across records and segments. Scanning
        // still decodes and validates every record in the sealed range.
        let mut bytes = Vec::new();
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
            // Avoid two filesystem reads for every record during full scans.
            let mut file = BufReader::with_capacity(64 * 1024, file);
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
                if total > bytes.capacity() {
                    // Avoid geometric growth beyond the validated record limit.
                    bytes.reserve_exact(total - bytes.len());
                }
                bytes.resize(total, 0);
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

pub(crate) fn read_segment_header(file: &mut File, expected_base: u64) -> io::Result<()> {
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
