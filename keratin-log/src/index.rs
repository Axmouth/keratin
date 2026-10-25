use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};

pub const IDX_MAGIC: &[u8; 8] = b"KIDX\0\0\0\0";
pub const IDX_VERSION: u16 = 1;
pub const IDX_HEADER_LEN: u32 = 8 + 2 + 2 + 4 + 8 + 8 + 2 + 2 + 32 + 4;
pub const IDX_ENTRY_LEN: usize = 16;

pub struct Index {
    file: File,
    last_sync: std::time::Instant,
}

/// The active index syncs at most this often. It only speeds up lookups, so
/// recovery rebuilds whatever a crash left unsynced from the log itself.
const INDEX_SYNC_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

impl Index {
    pub fn create(file: File, base_offset: u64, created_ts_ms: u64) -> io::Result<Self> {
        let idx = Self::create_unsynced(file, base_offset, created_ts_ms)?;
        crate::util::sync_file_all(&idx.file)?;
        Ok(idx)
    }

    /// Start an empty index without syncing it. Recovery rebuilds a lost one.
    pub(crate) fn create_unsynced(mut file: File, base_offset: u64, created_ts_ms: u64) -> io::Result<Self> {
        // header_len fixed for v1
        let header_len: u32 = IDX_HEADER_LEN;

        let mut header = Vec::with_capacity(header_len as usize);
        header.extend_from_slice(IDX_MAGIC);
        header.extend_from_slice(&IDX_VERSION.to_be_bytes());
        header.extend_from_slice(&0u16.to_be_bytes()); // flags
        header.extend_from_slice(&header_len.to_be_bytes());
        header.extend_from_slice(&base_offset.to_be_bytes());
        header.extend_from_slice(&created_ts_ms.to_be_bytes());
        header.extend_from_slice(&16u16.to_be_bytes()); // entry_len
        header.extend_from_slice(&0u16.to_be_bytes()); // reserved0
        header.extend_from_slice(&[0u8; 32]); // reserved
        // header crc
        let crc = crc32c::crc32c(&header);
        header.extend_from_slice(&(crc).to_be_bytes());

        file.seek(SeekFrom::Start(0))?;
        file.write_all(&header)?;

        let end = file.metadata()?.len();
        file.seek(SeekFrom::Start(end))?;

        Ok(Self { file, last_sync: std::time::Instant::now() })
    }

    pub fn open(mut file: File, expected_base: u64) -> io::Result<Self> {
        let mut hdr = [0u8; 8 + 2 + 2 + 4 + 8 + 8 + 2 + 2 + 32 + 4];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut hdr)?;

        if &hdr[0..8] != IDX_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad idx magic"));
        }
        let version = u16::from_be_bytes(hdr[8..10].try_into().expect("exact-length slice"));
        if version != IDX_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad idx version",
            ));
        }
        let base = u64::from_be_bytes(hdr[16..24].try_into().expect("exact-length slice"));
        if base != expected_base {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "idx base offset mismatch",
            ));
        }

        let end = file.metadata()?.len();
        file.seek(SeekFrom::Start(end))?;

        Ok(Self { file, last_sync: std::time::Instant::now() })
    }

    pub fn fsync(&self) -> io::Result<()> {
        crate::util::sync_file_data(&self.file)
    }

    /// True at most once per `INDEX_SYNC_INTERVAL`, and counts as the sync.
    pub(crate) fn sync_due(&mut self) -> bool {
        if self.last_sync.elapsed() >= INDEX_SYNC_INTERVAL {
            self.last_sync = std::time::Instant::now();
            true
        } else {
            false
        }
    }

    pub fn try_clone_file(&self) -> io::Result<File> {
        self.file.try_clone()
    }

    /// Bring the index in line with a recovered segment whose records end at
    /// `log_end`. Entries past that end or out of order are dropped, then
    /// entries for the records after the last kept one are rebuilt with the
    /// writer's stride rule. The index syncs lazily, so after a crash it can
    /// lag the log, and a rebuilt tail keeps lookups in that range from
    /// scanning the whole segment.
    pub(crate) fn repair_against_log(
        &mut self,
        log: &File,
        base: u64,
        log_end: u64,
        stride: u32,
    ) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        let header = IDX_HEADER_LEN as u64;
        let len = self.file.metadata()?.len();
        if len < header {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "idx too small"));
        }
        let mut entries = vec![0u8; ((len - header) / IDX_ENTRY_LEN as u64) as usize * IDX_ENTRY_LEN];
        self.file.read_exact_at(&mut entries, header)?;
        let log_start = crate::segment::LOG_HEADER_LEN as u64;
        let mut kept = 0usize;
        let mut last_pos = log_start;
        let mut last_rel = None;
        for entry in entries.chunks_exact(IDX_ENTRY_LEN) {
            let rel = u32::from_be_bytes(entry[0..4].try_into().expect("exact-length slice"));
            let pos = u64::from_be_bytes(entry[8..16].try_into().expect("exact-length slice"));
            let out_of_order = (kept > 0 && pos <= last_pos) || last_rel.is_some_and(|r| rel <= r);
            if pos < log_start || pos >= log_end || out_of_order {
                break;
            }
            kept += 1;
            last_pos = pos;
            last_rel = Some(rel);
        }

        // The recovery scan already checked every record below `log_end`, so
        // the cheap frame peek is enough here. `window` holds the bytes from `pos`.
        let mut rebuilt = Vec::new();
        let mut index_at = last_pos;
        let mut pos = last_pos;
        let mut window = Vec::new();
        let mut chunk = vec![0u8; 64 * 1024];
        while pos < log_end {
            match crate::record::peek_record_prefix(&window) {
                Ok((offset, used)) if offset >= base => {
                    if pos - index_at >= stride as u64 {
                        let rel = (offset - base) as u32;
                        rebuilt.extend_from_slice(&rel.to_be_bytes());
                        rebuilt.extend_from_slice(&0u32.to_be_bytes());
                        rebuilt.extend_from_slice(&pos.to_be_bytes());
                        index_at = pos;
                    }
                    pos += used as u64;
                    window.drain(..used);
                }
                Err(crate::record::RecordError::Truncated) => {
                    let n = log.read_at(&mut chunk, pos + window.len() as u64)?;
                    if n == 0 {
                        break;
                    }
                    window.extend_from_slice(&chunk[..n]);
                }
                _ => break,
            }
        }

        self.file.set_len(header + (kept * IDX_ENTRY_LEN) as u64)?;
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&rebuilt)?;
        Ok(())
    }

    pub fn append_entries_raw(&mut self, bytes: &[u8]) -> io::Result<()> {
        // IMPORTANT: avoid seek(End) every time; keep cursor at end by never seeking elsewhere.
        self.file.write_all(bytes)
    }
}
