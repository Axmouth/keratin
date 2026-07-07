use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};

pub const LOG_MAGIC: &[u8; 8] = b"KLOG\0\0\0\0";
pub const LOG_VERSION: u16 = 1;
pub const LOG_HEADER_LEN: u32 = 8 + 2 + 2 + 4 + 8 + 8 + 32 + 4;

pub struct Segment {
    pub base_offset: u64,
    file: File,
    /// Logical bytes written (header + records). The authoritative write cursor
    /// and end-of-data, independent of the physical file size. With
    /// preallocation the file is larger than this (zero-padded tail).
    pub bytes_written: u64,
    /// Physically allocated size of the file. `>= bytes_written`. When
    /// preallocation is off this tracks `bytes_written` (file grows with writes).
    alloc_end: u64,
    /// Preallocate this many bytes ahead of the write cursor (`0` = off). Set on
    /// the active segment. Turned off automatically if the filesystem rejects
    /// `fallocate`.
    prealloc_chunk: u64,
}

impl Segment {
    pub fn create(mut file: File, base_offset: u64, created_ts_ms: u64) -> io::Result<Self> {
        let header_len: u32 = LOG_HEADER_LEN;

        let mut hdr = Vec::with_capacity(header_len as usize);
        hdr.extend_from_slice(LOG_MAGIC);
        hdr.extend_from_slice(&LOG_VERSION.to_be_bytes());
        hdr.extend_from_slice(&0u16.to_be_bytes()); // flags
        hdr.extend_from_slice(&header_len.to_be_bytes());
        hdr.extend_from_slice(&base_offset.to_be_bytes());
        hdr.extend_from_slice(&created_ts_ms.to_be_bytes());
        hdr.extend_from_slice(&[0u8; 32]); // reserved
        let crc = crc32c::crc32c(&hdr);
        hdr.extend_from_slice(&crc.to_be_bytes());

        file.seek(SeekFrom::Start(0))?;
        file.write_all(&hdr)?;
        file.flush()?;
        file.sync_all()?;

        Ok(Self {
            base_offset,
            file,
            bytes_written: header_len as u64,
            alloc_end: header_len as u64,
            prealloc_chunk: 0,
        })
    }

    pub fn open(mut file: File, expected_base: u64) -> io::Result<Self> {
        let mut hdr = [0u8; 8 + 2 + 2 + 4 + 8 + 8 + 32 + 4];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut hdr)?;
        if &hdr[0..8] != LOG_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad log magic"));
        }
        let ver = u16::from_be_bytes(hdr[8..10].try_into().expect("exact-length slice"));
        if ver != LOG_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad log version",
            ));
        }
        let base = u64::from_be_bytes(hdr[16..24].try_into().expect("exact-length slice"));
        if base != expected_base {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "log base offset mismatch",
            ));
        }
        let len = file.metadata()?.len();
        Ok(Self {
            base_offset: base,
            file,
            // On a padded segment (crash with preallocation) this is temporarily
            // the padded size; the recovery scan corrects it to the logical tail
            // via `set_len`. On a clean open the padding was trimmed at shutdown,
            // so len == data size.
            bytes_written: len,
            alloc_end: len,
            prealloc_chunk: 0,
        })
    }

    pub fn append_bytes(&mut self, data: &[u8]) -> io::Result<u64> {
        // Preallocate ahead of the cursor so the write lands in already-allocated
        // blocks. fdatasync of an in-place write skips the block-allocation
        // metadata flush that an extending write pays.
        if self.prealloc_chunk > 0 {
            let need = self.bytes_written + data.len() as u64;
            if need > self.alloc_end {
                self.grow_alloc(need);
            }
        }
        // Write at the logical cursor, not SeekFrom::End: with preallocation the
        // file end is past the data (zero padding).
        let start = self.bytes_written;
        self.file.seek(SeekFrom::Start(start))?;
        self.file.write_all(data)?;
        self.bytes_written += data.len() as u64;
        Ok(start)
    }

    /// Enable preallocation on this (active) segment: keep `chunk` bytes allocated
    /// ahead of the write cursor. A filesystem that rejects `fallocate` silently
    /// disables it (writes just extend the file, correctness unchanged).
    pub fn enable_prealloc(&mut self, chunk: u64) {
        if chunk == 0 {
            return;
        }
        self.prealloc_chunk = chunk;
        let need = self.bytes_written + chunk;
        if need > self.alloc_end {
            self.grow_alloc(need);
        }
    }

    /// Grow the physical allocation to at least `need`, rounded up to a chunk
    /// boundary. Best-effort: on failure (e.g. filesystem without fallocate)
    /// preallocation is turned off and writes fall back to plain extend.
    fn grow_alloc(&mut self, need: u64) {
        use fs2::FileExt;
        let chunk = self.prealloc_chunk.max(1);
        let target = need.div_ceil(chunk).saturating_mul(chunk).max(need);
        match self.file.allocate(target) {
            Ok(()) => self.alloc_end = target,
            Err(_) => self.prealloc_chunk = 0,
        }
    }

    /// Trim the physical file down to the logical bytes written, removing any
    /// preallocated padding. Called on clean shutdown so the next clean open sees
    /// file size == data size and needs no scan.
    pub fn trim_to_written(&mut self) -> io::Result<()> {
        if self.alloc_end > self.bytes_written {
            self.file.set_len(self.bytes_written)?;
            self.alloc_end = self.bytes_written;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    pub fn fsync(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    pub fn try_clone_file(&self) -> io::Result<File> {
        self.file.try_clone()
    }

    pub fn set_len(&mut self, new_len: u64) -> io::Result<()> {
        self.file.set_len(new_len)?;
        self.bytes_written = new_len;
        self.alloc_end = new_len;
        Ok(())
    }

    // A tiny helper so recovery can borrow File.
    // Segment currently stores File privately; simplest v0: add a method on Segment to expose &File.
    // If you don't want that, scan using std::fs::File reopened by path instead.
    pub fn file_ref(&self) -> &File {
        &self.file
    }
}

/// Summary of one segment, for retention decisions. Cheap to build from the
/// in-memory segment list plus each segment file's header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentInfo {
    pub base_offset: u64,
    /// Exclusive end: one past the last offset this segment can hold. For a sealed
    /// segment this is the next segment's base; for the active segment it is the
    /// log's next offset.
    pub end_offset: u64,
    /// On-disk size of the segment file in bytes (including the header).
    pub bytes: u64,
    /// Wall-clock time the segment file was created, in milliseconds.
    pub created_ts_ms: u64,
    /// False only for the active (still-being-written) segment, which retention
    /// never drops.
    pub sealed: bool,
}

/// Read a segment file's creation timestamp (ms) from its header without opening
/// the whole segment.
pub fn read_segment_created_ts_ms(path: &std::path::Path) -> io::Result<u64> {
    let mut file = File::open(path)?;
    // magic(8) ver(2) flags(2) hdrlen(4) base(8) created_ts(8)
    let mut hdr = [0u8; 32];
    file.read_exact(&mut hdr)?;
    if &hdr[0..8] != LOG_MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad log magic"));
    }
    Ok(u64::from_be_bytes(
        hdr[24..32].try_into().expect("exact-length slice"),
    ))
}
