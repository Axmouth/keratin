use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};

pub const IDX_MAGIC: &[u8; 8] = b"KIDX\0\0\0\0";
pub const IDX_VERSION: u16 = 1;
pub const IDX_HEADER_LEN: u32 = 8 + 2 + 2 + 4 + 8 + 8 + 2 + 2 + 32 + 4;
pub const IDX_ENTRY_LEN: usize = 16;

pub struct Index {
    file: File,
}

impl Index {
    pub fn create(mut file: File, base_offset: u64, created_ts_ms: u64) -> io::Result<Self> {
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
        file.sync_all()?;

        Ok(Self { file })
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

        Ok(Self { file })
    }

    pub fn fsync(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    pub fn try_clone_file(&self) -> io::Result<File> {
        self.file.try_clone()
    }

    /// Truncate idx to last complete entry (used on recovery).
    pub fn repair_truncate_to_entries(&mut self) -> io::Result<()> {
        let len = self.file.metadata()?.len();
        let header_len = 8 + 2 + 2 + 4 + 8 + 8 + 2 + 2 + 32 + 4;
        if len < header_len as u64 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "idx too small"));
        }
        let entries_bytes = len - header_len as u64;
        let complete = entries_bytes / 16;
        let new_len = header_len as u64 + complete * 16;
        self.file.set_len(new_len)?;
        Ok(())
    }

    pub fn append_entries_raw(&mut self, bytes: &[u8]) -> io::Result<()> {
        // IMPORTANT: avoid seek(End) every time; keep cursor at end by never seeking elsewhere.
        self.file.write_all(bytes)
    }
}
