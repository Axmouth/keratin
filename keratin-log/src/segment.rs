use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};

pub const LOG_MAGIC: &[u8; 8] = b"KLOG\0\0\0\0";
pub const LOG_VERSION: u16 = 1;
pub const LOG_HEADER_LEN: u32 = 8 + 2 + 2 + 4 + 8 + 8 + 32 + 4;

pub struct Segment {
    pub base_offset: u64,
    file: File,
    pub bytes_written: u64,
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

        Ok(Self {
            base_offset,
            file,
            bytes_written: header_len as u64,
        })
    }

    pub fn open(mut file: File, expected_base: u64) -> io::Result<Self> {
        let mut hdr = [0u8; 8 + 2 + 2 + 4 + 8 + 8 + 32 + 4];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut hdr)?;
        if &hdr[0..8] != LOG_MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad log magic"));
        }
        let ver = u16::from_be_bytes(hdr[8..10].try_into().unwrap());
        if ver != LOG_VERSION {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad log version"));
        }
        let base = u64::from_be_bytes(hdr[16..24].try_into().unwrap());
        if base != expected_base {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "log base offset mismatch"));
        }
        let len = file.metadata()?.len();
        Ok(Self { base_offset: base, file, bytes_written: len })
    }

    pub fn append_bytes(&mut self, data: &[u8]) -> io::Result<u64> {
        // returns start position where this append begins
        let start = self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(data)?;
        self.bytes_written += data.len() as u64;
        Ok(start)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    pub fn fsync(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    pub fn set_len(&mut self, new_len: u64) -> io::Result<()> {
        self.file.set_len(new_len)?;
        self.bytes_written = new_len;
        Ok(())
    }

    // A tiny helper so recovery can borrow File.
    // Segment currently stores File privately; simplest v0: add a method on Segment to expose &File.
    // If you don't want that, scan using std::fs::File reopened by path instead.
    pub fn file_ref(&self) -> &File {
        &self.file
    }
}
