use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crc32c::crc32c;

const MAN_MAGIC: &[u8; 8] = b"KERATIN\0";
const MAN_VERSION: u16 = 1;

#[derive(Debug, Clone)]
pub struct Manifest {
    pub created_ts_ms: u64,
    pub segment_max_bytes: u64,
    pub index_stride_bytes: u32,
    pub active_base_offset: u64,
    pub next_offset: u64,
    pub head_offset: u64,
}

impl Manifest {
    pub fn default_new(now_ms: u64, segment_max_bytes: u64, index_stride_bytes: u32) -> Self {
        Self {
            created_ts_ms: now_ms,
            segment_max_bytes,
            index_stride_bytes,
            active_base_offset: 0,
            next_offset: 0,
            head_offset: 0,
        }
    }

    pub fn path(root: &Path) -> PathBuf {
        root.join("manifest.bin")
    }

    pub fn tmp_path(root: &Path) -> PathBuf {
        root.join("tmp").join("manifest.new")
    }

    pub fn load_or_create(
        root: &Path,
        now_ms: u64,
        segment_max_bytes: u64,
        index_stride_bytes: u32,
    ) -> io::Result<Self> {
        let p = Self::path(root);
        match File::open(&p) {
            Ok(mut f) => Self::read_from(&mut f),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let m = Self::default_new(now_ms, segment_max_bytes, index_stride_bytes);
                m.store_atomic(root)?;
                Ok(m)
            }
            Err(e) => Err(e),
        }
    }

    fn read_from(f: &mut File) -> io::Result<Self> {
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;

        // header: magic(8) ver(2) flags(2) header_len(4) crc(4) then payload
        if buf.len() < 8 + 2 + 2 + 4 + 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "manifest too small",
            ));
        }
        if &buf[0..8] != MAN_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad manifest magic",
            ));
        }
        let ver = u16::from_be_bytes(buf[8..10].try_into().unwrap());
        if ver != MAN_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad manifest version",
            ));
        }
        let header_len = u32::from_be_bytes(buf[12..16].try_into().unwrap()) as usize;
        if header_len != 8 + 2 + 2 + 4 + 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected manifest header_len",
            ));
        }
        let stored_crc = u32::from_be_bytes(buf[16..20].try_into().unwrap());
        let payload = &buf[20..];
        let crc = crc32c(payload);
        if crc != stored_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "manifest crc mismatch",
            ));
        }

        // payload v1:
        // created_ts(8) segment_max(8) index_stride(4) pad(4) active_base(8) next_offset(8) head_offset(8)
        if payload.len() != 8 + 8 + 4 + 4 + 8 + 8 + 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "manifest payload len mismatch",
            ));
        }
        let created_ts_ms = u64::from_be_bytes(payload[0..8].try_into().unwrap());
        let segment_max_bytes = u64::from_be_bytes(payload[8..16].try_into().unwrap());
        let index_stride_bytes = u32::from_be_bytes(payload[16..20].try_into().unwrap());
        let active_base_offset = u64::from_be_bytes(payload[24..32].try_into().unwrap());
        let next_offset = u64::from_be_bytes(payload[32..40].try_into().unwrap());
        let head_offset = u64::from_be_bytes(payload[40..48].try_into().unwrap());

        Ok(Self {
            created_ts_ms,
            segment_max_bytes,
            index_stride_bytes,
            active_base_offset,
            next_offset,
            head_offset,
        })
    }

    pub fn store_atomic(&self, root: &Path) -> io::Result<()> {
        fs::create_dir_all(root.join("tmp"))?;

        let tmp = Self::tmp_path(root);
        let finalp = Self::path(root);

        let mut f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?;

        // header
        let header_len: u32 = (8 + 2 + 2 + 4 + 4) as u32;
        let mut out = Vec::new();
        out.extend_from_slice(MAN_MAGIC);
        out.extend_from_slice(&MAN_VERSION.to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes()); // flags
        out.extend_from_slice(&header_len.to_be_bytes());

        // payload
        let mut payload = Vec::with_capacity(40);
        payload.extend_from_slice(&self.created_ts_ms.to_be_bytes());
        payload.extend_from_slice(&self.segment_max_bytes.to_be_bytes());
        payload.extend_from_slice(&self.index_stride_bytes.to_be_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes()); // pad
        payload.extend_from_slice(&self.active_base_offset.to_be_bytes());
        payload.extend_from_slice(&self.next_offset.to_be_bytes());
        payload.extend_from_slice(&self.head_offset.to_be_bytes());

        let crc = crc32c(&payload);
        out.extend_from_slice(&crc.to_be_bytes());
        out.extend_from_slice(&payload);

        f.write_all(&out)?;
        f.flush()?;
        f.sync_data()?;

        // atomic replace
        // On Windows, rename over existing can be tricky; simplest: remove then rename.
        if finalp.exists() {
            let _ = fs::remove_file(&finalp);
        }
        fs::rename(&tmp, &finalp)?;
        Ok(())
    }
}

#[test]
fn manifest_roundtrip() {
    let dir = crate::util::test_dir("test_data/manifest_roundtrip");
    let m1 = Manifest::default_new(123, 4096, 128);
    m1.store_atomic(&dir.root).unwrap();
    let m2 = Manifest::load_or_create(&dir.root, 0, 0, 0).unwrap();
    assert_eq!(m1.created_ts_ms, m2.created_ts_ms);
    assert_eq!(m1.segment_max_bytes, m2.segment_max_bytes);
    assert_eq!(m1.index_stride_bytes, m2.index_stride_bytes);
}
