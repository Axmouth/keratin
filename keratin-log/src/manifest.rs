use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crc32c::crc32c;

use crate::util::fsync_dir;

const MAN_MAGIC: &[u8; 8] = b"KERATIN\0";
const MAN_VERSION: u16 = 2;
const MAN_FLAG_CLEAN_SHUTDOWN: u16 = 0x0001;

#[derive(Debug, Clone)]
pub struct Manifest {
    pub created_ts_ms: u64,
    pub segment_max_bytes: u64,
    pub index_stride_bytes: u32,
    pub active_base_offset: u64,
    pub next_offset: u64,
    pub head_offset: u64,
    pub epoch: u64,
    pub clean_shutdown: bool,
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
            epoch: 0,
            clean_shutdown: true,
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
        let ver = u16::from_be_bytes(buf[8..10].try_into().expect("exact-length slice"));
        let flags = u16::from_be_bytes(buf[10..12].try_into().expect("exact-length slice"));
        if ver != MAN_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported Keratin manifest version {ver}, expected {MAN_VERSION}. Pre-0.1 data may need to be recreated"
                ),
            ));
        }
        let header_len =
            u32::from_be_bytes(buf[12..16].try_into().expect("exact-length slice")) as usize;
        if header_len != 8 + 2 + 2 + 4 + 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected manifest header_len",
            ));
        }
        let stored_crc = u32::from_be_bytes(buf[16..20].try_into().expect("exact-length slice"));
        let payload = &buf[20..];
        let crc = crc32c(payload);
        if crc != stored_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "manifest crc mismatch",
            ));
        }

        // payload v2:
        // created_ts(8) segment_max(8) index_stride(4) pad(4)
        // active_base(8) next_offset(8) head_offset(8) epoch(8)
        if payload.len() != 8 + 8 + 4 + 4 + 8 + 8 + 8 + 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "manifest payload len mismatch",
            ));
        }
        let created_ts_ms =
            u64::from_be_bytes(payload[0..8].try_into().expect("exact-length slice"));
        let segment_max_bytes =
            u64::from_be_bytes(payload[8..16].try_into().expect("exact-length slice"));
        let index_stride_bytes =
            u32::from_be_bytes(payload[16..20].try_into().expect("exact-length slice"));
        let active_base_offset =
            u64::from_be_bytes(payload[24..32].try_into().expect("exact-length slice"));
        let next_offset =
            u64::from_be_bytes(payload[32..40].try_into().expect("exact-length slice"));
        let head_offset =
            u64::from_be_bytes(payload[40..48].try_into().expect("exact-length slice"));
        let epoch = u64::from_be_bytes(payload[48..56].try_into().expect("exact-length slice"));

        Ok(Self {
            created_ts_ms,
            segment_max_bytes,
            index_stride_bytes,
            active_base_offset,
            next_offset,
            head_offset,
            epoch,
            clean_shutdown: flags & MAN_FLAG_CLEAN_SHUTDOWN != 0,
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
        let flags = if self.clean_shutdown {
            MAN_FLAG_CLEAN_SHUTDOWN
        } else {
            0
        };
        out.extend_from_slice(&flags.to_be_bytes());
        out.extend_from_slice(&header_len.to_be_bytes());

        // payload
        let mut payload = Vec::with_capacity(56);
        payload.extend_from_slice(&self.created_ts_ms.to_be_bytes());
        payload.extend_from_slice(&self.segment_max_bytes.to_be_bytes());
        payload.extend_from_slice(&self.index_stride_bytes.to_be_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes()); // pad
        payload.extend_from_slice(&self.active_base_offset.to_be_bytes());
        payload.extend_from_slice(&self.next_offset.to_be_bytes());
        payload.extend_from_slice(&self.head_offset.to_be_bytes());
        payload.extend_from_slice(&self.epoch.to_be_bytes());

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

        fsync_dir(root)?;

        Ok(())
    }
}

#[test]
fn manifest_roundtrip() {
    use crate::test_dir;

    let dir = test_dir!("test_data/manifest_roundtrip");
    let mut m1 = Manifest::default_new(123, 4096, 128);
    m1.epoch = 7;
    m1.store_atomic(&dir.root).unwrap();
    let m2 = Manifest::load_or_create(&dir.root, 0, 0, 0).unwrap();
    assert_eq!(m1.created_ts_ms, m2.created_ts_ms);
    assert_eq!(m1.segment_max_bytes, m2.segment_max_bytes);
    assert_eq!(m1.index_stride_bytes, m2.index_stride_bytes);
    assert_eq!(m1.epoch, m2.epoch);
    assert_eq!(m1.clean_shutdown, m2.clean_shutdown);
}

#[test]
fn manifest_dirty_flag_roundtrip() {
    use crate::test_dir;

    let dir = test_dir!("test_data/manifest_dirty_flag_roundtrip");
    let mut m1 = Manifest::default_new(123, 4096, 128);
    m1.clean_shutdown = false;
    m1.store_atomic(&dir.root).unwrap();
    let m2 = Manifest::load_or_create(&dir.root, 0, 0, 0).unwrap();
    assert!(!m2.clean_shutdown);
}
