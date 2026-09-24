use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crc32c::crc32c;

use crate::util::fsync_dir;

const MAN_MAGIC: &[u8; 8] = b"KERATIN\0";
const MAN_VERSION: u16 = 2;
const SHARED_MARKER: &[u8; 8] = b"SHARED\0\0";
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
    // Version 3 fences older binaries that lack shared-segment mutation barriers.
    pub shared_segments: bool,
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
            shared_segments: false,
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

    pub(crate) fn read_from(f: &mut File) -> io::Result<Self> {
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
        if ver != MAN_VERSION && ver != 3 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported Keratin manifest version {ver}, expected 2 or 3. Pre-0.1 data may need to be recreated"
                ),
            ));
        }
        if ver == 3 && !cfg!(unix) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "shared-segment manifests require Unix mutation barriers",
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
        let expected_len = if ver == 3 { 64 } else { 56 };
        if payload.len() != expected_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "manifest payload len mismatch",
            ));
        }
        // Version 3 has a checksum-covered extension and a different length.
        // Corrupting just its version header cannot make an old reader accept
        // shared storage as an ordinary version 2 log.
        if ver == 3 && &payload[56..] != SHARED_MARKER {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid shared manifest marker"));
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
            shared_segments: ver == 3,
        })
    }

    pub fn store_atomic(&self, root: &Path) -> io::Result<()> {
        self.store_atomic_with_rename(root, |from, to| fs::rename(from, to))
    }

    fn store_atomic_with_rename(
        &self,
        root: &Path,
        rename: impl FnOnce(&Path, &Path) -> io::Result<()>,
    ) -> io::Result<()> {
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
        let version = if self.shared_segments { 3u16 } else { MAN_VERSION };
        out.extend_from_slice(&version.to_be_bytes());
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
        if self.shared_segments { payload.extend_from_slice(SHARED_MARKER); }

        let crc = crc32c(&payload);
        out.extend_from_slice(&crc.to_be_bytes());
        out.extend_from_slice(&payload);

        f.write_all(&out)?;
        f.flush()?;
        f.sync_all()?;
        drop(f);

        // Replace the existing file directly. Unlinking first exposes a missing
        // manifest on error/crash, which load_or_create would reset to epoch 0.
        // std::fs::rename also supports replacing an existing file on Windows.
        rename(&tmp, &finalp)?;

        fsync_dir(root)?;
        fsync_dir(&root.join("tmp"))?;

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

#[test]
fn failed_manifest_rename_preserves_the_previous_fence_and_can_retry() {
    let dir = crate::test_dir!("manifest_failed_rename");
    let mut old = Manifest::default_new(123, 4096, 128);
    old.epoch = 7;
    old.next_offset = 23;
    old.store_atomic(&dir.root).unwrap();
    let mut new = old.clone();
    new.epoch = 8;
    new.next_offset = 29;
    let failure = new.store_atomic_with_rename(&dir.root, |_, _| {
        Err(io::Error::other("injected rename failure"))
    });
    assert!(failure.is_err());
    let recovered = Manifest::load_or_create(&dir.root, 0, 0, 0).unwrap();
    assert_eq!(
        recovered.epoch, 7,
        "failed replacement must retain the old fence"
    );
    assert_eq!(recovered.next_offset, 23);
    new.store_atomic(&dir.root).unwrap();
    let recovered = Manifest::load_or_create(&dir.root, 0, 0, 0).unwrap();
    assert_eq!((recovered.epoch, recovered.next_offset), (8, 29));
}

#[test]
fn manifest_replacement_keeps_an_open_reader_on_the_previous_generation() {
    let dir = crate::test_dir!("manifest_open_reader");
    let mut old = Manifest::default_new(123, 4096, 128);
    old.epoch = 7;
    old.store_atomic(&dir.root).unwrap();
    let mut reader = File::open(Manifest::path(&dir.root)).unwrap();
    old.epoch = 8;
    old.store_atomic(&dir.root).unwrap();
    assert_eq!(Manifest::read_from(&mut reader).unwrap().epoch, 7);
    assert_eq!(
        Manifest::load_or_create(&dir.root, 0, 0, 0).unwrap().epoch,
        8
    );
}

#[cfg(unix)]
#[test]
fn shared_manifest_cannot_be_downgraded_by_corrupting_version_header() {
    let dir = crate::test_dir!("shared_manifest_version");
    let mut manifest = Manifest::default_new(0, 1024, 64);
    manifest.shared_segments = true;
    manifest.store_atomic(&dir.root).unwrap();
    let path = Manifest::path(&dir.root);
    assert!(Manifest::read_from(&mut File::open(&path).unwrap()).unwrap().shared_segments);
    let mut bytes = fs::read(&path).unwrap();
    assert_eq!(bytes.len(), 84);
    bytes[8..10].copy_from_slice(&2u16.to_be_bytes());
    fs::write(&path, bytes).unwrap();
    assert!(Manifest::read_from(&mut File::open(&path).unwrap()).is_err());
}
