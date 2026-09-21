//! Journaled suffix truncation. Every mutation is repeatable after interruption;
//! records below the cut are never rewritten or removed. Caller holds the log
//! lock and has drained readers, appends and outstanding fsync completions.

use crate::{
    frozen_reader::{MAX_FROZEN_RECORD_BYTES, read_segment_header},
    index::Index,
    manifest::Manifest,
    record::{RECORD_HEADER_LEN, decode_record_prefix},
    segment::LOG_HEADER_LEN,
    util::fsync_dir,
};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, Write},
    path::Path,
};

pub(crate) const JOURNAL: &str = "suffix-repair.pending";
const MAGIC: &[u8; 8] = b"KSUFFIX1";
const SIZE: usize = 8 + 6 * 8 + 4;

#[derive(Debug, Clone, Copy)]
struct Repair {
    epoch: u64,
    head: u64,
    old_next: u64,
    next: u64,
    base: u64,
    pos: u64,
}

fn invalid(text: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, text)
}

fn prefix_end(root: &Path, base: u64, next: u64) -> io::Result<u64> {
    let mut file = File::open(root.join("segments").join(format!("{base:020}.log")))?;
    read_segment_header(&mut file, base)?;
    for expected in base..next {
        let mut fixed = [0u8; RECORD_HEADER_LEN];
        file.read_exact(&mut fixed)?;
        let headers = u32::from_be_bytes(fixed[8..12].try_into().unwrap()) as usize;
        let payload = u32::from_be_bytes(fixed[12..16].try_into().unwrap()) as usize;
        let total = RECORD_HEADER_LEN
            .checked_add(headers)
            .and_then(|n| n.checked_add(payload))
            .and_then(|n| n.checked_add(4))
            .ok_or_else(|| invalid("repair record size overflow"))?;
        if total > MAX_FROZEN_RECORD_BYTES {
            return Err(invalid("repair record exceeds allocation limit"));
        }
        let mut bytes = vec![0; total];
        bytes[..RECORD_HEADER_LEN].copy_from_slice(&fixed);
        file.read_exact(&mut bytes[RECORD_HEADER_LEN..])?;
        let (record, _) =
            decode_record_prefix(&bytes).map_err(|_| invalid("corrupt retained repair prefix"))?;
        if record.offset != expected {
            return Err(invalid("noncontiguous retained repair prefix"));
        }
    }
    file.stream_position()
}

fn segment_base(path: &Path) -> io::Result<Option<u64>> {
    if !matches!(
        path.extension().and_then(|v| v.to_str()),
        Some("log" | "idx")
    ) {
        return Ok(None);
    }
    let name = path
        .file_stem()
        .and_then(|v| v.to_str())
        .ok_or_else(|| invalid("invalid repair segment name"))?;
    let base: u64 = name
        .parse()
        .map_err(|_| invalid("invalid repair segment base"))?;
    if name != format!("{base:020}") {
        return Err(invalid("noncanonical repair segment name"));
    }
    Ok(Some(base))
}

pub(crate) fn prepare(root: &Path, manifest: &Manifest, next: u64) -> io::Result<()> {
    if !cfg!(unix) {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "durable suffix repair requires Unix directory sync",
        ));
    }
    if root.join(JOURNAL).try_exists()? {
        return Err(invalid("suffix repair already pending"));
    }
    if next < manifest.head_offset || next > manifest.next_offset {
        return Err(invalid("suffix repair cut outside retained log"));
    }
    let mut base = None;
    for entry in fs::read_dir(root.join("segments"))? {
        let path = entry?.path();
        if path.extension().and_then(|v| v.to_str()) == Some("log") {
            if let Some(value) = segment_base(&path)? {
                if value <= next {
                    base = Some(base.unwrap_or(0).max(value));
                }
            }
        }
    }
    let base = base.ok_or_else(|| invalid("missing suffix repair boundary segment"))?;
    let repair = Repair {
        epoch: manifest.epoch,
        head: manifest.head_offset,
        old_next: manifest.next_offset,
        next,
        base,
        pos: prefix_end(root, base, next)?,
    };
    let mut bytes = MAGIC.to_vec();
    for value in [
        repair.epoch,
        repair.head,
        repair.old_next,
        repair.next,
        repair.base,
        repair.pos,
    ] {
        bytes.extend_from_slice(&value.to_be_bytes());
    }
    bytes.extend_from_slice(&crc32c::crc32c(&bytes).to_be_bytes());
    fs::create_dir_all(root.join("tmp"))?;
    let tmp = root.join("tmp/suffix-repair.new");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(tmp, root.join(JOURNAL))?;
    fsync_dir(root)
}

pub(crate) fn resume(root: &Path) -> io::Result<bool> {
    resume_with_hook(root, |_| Ok(()))
}

fn resume_with_hook(root: &Path, mut after: impl FnMut(u8) -> io::Result<()>) -> io::Result<bool> {
    let mut file = match File::open(root.join(JOURNAL)) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e),
    };
    if !cfg!(unix) {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "durable suffix repair requires Unix directory sync",
        ));
    }
    if file.metadata()?.len() != SIZE as u64 {
        return Err(invalid("invalid suffix repair journal size"));
    }
    let mut bytes = [0; SIZE];
    file.read_exact(&mut bytes)?;
    if &bytes[..8] != MAGIC
        || crc32c::crc32c(&bytes[..SIZE - 4])
            != u32::from_be_bytes(bytes[SIZE - 4..].try_into().unwrap())
    {
        return Err(invalid("invalid suffix repair journal checksum or version"));
    }
    let n = |i: usize| u64::from_be_bytes(bytes[8 + i * 8..16 + i * 8].try_into().unwrap());
    let repair = Repair {
        epoch: n(0),
        head: n(1),
        old_next: n(2),
        next: n(3),
        base: n(4),
        pos: n(5),
    };
    let mut manifest = Manifest::read_from(&mut File::open(Manifest::path(root))?)?;
    if repair.head > repair.next
        || repair.next > repair.old_next
        || repair.base > repair.next
        || repair.pos < LOG_HEADER_LEN as u64
        || manifest.epoch != repair.epoch
        || manifest.head_offset != repair.head
        || ![repair.old_next, repair.next].contains(&manifest.next_offset)
        || prefix_end(root, repair.base, repair.next)? != repair.pos
    {
        return Err(invalid(
            "suffix repair journal does not match retained history",
        ));
    }
    after(0)?;
    let segments = root.join("segments");
    let boundary = OpenOptions::new()
        .write(true)
        .open(segments.join(format!("{:020}.log", repair.base)))?;
    boundary.set_len(repair.pos)?;
    boundary.sync_all()?;
    after(1)?;
    // An empty sparse index safely falls back to scanning the retained segment.
    // Replace it atomically so an interrupted header write cannot poison reopen.
    fs::create_dir_all(root.join("tmp"))?;
    let tmp = root.join("tmp/suffix-repair.idx");
    let index = Index::create(
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?,
        repair.base,
        crate::util::unix_millis(),
    )?;
    index.fsync()?;
    fs::rename(tmp, segments.join(format!("{:020}.idx", repair.base)))?;
    after(2)?;
    for entry in fs::read_dir(&segments)? {
        let path = entry?.path();
        if segment_base(&path)?.is_some_and(|base| base > repair.base) {
            fs::remove_file(path)?;
        }
    }
    fsync_dir(&segments)?;
    after(3)?;
    manifest.active_base_offset = repair.base;
    manifest.next_offset = repair.next;
    manifest.clean_shutdown = false;
    manifest.store_atomic(root)?;
    after(4)?;
    fs::remove_file(root.join(JOURNAL))?;
    fsync_dir(root)?;
    after(5)?;
    Ok(true)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::{KDurability, Keratin, KeratinConfig, Message};
    use std::time::{Duration, Instant};

    fn config() -> KeratinConfig {
        KeratinConfig {
            segment_max_bytes: 280,
            index_stride_bytes: 1,
            segment_preallocate_bytes: 4096,
            writer_buffer_factor: 1,
            tail_cache_bytes: 4096,
            ..KeratinConfig::test_default()
        }
    }
    async fn fixture(root: &Path) {
        let log = Keratin::open(root, config()).await.unwrap();
        log.advance_epoch(4).await.unwrap();
        for off in 0..8u8 {
            log.append_batch(
                vec![Message {
                    flags: off as u16,
                    headers: vec![off],
                    payload: vec![off; 80],
                }],
                Some(KDurability::AfterFsync),
            )
            .await
            .unwrap();
        }
        log.shutdown().await.unwrap();
    }
    fn journal(root: &Path, next: u64) {
        let manifest = Manifest::read_from(&mut File::open(Manifest::path(root)).unwrap()).unwrap();
        prepare(root, &manifest, next).unwrap();
    }
    async fn verify(root: &Path, next: u64) {
        let log = Keratin::open(root, config()).await.unwrap();
        assert_eq!(log.head_offset(), 0);
        assert_eq!(log.next_offset(), next);
        assert_eq!(log.current_epoch(), 4);
        let records = log.reader().scan_from_disk(0, 100).unwrap();
        assert_eq!(records.len(), next as usize);
        for (off, record) in records.iter().enumerate() {
            assert_eq!(record.offset, off as u64);
            assert_eq!(record.payload, vec![off as u8; 80]);
            assert_eq!(record.headers, vec![off as u8]);
            assert_eq!(record.flags, off as u16);
        }
        assert!(log.reader().fetch(next).unwrap().is_none());
        let appended = log
            .append_batch(
                vec![Message {
                    flags: 0,
                    headers: vec![],
                    payload: b"replacement".to_vec(),
                }],
                Some(KDurability::AfterFsync),
            )
            .await
            .unwrap();
        assert_eq!(appended.base_offset, next);
        assert_eq!(
            log.reader().fetch(next).unwrap().unwrap().payload,
            b"replacement"
        );
        log.shutdown().await.unwrap();
        drop(log);
        let log = Keratin::open(root, config()).await.unwrap();
        assert_eq!(log.next_offset(), next + 1);
        assert_eq!(
            log.reader().fetch(next).unwrap().unwrap().payload,
            b"replacement"
        );
        log.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn suffix_repair_resumes_each_io_error_boundary_and_preserves_prefix() {
        for next in [0, 1, 3, 7, 8] {
            for phase in 0..=5 {
                let dir = crate::test_dir!("suffix_repair_io");
                fixture(&dir.root).await;
                journal(&dir.root, next);
                let error = resume_with_hook(&dir.root, |at| {
                    if at == phase {
                        Err(io::Error::other("injected repair I/O error"))
                    } else {
                        Ok(())
                    }
                });
                assert!(error.is_err());
                verify(&dir.root, next).await;
                assert!(!dir.root.join(JOURNAL).exists());
            }
        }
    }

    #[tokio::test]
    async fn suffix_repair_rejects_corrupt_journal_wrong_epoch_and_unretained_cut() {
        let dir = crate::test_dir!("suffix_repair_invalid");
        fixture(&dir.root).await;
        let manifest =
            Manifest::read_from(&mut File::open(Manifest::path(&dir.root)).unwrap()).unwrap();
        assert!(prepare(&dir.root, &manifest, 9).is_err());
        assert!(!dir.root.join(JOURNAL).exists());
        journal(&dir.root, 3);
        let original = fs::read(dir.root.join(JOURNAL)).unwrap();
        let mut corrupt = original.clone();
        corrupt[20] ^= 1;
        fs::write(dir.root.join(JOURNAL), corrupt).unwrap();
        assert!(Keratin::open(&dir.root, config()).await.is_err());
        fs::write(dir.root.join(JOURNAL), original).unwrap();
        let mut changed = manifest.clone();
        changed.epoch += 1;
        changed.store_atomic(&dir.root).unwrap();
        assert!(Keratin::open(&dir.root, config()).await.is_err());
        manifest.store_atomic(&dir.root).unwrap();
        verify(&dir.root, 3).await;
    }

    #[tokio::test]
    async fn suffix_repair_live_writer_clears_cache_and_enforces_epoch_and_role() {
        let dir = crate::test_dir!("suffix_repair_live");
        fixture(&dir.root).await;
        let log = Keratin::open(&dir.root, config()).await.unwrap();
        assert!(log.repair_suffix_at_epoch(3, 4).await.is_err());
        log.append_batch(
            vec![Message {
                flags: 0,
                headers: vec![],
                payload: b"cached tail".to_vec(),
            }],
            Some(KDurability::AfterFsync),
        )
        .await
        .unwrap();
        assert_eq!(log.reader().scan_from(8, 1).unwrap().len(), 1);
        log.become_follower();
        assert!(log.repair_suffix_at_epoch(3, 3).await.is_err());
        assert!(log.repair_suffix_at_epoch(10, 4).await.is_err());
        log.repair_suffix_at_epoch(3, 4).await.unwrap();
        assert_eq!(log.next_offset(), 3);
        assert_eq!(log.head_offset(), 0);
        assert_eq!(log.reader().scan_from(0, 100).unwrap().len(), 3);
        assert!(log.reader().scan_from(8, 1).unwrap().is_empty());
        log.shutdown().await.unwrap();
        drop(log);
        verify(&dir.root, 3).await;
    }

    #[tokio::test]
    async fn suffix_repair_preserves_a_nonzero_retained_head() {
        let dir = crate::test_dir!("suffix_repair_nonzero_head");
        fixture(&dir.root).await;
        let log = Keratin::open(&dir.root, config()).await.unwrap();
        log.truncate_before(4).await.unwrap();
        let head = log.head_offset();
        assert!(head > 0 && head <= 4);
        log.become_follower();
        assert!(log.repair_suffix_at_epoch(head - 1, 4).await.is_err());
        log.repair_suffix_at_epoch(6, 4).await.unwrap();
        assert_eq!(log.head_offset(), head);
        log.shutdown().await.unwrap();
        drop(log);
        let log = Keratin::open(&dir.root, config()).await.unwrap();
        assert_eq!(log.head_offset(), head);
        assert_eq!(log.next_offset(), 6);
        let records = log.reader().scan_from_disk(head, 100).unwrap();
        assert_eq!(records.len() as u64, 6 - head);
        assert_eq!(records[0].offset, head);
        log.shutdown().await.unwrap();
    }

    // Child is invoked only by the kill test below. The parent kills it while
    // the journal is at a known durable phase; no orderly shutdown can help it.
    #[test]
    fn suffix_repair_crash_worker() {
        let Ok(root) = std::env::var("KERATIN_SUFFIX_CRASH_ROOT") else {
            return;
        };
        let phase: u8 = std::env::var("KERATIN_SUFFIX_CRASH_PHASE")
            .unwrap()
            .parse()
            .unwrap();
        let root = Path::new(&root);
        resume_with_hook(root, |at| {
            if at == phase {
                fs::write(root.join("crash-ready"), b"ready").unwrap();
                loop {
                    std::thread::park();
                }
            }
            Ok(())
        })
        .unwrap();
        panic!("crash phase not reached");
    }

    #[tokio::test]
    async fn suffix_repair_survives_sigkill_at_every_durable_boundary() {
        for phase in 0..=5 {
            let dir = crate::test_dir!("suffix_repair_sigkill");
            fixture(&dir.root).await;
            journal(&dir.root, 3);
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "suffix_repair::tests::suffix_repair_crash_worker",
                    "--nocapture",
                ])
                .env("KERATIN_SUFFIX_CRASH_ROOT", &dir.root)
                .env("KERATIN_SUFFIX_CRASH_PHASE", phase.to_string())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap();
            let start = Instant::now();
            while !dir.root.join("crash-ready").exists() {
                if start.elapsed() > Duration::from_secs(10) || child.try_wait().unwrap().is_some()
                {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("suffix repair child did not reach phase {phase}");
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            child.kill().unwrap();
            child.wait().unwrap();
            verify(&dir.root, 3).await;
        }
    }
}
