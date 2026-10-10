//! Bounded reclamation of private inspection caches after a transition retires.
//! Active logs, recovery stages and installed receipts are outside this tree.
use keratin_log::{lock::FileLock, util::fsync_dir};
use std::{
    fs::{self, File},
    io,
    path::Path,
};

#[derive(Debug, Default)]
pub struct RecoveryCacheCleanup {
    pub removed_files: usize,
    pub removed_bytes: u64,
    pub busy_entries: usize,
    pub more: bool,
}
fn hashed_name(path: &Path) -> bool {
    path.file_name().and_then(|s| s.to_str()).is_some_and(|s| {
        s.len() == 64
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}
fn lock(root: &Path) -> io::Result<Option<FileLock>> {
    match FileLock::try_exclusive(&root.join("lock")) {
        Ok(lock) => Ok(Some(lock)),
        Err(e) if matches!(e.kind(), io::ErrorKind::NotFound | io::ErrorKind::WouldBlock) => Ok(None),
        Err(e) => Err(e),
    }
}
fn remove(path: &Path, max_files: usize, result: &mut RecoveryCacheCleanup) -> io::Result<bool> {
    if result.removed_files >= max_files {
        result.more = true;
        return Ok(false);
    }
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.is_file() => {
            fs::remove_file(path)?;
            result.removed_files += 1;
            result.removed_bytes = result.removed_bytes.saturating_add(meta.len());
            Ok(true)
        }
        Ok(_) => Ok(false),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}
fn journals(root: &Path, max_files: usize, result: &mut RecoveryCacheCleanup) -> io::Result<()> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() || !hashed_name(&entry.path()) {
            continue;
        }
        let Some(_lock) = lock(&entry.path())? else {
            result.busy_entries += 1;
            result.more = true;
            continue;
        };
        let mut changed = false;
        for page in fs::read_dir(entry.path())? {
            let page = page?;
            if page.file_type()?.is_file()
                && (page.path().extension().is_some_and(|e| e == "page")
                    || page.file_name() == "page.writing")
            {
                changed |= remove(&page.path(), max_files, result)?;
                if result.more && result.removed_files >= max_files {
                    break;
                }
            }
        }
        if changed {
            fsync_dir(&entry.path())?;
        }
        if result.removed_files >= max_files {
            result.more = true;
            return Ok(());
        }
    }
    Ok(())
}
fn phases(root: &Path, max_files: usize, result: &mut RecoveryCacheCleanup) -> io::Result<()> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() || !hashed_name(&entry.path()) {
            continue;
        }
        let Some(_lock) = lock(&entry.path())? else {
            result.busy_entries += 1;
            result.more = true;
            continue;
        };
        // Retire the complete result before removing its artifact. Reopening
        // after interruption sees an incomplete phase and can reconstruct it.
        let changed = remove(&entry.path().join("complete"), max_files, result)?;
        if changed {
            fsync_dir(&entry.path())?;
        }
        if result.removed_files >= max_files {
            result.more = true;
            return Ok(());
        }
        remove(&entry.path().join("phase.writing"), max_files, result)?;
        let artifact_root = entry.path().join("artifact");
        if artifact_root.is_dir() {
            for artifact in fs::read_dir(&artifact_root)? {
                let artifact = artifact?;
                if !artifact.file_type()?.is_dir() || !hashed_name(&artifact.path()) {
                    continue;
                }
                let Some(_artifact_lock) = lock(&artifact.path())? else {
                    result.busy_entries += 1;
                    result.more = true;
                    continue;
                };
                // Forget tentative progress durably before reclaiming the data.
                let changed = remove(&artifact.path().join("progress"), max_files, result)?;
                if changed {
                    fsync_dir(&artifact.path())?;
                }
                if result.removed_files >= max_files {
                    result.more = true;
                    return Ok(());
                }
                remove(&artifact.path().join("data"), max_files, result)?;
                if result.removed_files >= max_files {
                    result.more = true;
                    return Ok(());
                }
                remove(&artifact.path().join("metadata.writing"), max_files, result)?;
                fsync_dir(&artifact.path())?;
            }
        }
        fsync_dir(&entry.path())?;
        if result.removed_files >= max_files {
            result.more = true;
            return Ok(());
        }
    }
    Ok(())
}
/// The caller must identify an obsolete transition's private inspection root.
/// Lock/identity markers remain so another opener cannot acquire a replacement
/// lock inode while old owned work still holds the original one. Busy work is
/// skipped and reported for retry. Each call unlinks at most `max_files` files.
pub fn cleanup_recovery_cache(root: &Path, max_files: usize) -> io::Result<RecoveryCacheCleanup> {
    if max_files == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cache cleanup requires a positive work bound",
        ));
    }
    let mut result = RecoveryCacheCleanup::default();
    journals(root, max_files, &mut result)?;
    if result.removed_files < max_files {
        phases(&root.join("phases"), max_files, &mut result)?;
    }
    Ok(result)
}

/// Lock file of a cache namespace. Its presence also marks the namespace as
/// the top of its storage tree for directory syncs.
pub(crate) const NAMESPACE_LOCK: &str = ".namespace.lock";

/// All resumable inspection work holds shared namespace ownership. Retirement
/// takes exclusive ownership and leaves a durable marker to reject stale work.
pub(crate) struct RecoveryCacheLease {
    _lock: FileLock,
}
impl RecoveryCacheLease {
    pub(crate) fn acquire(root: &Path) -> io::Result<Self> {
        crate::dir_sync::create_dir_all_synced(root)?;
        let lock = FileLock::try_shared_or_create(&root.join(NAMESPACE_LOCK))?;
        if root.join("retired").try_exists()? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "inspection transition cache has retired",
            ));
        }
        Ok(Self { _lock: lock })
    }
}
/// Retire one obsolete transition and reclaim at most `max_files` files. Shared
/// namespace ownership prevents unlinking live lock inodes. The two namespace
/// markers remain so a stale worker cannot recreate a retired transition cache.
pub fn retire_recovery_cache(root: &Path, max_files: usize) -> io::Result<RecoveryCacheCleanup> {
    use std::io::Write;
    if max_files == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cache retirement requires a positive work bound",
        ));
    }
    if !root.try_exists()? {
        return Ok(Default::default());
    }
    let _lock = match FileLock::try_exclusive_or_create(&root.join(NAMESPACE_LOCK)) {
        Ok(lock) => lock,
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
            return Ok(RecoveryCacheCleanup {
                busy_entries: 1,
                more: true,
                ..Default::default()
            });
        }
        Err(e) => return Err(e),
    };
    if !root.join("retired").try_exists()? {
        let mut marker = File::create(root.join("retired"))?;
        marker.write_all(b"retired-v1\n")?;
        marker.sync_all()?;
        fsync_dir(root)?;
    }
    File::open(root.join("retired"))?.sync_all()?;
    fsync_dir(root)?;
    fn tree(path: &Path, max: usize, result: &mut RecoveryCacheCleanup) -> io::Result<()> {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            if result.removed_files >= max {
                result.more = true;
                fsync_dir(path)?;
                return Ok(());
            }
            if entry.file_type()?.is_dir() {
                tree(&entry.path(), max, result)?;
                match fs::remove_dir(entry.path()) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::DirectoryNotEmpty => {}
                    Err(e) => return Err(e),
                }
            } else {
                let size = entry.metadata()?.len();
                fs::remove_file(entry.path())?;
                result.removed_files += 1;
                result.removed_bytes = result.removed_bytes.saturating_add(size);
            }
        }
        fsync_dir(path)?;
        Ok(())
    }
    let mut result = RecoveryCacheCleanup::default();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir()
            && (hashed_name(&entry.path()) || entry.file_name() == "phases")
        {
            tree(&entry.path(), max_files, &mut result)?;
            match fs::remove_dir(entry.path()) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::DirectoryNotEmpty => {}
                Err(e) => return Err(e),
            }
        }
        if result.removed_files >= max_files {
            result.more = true;
            break;
        }
    }
    if !result.more {
        let marker = File::create(root.join("reclaimed"))?;
        marker.sync_all()?;
    }
    fsync_dir(root)?;
    Ok(result)
}

#[derive(Default)]
pub struct RecoveryCacheSweep {
    entries: Option<fs::ReadDir>,
    current: Option<std::path::PathBuf>,
}
#[derive(Debug, Default)]
pub struct RecoveryCacheSweepStep {
    pub cleanup: RecoveryCacheCleanup,
    pub cycle_complete: bool,
}
impl RecoveryCacheSweep {
    /// Walk a node's transition namespaces incrementally. After discovering each
    /// existing namespace, recheck its activity against current metadata. The
    /// transition must have been committed before this namespace was created,
    /// and retired transition IDs must never be reused. A captured active set
    /// can become stale while this background sweep runs. Metadata errors must
    /// propagate rather than treating an unknown transition as retired.
    pub fn step(
        &mut self,
        root: &Path,
        mut is_active: impl FnMut(&str) -> io::Result<bool>,
        max_entries: usize,
        max_files: usize,
    ) -> io::Result<RecoveryCacheSweepStep> {
        if max_entries == 0 || max_files == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cache sweep requires positive work bounds",
            ));
        }
        if self.entries.is_none() {
            self.entries = match fs::read_dir(root) {
                Ok(entries) => Some(entries),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    return Ok(RecoveryCacheSweepStep {
                        cycle_complete: true,
                        ..Default::default()
                    });
                }
                Err(e) => return Err(e),
            };
        }
        for _ in 0..max_entries {
            let path = if let Some(path) = self.current.take() {
                path
            } else {
                match self.entries.as_mut().unwrap().next() {
                    Some(entry) => {
                        let entry = entry?;
                        if !entry.file_type()?.is_dir() {
                            continue;
                        }
                        entry.path()
                    }
                    None => {
                        self.entries = None;
                        return Ok(RecoveryCacheSweepStep {
                            cycle_complete: true,
                            ..Default::default()
                        });
                    }
                }
            };
            if !hashed_name(&path)
                || !path.join(NAMESPACE_LOCK).is_file()
                || path.join("reclaimed").try_exists()?
                || is_active(path.file_name().unwrap().to_str().unwrap())?
            {
                continue;
            }
            let cleanup = retire_recovery_cache(&path, max_files)?;
            if cleanup.more {
                self.current = Some(path);
            }
            return Ok(RecoveryCacheSweepStep {
                cleanup,
                cycle_complete: false,
            });
        }
        Ok(Default::default())
    }
}
