//! Advisory file locks owned by the process.
//!
//! On Unix these are POSIX record locks (fcntl F_SETLK). A record lock belongs
//! to the process that took it. Child processes started with spawn or fork
//! never inherit it, so a released lock is free for every other process at
//! once. flock style locks belong to the open file description instead, and a
//! child shares that until it execs, which can make a released lock look held.
//!
//! Record locks have two traps, both handled by the registry below:
//! - They never conflict within one process, so the registry refuses a second
//!   holder in this process itself.
//! - Closing any fd for a file drops the process's lock on it, so the registry
//!   keeps the only fd for each locked file and never hands it out. Lock files
//!   must only be opened through this module.
//!
//! Elsewhere std `File::try_lock` is used. Windows handles are not inherited by
//! child processes unless asked for, and the registry keeps the in-process
//! rules the same on every platform.

use parking_lot::Mutex;
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io,
    path::{Path, PathBuf},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LockMode {
    Shared,
    Exclusive,
}

// Identity of a lock file. Paths can differ for the same file, so Unix uses
// (device, inode). Windows locks are per handle anyway, so a canonical path is
// enough there to keep the in-process rules.
#[cfg(unix)]
type Key = (u64, u64);
#[cfg(not(unix))]
type Key = PathBuf;

struct Entry {
    // The only fd this process keeps for the file while it is locked.
    _file: File,
    mode: LockMode,
    holders: usize,
    // Fds opened by an acquire that raced with a rename onto a locked file.
    // Closing them would drop the lock, so they live as long as the entry.
    spare: Vec<File>,
}

// Every lock this process holds. Acquire and release both run under this
// mutex, including the close that drops the OS lock, so a new acquire can never
// slip between the registry update and the close.
static REGISTRY: Mutex<BTreeMap<Key, Entry>> = Mutex::new(BTreeMap::new());

/// A held lock on one file. Dropping it, or calling `release`, unlocks.
#[derive(Debug)]
pub struct FileLock {
    path: PathBuf,
    held: Mutex<Option<Key>>,
}

impl FileLock {
    /// Lock an existing file exclusively without waiting.
    pub fn try_exclusive(path: &Path) -> io::Result<Self> {
        acquire(path, LockMode::Exclusive, false)
    }

    /// Lock a file exclusively without waiting, creating it if missing.
    pub fn try_exclusive_or_create(path: &Path) -> io::Result<Self> {
        acquire(path, LockMode::Exclusive, true)
    }

    /// Take a shared lock without waiting, creating the file if missing.
    pub fn try_shared_or_create(path: &Path) -> io::Result<Self> {
        acquire(path, LockMode::Shared, true)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Unlock now. Later calls and the eventual drop do nothing.
    pub fn release(&self) {
        let Some(key) = self.held.lock().take() else {
            return;
        };
        let mut registry = REGISTRY.lock();
        let Some(entry) = registry.get_mut(&key) else {
            return;
        };
        entry.holders -= 1;
        if entry.holders == 0 {
            // Closing the fds here, with the registry still locked, is what
            // drops the OS lock.
            drop(registry.remove(&key));
        }
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        self.release();
    }
}

/// Create a lock file if it does not exist yet, without touching any lock.
/// Only ever creates a new file, so it never opens and closes a locked one.
pub fn create_lock_file(path: &Path) -> io::Result<()> {
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e),
    }
}

/// Lock file of one Keratin log directory. Kept private to this crate so other
/// code takes the lock through `lock_existing_log` or `create_log_lock_file`.
pub(crate) fn log_lock_path(root: &Path) -> PathBuf {
    root.join(".keratin.lock")
}

fn busy(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        format!("{} is already locked", path.display()),
    )
}

fn acquire(path: &Path, mode: LockMode, create: bool) -> io::Result<FileLock> {
    let mut registry = REGISTRY.lock();
    // Look the file up before opening it: opening and closing another fd for a
    // file this process has locked would drop that lock.
    let before = match key_of_path(path) {
        Ok(key) => Some(key),
        Err(e) if create && e.kind() == io::ErrorKind::NotFound => None,
        Err(e) => return Err(e),
    };
    if let Some(key) = before
        && let Some(entry) = registry.get_mut(&key)
    {
        return join(entry, key, mode, path);
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(create)
        .truncate(false)
        .open(path)?;
    let key = key_of_file(&file, path)?;
    if let Some(entry) = registry.get_mut(&key) {
        entry.spare.push(file);
        return join(entry, key, mode, path);
    }
    os_try_lock(&file, mode, path)?;
    registry.insert(
        key.clone(),
        Entry {
            _file: file,
            mode,
            holders: 1,
            spare: Vec::new(),
        },
    );
    Ok(FileLock {
        path: path.to_path_buf(),
        held: Mutex::new(Some(key)),
    })
}

fn join(entry: &mut Entry, key: Key, mode: LockMode, path: &Path) -> io::Result<FileLock> {
    if mode == LockMode::Shared && entry.mode == LockMode::Shared {
        entry.holders += 1;
        Ok(FileLock {
            path: path.to_path_buf(),
            held: Mutex::new(Some(key)),
        })
    } else {
        Err(busy(path))
    }
}

#[cfg(unix)]
fn key_of_path(path: &Path) -> io::Result<Key> {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::metadata(path)?;
    Ok((meta.dev(), meta.ino()))
}

#[cfg(unix)]
fn key_of_file(file: &File, _path: &Path) -> io::Result<Key> {
    use std::os::unix::fs::MetadataExt;
    let meta = file.metadata()?;
    Ok((meta.dev(), meta.ino()))
}

#[cfg(unix)]
fn os_try_lock(file: &File, mode: LockMode, path: &Path) -> io::Result<()> {
    use nix::{
        errno::Errno,
        fcntl::{FcntlArg, fcntl},
    };
    use std::os::fd::AsRawFd;
    let kind = match mode {
        LockMode::Shared => libc::F_RDLCK,
        LockMode::Exclusive => libc::F_WRLCK,
    };
    // A zero length covers the whole file, however large it grows.
    let request = libc::flock {
        l_type: kind as libc::c_short,
        l_whence: libc::SEEK_SET as libc::c_short,
        l_start: 0,
        l_len: 0,
        l_pid: 0,
    };
    match fcntl(file.as_raw_fd(), FcntlArg::F_SETLK(&request)) {
        Ok(_) => Ok(()),
        // POSIX allows either errno for a lock held by another process.
        Err(Errno::EAGAIN | Errno::EACCES) => Err(busy(path)),
        Err(e) => Err(e.into()),
    }
}

#[cfg(not(unix))]
fn key_of_path(path: &Path) -> io::Result<Key> {
    std::fs::canonicalize(path)
}

#[cfg(not(unix))]
fn key_of_file(_file: &File, path: &Path) -> io::Result<Key> {
    std::fs::canonicalize(path)
}

#[cfg(not(unix))]
fn os_try_lock(file: &File, mode: LockMode, path: &Path) -> io::Result<()> {
    let result = match mode {
        LockMode::Shared => file.try_lock_shared(),
        LockMode::Exclusive => file.try_lock(),
    };
    match result {
        Ok(()) => Ok(()),
        Err(std::fs::TryLockError::WouldBlock) => Err(busy(path)),
        Err(std::fs::TryLockError::Error(e)) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn assert_busy(result: io::Result<FileLock>) {
        let err = result.expect_err("lock should be refused");
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        assert!(err.to_string().contains("already locked"), "{err}");
    }

    #[test]
    fn second_lock_in_the_same_process_is_refused() {
        let dir = crate::test_dir!("lock_double");
        let path = dir.root.join("lock");
        let held = FileLock::try_exclusive_or_create(&path).unwrap();
        assert_busy(FileLock::try_exclusive(&path));
        assert_busy(FileLock::try_exclusive_or_create(&path));
        assert_busy(FileLock::try_shared_or_create(&path));
        drop(held);

        let first = FileLock::try_shared_or_create(&path).unwrap();
        let second = FileLock::try_shared_or_create(&path).unwrap();
        assert_busy(FileLock::try_exclusive(&path));
        drop(first);
        assert_busy(FileLock::try_exclusive(&path));
        drop(second);
        FileLock::try_exclusive(&path).unwrap();
    }

    #[test]
    fn lock_is_free_again_after_drop_or_release() {
        let dir = crate::test_dir!("lock_release");
        let path = dir.root.join("lock");
        for _ in 0..3 {
            drop(FileLock::try_exclusive_or_create(&path).unwrap());
        }
        let held = FileLock::try_exclusive(&path).unwrap();
        held.release();
        held.release();
        let again = FileLock::try_exclusive(&path).unwrap();
        // A released handle must not unlock someone else's lock when dropped.
        drop(held);
        assert_busy(FileLock::try_exclusive(&path));
        drop(again);
    }

    #[test]
    fn missing_file_is_not_created_by_plain_exclusive() {
        let dir = crate::test_dir!("lock_missing");
        let path = dir.root.join("lock");
        let err = FileLock::try_exclusive(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert!(!path.exists());
    }

    // Child half of the cross-process tests. Does nothing unless a parent test
    // passes the lock path and the expected outcome through the environment.
    #[test]
    fn child_lock_probe() {
        let Ok(path) = std::env::var("KERATIN_LOCK_PROBE") else {
            return;
        };
        let result = FileLock::try_exclusive(Path::new(&path));
        match std::env::var("KERATIN_LOCK_EXPECT").as_deref() {
            Ok("busy") => assert_busy(result),
            Ok("free") => drop(result.unwrap()),
            other => panic!("unexpected probe expectation {other:?}"),
        }
    }

    #[cfg(unix)]
    fn other_process_sees(path: &Path, expect: &str) {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "lock::tests::child_lock_probe",
                "--test-threads=1",
            ])
            .env("KERATIN_LOCK_PROBE", path)
            .env("KERATIN_LOCK_EXPECT", expect)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        assert!(
            status.success(),
            "other process did not see the lock as {expect}"
        );
    }

    #[cfg(target_os = "linux")]
    fn open_fds_for(path: &Path) -> usize {
        let target = std::fs::canonicalize(path).unwrap();
        std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
            .filter(|link| *link == target)
            .count()
    }

    #[cfg(unix)]
    #[test]
    fn lock_is_visible_to_other_processes_until_released() {
        let dir = crate::test_dir!("lock_cross_process");
        let path = dir.root.join("lock");
        let held = FileLock::try_exclusive_or_create(&path).unwrap();
        other_process_sees(&path, "busy");
        drop(held);
        other_process_sees(&path, "free");
    }

    // Refused acquires, shared joins and lock file creation must not open and
    // close a second fd, since that would silently drop the process's lock.
    #[cfg(unix)]
    #[test]
    fn refused_or_joined_acquire_keeps_one_fd_and_the_lock() {
        let dir = crate::test_dir!("lock_one_fd");
        let path = dir.root.join("lock");
        let held = FileLock::try_exclusive_or_create(&path).unwrap();
        assert_busy(FileLock::try_exclusive(&path));
        assert_busy(FileLock::try_shared_or_create(&path));
        create_lock_file(&path).unwrap();
        #[cfg(target_os = "linux")]
        assert_eq!(open_fds_for(&path), 1);
        other_process_sees(&path, "busy");
        drop(held);
        #[cfg(target_os = "linux")]
        assert_eq!(open_fds_for(&path), 0);

        let first = FileLock::try_shared_or_create(&path).unwrap();
        let second = FileLock::try_shared_or_create(&path).unwrap();
        #[cfg(target_os = "linux")]
        assert_eq!(open_fds_for(&path), 1);
        drop(first);
        other_process_sees(&path, "busy");
        drop(second);
        other_process_sees(&path, "free");
    }

    // Children spawned while a lock is held must not keep it alive after the
    // release, so a lock released by one thread is always free for the next.
    #[cfg(unix)]
    #[test]
    fn spawning_children_never_makes_a_released_lock_look_held() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        let dir = crate::test_dir!("lock_spawn_storm");
        let paths: Vec<_> = (0..4).map(|i| dir.root.join(format!("lock{i}"))).collect();
        for path in &paths {
            create_lock_file(path).unwrap();
        }
        let stop = Arc::new(AtomicBool::new(false));
        let spawners: Vec<_> = (0..2)
            .map(|_| {
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let mut spawned = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        std::process::Command::new("/bin/true").status().unwrap();
                        spawned += 1;
                    }
                    spawned
                })
            })
            .collect();
        let deadline = Instant::now() + Duration::from_secs(2);
        let lockers: Vec<_> = paths
            .into_iter()
            .map(|path| {
                std::thread::spawn(move || {
                    let mut rounds = 0u64;
                    while rounds < 200_000 && Instant::now() < deadline {
                        let held = FileLock::try_exclusive(&path).unwrap_or_else(|e| {
                            panic!("spurious refusal after {rounds} rounds: {e}")
                        });
                        drop(held);
                        rounds += 1;
                    }
                    rounds
                })
            })
            .collect();
        let rounds: u64 = lockers.into_iter().map(|t| t.join().unwrap()).sum();
        stop.store(true, Ordering::Relaxed);
        let spawned: u64 = spawners.into_iter().map(|t| t.join().unwrap()).sum();
        assert!(
            rounds > 0 && spawned > 0,
            "rounds={rounds} spawned={spawned}"
        );
    }
}
