use std::io;
use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Milliseconds since UNIX epoch
pub type UnixMillis = u64;

pub fn unix_millis() -> UnixMillis {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis().min(u64::MAX as u128) as u64,
        Err(_) => 0, // clock went backwards; clamp
    }
}

#[macro_export]
macro_rules! test_dir {
    ($prefix:expr) => {{
        use std::path::PathBuf;
        // This env! now resolves to the caller's Cargo.toml directory
        let root: PathBuf = format!(
            "{}/test_data/{}-{}",
            env!("CARGO_WORKSPACE_DIR"),
            $prefix,
            fastrand::u64(..)
        )
        .into();

        std::fs::create_dir_all(&root).unwrap();
        // Ensure TempDir is accessible from the library's path
        $crate::util::TempDir { root }
    }};
}

// pub fn test_dir(prefix: &str) -> TempDir {
//     let root: PathBuf = format!("{}/test_data/{prefix}-{}", env!("CARGO_MANIFEST_DIR"), fastrand::u64(..)).into();
//     // let p = std::env::temp_dir()
//     //     .join(format!("{}-{}", prefix, fastrand::u64(..)));
//     println!("Temp path: {}", root.display());
//     std::fs::create_dir_all(&root).unwrap();
//     TempDir { root }
// }

pub struct TempDir {
    pub root: PathBuf,
}

impl Drop for TempDir {
    fn drop(&mut self) {
        println!("Dropping TempDir at {}", self.root.display());
        let res = std::fs::remove_dir_all(&self.root).inspect_err(|err| {
            println!("Error cleaning up temp dir {}: {err}", self.root.display());
        });
        if let Ok(()) = res {
            println!("Cleaned up temp dir: {}", self.root.display());
        }
    }
}

/// Sync a segment or index file's data. Fault-injection builds route every
/// such sync through the test hooks, so any path that syncs log data can be
/// made to fail on purpose.
pub(crate) fn sync_file_data(file: &std::fs::File) -> io::Result<()> {
    #[cfg(feature = "io-fault-injection")]
    return crate::io_fault_injection::sync_data(file);
    #[cfg(not(feature = "io-fault-injection"))]
    file.sync_data()
}

/// Like `sync_file_data`, but also syncs metadata such as timestamps.
pub(crate) fn sync_file_all(file: &std::fs::File) -> io::Result<()> {
    #[cfg(feature = "io-fault-injection")]
    return crate::io_fault_injection::sync_all(file);
    #[cfg(not(feature = "io-fault-injection"))]
    file.sync_all()
}

/// Make the entries of a directory durable, so a file created, renamed or
/// removed inside it survives power loss.
#[cfg(unix)]
pub fn fsync_dir(path: &Path) -> io::Result<()> {
    std::fs::File::open(path)?.sync_all()
}

/// Make the entries of a directory durable, so a file created, renamed or
/// removed inside it survives power loss.
///
/// A directory handle needs FILE_FLAG_BACKUP_SEMANTICS, and FlushFileBuffers,
/// which `sync_all` calls, needs write access to the handle.
#[cfg(windows)]
pub fn fsync_dir(path: &Path) -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?
        .sync_all()
}

#[derive(Debug, thiserror::Error)]
pub enum LatestSegmentError {
    #[error("failed to read segment directory {path}: {source}")]
    ReadDir {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("no segments exist in {path}")]
    NoSegments { path: PathBuf },
}

pub fn latest_segment(root: impl AsRef<Path>) -> Result<PathBuf, LatestSegmentError> {
    let seg_dir = root.as_ref().join("segments");

    let mut bases: Vec<u64> = std::fs::read_dir(&seg_dir)
        .map_err(|source| LatestSegmentError::ReadDir {
            path: seg_dir.clone(),
            source,
        })?
        .filter_map(|e| {
            let e = e.ok()?;
            let name = e.file_name();
            let s = name.to_str()?;
            if let Some(stem) = s.strip_suffix(".log") {
                stem.parse::<u64>().ok()
            } else {
                None
            }
        })
        .collect();

    bases.sort_unstable();
    let base = bases.last().ok_or_else(|| LatestSegmentError::NoSegments {
        path: seg_dir.clone(),
    })?;

    Ok(seg_dir.join(format!("{:020}.log", base)))
}

pub fn all_segments(root: impl AsRef<Path>) -> std::io::Result<Vec<PathBuf>> {
    let seg_dir = root.as_ref().join("segments");

    let mut bases: Vec<u64> = std::fs::read_dir(&seg_dir)?
        .filter_map(|e| {
            let e = e.ok()?;
            let name = e.file_name();
            let s = name.to_str()?;
            if let Some(stem) = s.strip_suffix(".log") {
                stem.parse::<u64>().ok()
            } else {
                None
            }
        })
        .collect();

    bases.sort_unstable();
    Ok(bases
        .into_iter()
        .map(|b| seg_dir.join(format!("{:020}.log", b)))
        .collect())
}

pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_thread_ids(true)
                .with_line_number(true)
                .with_file(true),
        )
        .init();
}
