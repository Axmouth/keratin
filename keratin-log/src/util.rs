#[cfg(windows)]
use std::io;
use std::{
    path::{Path, PathBuf}, thread::sleep, time::{Duration, SystemTime, UNIX_EPOCH}
};

use anyhow::Context;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Milliseconds since UNIX epoch
pub type UnixMillis = u64;

pub fn unix_millis() -> UnixMillis {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis().min(u64::MAX as u128) as u64,
        Err(_) => 0, // clock went backwards; clamp
    }
}

pub fn test_dir(prefix: &str) -> TempDir {
    let root: PathBuf = format!("test_data/{prefix}-{}", fastrand::u64(..)).into();
    // let p = std::env::temp_dir()
    //     .join(format!("{}-{}", prefix, fastrand::u64(..)));
    println!("Temp path: {}", root.display());
    std::fs::create_dir_all(&root).unwrap();
    TempDir { root }
}

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

#[cfg(unix)]
pub(crate) fn fsync_dir(path: &Path) -> io::Result<()> {
    use std::fs::File;
    let dir = File::open(path)?;
    dir.sync_all()
}

#[cfg(windows)]
pub(crate) fn fsync_dir(_path: &Path) -> io::Result<()> {
    // Windows rename() is already metadata-durable.
    Ok(())
}

pub fn latest_segment(root: impl AsRef<Path>) -> anyhow::Result<PathBuf> {
    let seg_dir = root.as_ref().join("segments");

    let mut bases: Vec<u64> = std::fs::read_dir(&seg_dir)
        .unwrap()
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
    let base = bases.last().context("no segments exist")?;

    Ok(seg_dir.join(format!("{:020}.log", base)))
}

pub fn all_segments(root: impl AsRef<Path>) -> Vec<PathBuf> {
    let seg_dir = root.as_ref().join("segments");

    let mut bases: Vec<u64> = std::fs::read_dir(&seg_dir)
        .unwrap()
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
    bases
        .into_iter()
        .map(|b| seg_dir.join(format!("{:020}.log", b)))
        .collect()
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
