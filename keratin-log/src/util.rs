use std::{path::{Path, PathBuf}, time::{Duration, SystemTime, UNIX_EPOCH}};

use anyhow::Context;

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
        std::fs::remove_dir_all(&self.root).unwrap();
    }
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
    bases.into_iter()
        .map(|b| seg_dir.join(format!("{:020}.log", b)))
        .collect()
}
