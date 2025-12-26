use std::{path::PathBuf, time::{Duration, SystemTime, UNIX_EPOCH}};

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