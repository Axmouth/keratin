//! Approximate, read-only storage accounting. Never opens a queue or deletes data.
use super::*;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RecoveryDiskUsage {
    /// Unix uses allocated file blocks and deduplicates hard links. Other hosts
    /// report file lengths, where separate links can count more than once.
    pub basis: String,
    pub active_bytes: u64,
    pub checkpoint_bytes: u64,
    pub retained_bytes: u64,
    pub staging_bytes: u64,
    pub other_bytes: u64,
    pub files: u64,
    pub skipped_entries: u64,
}

impl Stroma {
    pub async fn recovery_disk_usage(&self) -> Result<RecoveryDiskUsage> {
        // The permit lives inside the blocking closure, including cancellation.
        static SCAN: Semaphore = Semaphore::const_new(1);
        let permit = SCAN.acquire().await.map_err(io_err)?;
        let st = self.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let generations = || {
                st.generations
                    .iter()
                    .map(|g| (g.key().clone(), g.value().name()))
                    .collect::<BTreeSet<_>>()
            };
            let before = generations();
            let report = scan(&st.root, 100_000, Duration::from_secs(5))?;
            if generations() != before {
                return Err(io_err("storage generations changed during disk sample"));
            }
            Ok(report)
        })
        .await
        .map_err(io_err)?
    }
}

// Lower numbers win when an inode is shared across categories. A retained hard
// link to active data consumes no additional blocks and is attributed to active.
//
// `currents` caches each partition's current generation name, read once per
// sample from `partitions/<group>/<topic>/<part>/current`.
fn category(
    root: &Path,
    path: &Path,
    currents: &mut HashMap<PathBuf, Option<String>>,
) -> usize {
    let Ok(relative) = path.strip_prefix(root.join(layout::PARTITIONS)) else {
        return 4;
    };
    let parts: Vec<_> = relative.components().map(|c| c.as_os_str()).collect();
    if parts.len() < 5 {
        return 4;
    }
    let part_dir = root
        .join(layout::PARTITIONS)
        .join(parts[0])
        .join(parts[1])
        .join(parts[2]);
    let entry = parts[3].to_string_lossy();
    if entry == layout::STAGING {
        return 3;
    }
    if layout::Generation::parse(&entry).is_none() {
        return 4;
    }
    let current = currents
        .entry(part_dir.clone())
        .or_insert_with(|| {
            layout::read_current(&part_dir)
                .ok()
                .flatten()
                .map(|g| g.name())
        });
    if current.as_deref() != Some(&*entry) {
        return 2;
    }
    if parts.get(4).is_some_and(|p| *p == layout::STATE)
        && parts.get(5).is_some_and(|p| *p == "agreed-checkpoints")
    {
        1
    } else {
        0
    }
}

fn scan(root: &Path, max_entries: usize, max_time: Duration) -> Result<RecoveryDiskUsage> {
    let start = Instant::now();
    let mut report = RecoveryDiskUsage {
        basis: if cfg!(unix) {
            "allocated_file_blocks"
        } else {
            "file_lengths"
        }
        .into(),
        ..Default::default()
    };
    let mut pending = vec![root.to_owned()];
    let mut visited = 0;
    let mut totals = [0u64; 5];
    let mut currents = HashMap::new();
    #[cfg(unix)]
    let mut inodes: HashMap<(u64, u64), (usize, u64)> = HashMap::new();
    while let Some(dir) = pending.pop() {
        if start.elapsed() > max_time {
            return Err(io_err("disk sample exceeded its traversal time budget"));
        }
        let entries = match fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                report.skipped_entries += 1;
                continue;
            }
            Err(e) => return Err(io_err(e)),
        };
        for entry in entries {
            visited += 1;
            if visited > max_entries || start.elapsed() > max_time {
                return Err(io_err(
                    "disk sample exceeded its 100,000-entry / 5-second budget",
                ));
            }
            let entry = entry.map_err(io_err)?;
            let path = entry.path();
            // Do not follow symlinks into another tree or outside storage.
            let metadata = match fs::symlink_metadata(&path) {
                Ok(meta) => meta,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    report.skipped_entries += 1;
                    continue;
                }
                Err(e) => return Err(io_err(e)),
            };
            if metadata.is_dir() {
                pending.push(path);
                continue;
            }
            if !metadata.is_file() {
                report.skipped_entries += 1;
                continue;
            }
            report.files += 1;
            let kind = category(root, &path, &mut currents);
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                let item = inodes
                    .entry((metadata.dev(), metadata.ino()))
                    .or_insert((kind, 0));
                item.0 = item.0.min(kind);
                item.1 = item.1.max(metadata.blocks().saturating_mul(512));
            }
            #[cfg(not(unix))]
            {
                totals[kind] = totals[kind].saturating_add(metadata.len());
            }
        }
    }
    #[cfg(unix)]
    for (_, (kind, bytes)) in inodes {
        totals[kind] = totals[kind].saturating_add(bytes);
    }
    [
        report.active_bytes,
        report.checkpoint_bytes,
        report.retained_bytes,
        report.staging_bytes,
        report.other_bytes,
    ] = totals;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn categories_distinguish_active_replacement_old_generation_and_checkpoints() {
        let dir = std::env::temp_dir().join(format!("recovery-disk-{}", uuid::Uuid::now_v7()));
        struct Cleanup(PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(dir.clone());
        let root = dir.as_path();
        let new = layout::Generation::new_initial().name();
        let old = layout::Generation::new_initial().name();
        let part = root.join("partitions/@none/q/0000000000");
        fs::create_dir_all(&part).unwrap();
        fs::write(part.join("current"), format!("{new}\n")).unwrap();
        for (path, want) in [
            (format!("partitions/@none/q/0000000000/{new}/log/segment"), 0),
            (format!("partitions/@none/q/0000000000/{old}/log/segment"), 2),
            (format!("partitions/@none/q/0000000000/{new}/state/snapshot"), 0),
            (format!("partitions/@none/q/0000000000/{new}/state/agreed-checkpoints/base"), 1),
            (format!("partitions/@none/q/0000000000/{old}/state/agreed-checkpoints/base"), 2),
            ("partitions/@none/q/0000000000/staging/plan/messages/segment".into(), 3),
            ("partitions/@none/q/0000000000/recovery/started".into(), 4),
            ("partitions/@none/q/0000000000/current".into(), 4),
            (format!("partitions/@none/other/0000000000/{new}/log/segment"), 2),
            ("global/segment".into(), 4),
        ] {
            assert_eq!(
                category(root, &root.join(&path), &mut HashMap::new()),
                want,
                "{path}"
            );
        }
    }
    #[cfg(unix)]
    #[test]
    fn scan_deduplicates_hardlinks_counts_sparse_blocks_and_ignores_symlinks() {
        use std::os::unix::fs::{MetadataExt, symlink};
        let dir = std::env::temp_dir().join(format!("recovery-disk-{}", uuid::Uuid::now_v7()));
        fs::create_dir_all(&dir).unwrap();
        struct Cleanup(PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(dir.clone());
        let root = dir.as_path();
        let generation = layout::Generation::new_initial().name();
        let part = root.join("partitions/@none/q/0000000000");
        fs::create_dir_all(part.join(&generation).join("log")).unwrap();
        fs::create_dir_all(part.join("staging/p")).unwrap();
        fs::write(part.join("current"), format!("{generation}\n")).unwrap();
        let file = part.join(&generation).join("log/data");
        fs::write(&file, vec![1u8; 4096]).unwrap();
        fs::hard_link(&file, part.join("staging/p/shared")).unwrap();
        let sparse = fs::File::create(part.join("staging/p/sparse")).unwrap();
        sparse.set_len(64 * 1024 * 1024).unwrap();
        symlink(root, root.join("cycle")).unwrap();
        let report = scan(root, 100, Duration::from_secs(5)).unwrap();
        assert_eq!(
            report.active_bytes,
            fs::metadata(file).unwrap().blocks() * 512
        );
        assert_eq!(
            report.staging_bytes,
            sparse.metadata().unwrap().blocks() * 512
        );
        assert_eq!(report.skipped_entries, 1);
        assert!(
            scan(root, 1, Duration::from_secs(5)).is_err()
        );
    }
}
