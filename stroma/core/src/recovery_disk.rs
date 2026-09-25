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
            let routes = || {
                let keys = st
                    .recovery_routes
                    .iter()
                    .map(|r| (r.topic.clone(), r.partition, r.group.clone()))
                    .collect::<Vec<_>>();
                keys.into_iter()
                    .map(|(topic, part, group)| {
                        let root = st.partition_root(&topic, part, group.as_deref());
                        (topic, part, group, root)
                    })
                    .collect::<BTreeSet<_>>()
            };
            let before = routes();
            let mut active = HashSet::new();
            // Only routed partitions need explicit overrides. Ordinary log trees
            // remain active without discovering or loading every queue.
            let mut replaced = HashSet::new();
            for (topic, part, group, _) in &before {
                active.insert(st.msg_tp_part_dir(topic, *part, group.as_deref()));
                active.insert(st.tp_part_dir(topic, *part, group.as_deref()));
                active.insert(st.snap_dir(topic, *part, group.as_deref()));
                for kind in ["messages", "events", "snapshots"] {
                    let mut path = st.root.join(kind);
                    if let Some(group) = group {
                        path.push(Self::enc_component(group));
                    }
                    replaced.insert(
                        path.join(Self::enc_component(topic))
                            .join(format!("{part:010}")),
                    );
                }
            }
            let report = scan(
                &st.root,
                &active,
                &replaced,
                100_000,
                Duration::from_secs(5),
            )?;
            if routes() != before {
                return Err(io_err("storage routes changed during disk sample"));
            }
            Ok(report)
        })
        .await
        .map_err(io_err)?
    }
}

// Lower numbers win when an inode is shared across categories. A retained hard
// link to active data consumes no additional blocks and is attributed to active.
fn category(
    root: &Path,
    path: &Path,
    active: &HashSet<PathBuf>,
    replaced: &HashSet<PathBuf>,
) -> usize {
    if let Some(partition) = path.ancestors().find(|p| active.contains(*p)) {
        return if path
            .strip_prefix(partition)
            .ok()
            .and_then(|p| p.components().next())
            .is_some_and(|p| p.as_os_str() == "agreed-checkpoints")
        {
            1
        } else {
            0
        };
    }
    if path.starts_with(root.join("recovery-installed"))
        || path.ancestors().any(|p| replaced.contains(p))
    {
        return 2;
    }
    if path.starts_with(root.join("recovery-staging")) {
        return 3;
    }
    if ["messages", "events", "snapshots"]
        .iter()
        .any(|name| path.starts_with(root.join(name)))
    {
        let checkpoint = path
            .strip_prefix(root.join("snapshots"))
            .ok()
            .is_some_and(|p| {
                let parts: Vec<_> = p.components().map(|p| p.as_os_str()).collect();
                [1, 2].into_iter().any(|i| {
                    parts.get(i).is_some_and(|part| {
                        let text = part.to_string_lossy();
                        text.len() == 10 && text.bytes().all(|c| c.is_ascii_digit())
                    }) && parts.get(i + 1).is_some_and(|p| *p == "agreed-checkpoints")
                })
            });
        return if checkpoint { 1 } else { 0 };
    }
    4
}

fn scan(
    root: &Path,
    active: &HashSet<PathBuf>,
    replaced: &HashSet<PathBuf>,
    max_entries: usize,
    max_time: Duration,
) -> Result<RecoveryDiskUsage> {
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
            let kind = category(root, &path, active, replaced);
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
        let root = Path::new("/store");
        let active = HashSet::from([root.join("recovery-installed/new/events/q/0000000000")]);
        let replaced = HashSet::from([root.join("events/q/0000000000")]);
        for (path, want) in [
            ("recovery-installed/new/events/q/0000000000/segment", 0),
            ("recovery-installed/old/events/q/0000000000/segment", 2),
            ("events/q/0000000000/segment", 2),
            ("events/another/0000000000/segment", 0),
            ("events/agreed-checkpoints/0000000000/segment", 0),
            ("snapshots/agreed-checkpoints/0000000000/snapshot", 0),
            ("snapshots/another/0000000000/agreed-checkpoints/base", 1),
            ("recovery-staging/plan/messages/segment", 3),
            ("recovery-routes/route/active", 4),
        ] {
            assert_eq!(
                category(root, &root.join(path), &active, &replaced),
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
        fs::create_dir_all(root.join("events/q")).unwrap();
        fs::create_dir_all(root.join("recovery-staging/p")).unwrap();
        let file = root.join("events/q/data");
        fs::write(&file, vec![1u8; 4096]).unwrap();
        fs::hard_link(&file, root.join("recovery-staging/p/shared")).unwrap();
        let sparse = fs::File::create(root.join("recovery-staging/p/sparse")).unwrap();
        sparse.set_len(64 * 1024 * 1024).unwrap();
        symlink(root, root.join("cycle")).unwrap();
        let report = scan(
            root,
            &HashSet::new(),
            &HashSet::new(),
            100,
            Duration::from_secs(5),
        )
        .unwrap();
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
            scan(
                root,
                &HashSet::new(),
                &HashSet::new(),
                1,
                Duration::from_secs(5)
            )
            .is_err()
        );
    }
}
