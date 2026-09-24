//! Mutation barrier for immutable segments shared by recovery generations.
//! Call only under the log's exclusive lock, before opening a writable handle.

use std::{
    fs::{self, File, OpenOptions},
    io,
    path::Path,
};

pub(crate) fn make_private(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        let metadata = match fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        if metadata.nlink() <= 1 {
            return Ok(());
        }
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::other("missing segment parent"))?;
        let temporary = parent.join(format!(".private-{:016x}", fastrand::u64(..)));
        let mut destination = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)?;
        let result = (|| {
            io::copy(&mut File::open(path)?, &mut destination)?;
            fs::set_permissions(&temporary, metadata.permissions())?;
            destination.sync_all()?;
            boundary("private-copied");
            fs::rename(&temporary, path)?;
            boundary("private-renamed");
            crate::util::fsync_dir(parent)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }
    #[cfg(not(unix))]
    {
        // Forking uses independent copies on platforms without this barrier.
        let _ = path;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FrozenForkStats {
    pub shared_bytes: u64,
    pub copied_bytes: u64,
    pub shared_segments: u64,
}

// Unit-test child processes are killed at exact durable-storage boundaries.
// Environment variables have no effect in production builds.
pub(crate) fn boundary(_name: &str) {
    #[cfg(test)]
    if std::env::var("KERATIN_FORK_CRASH").as_deref() == Ok(_name) {
        std::fs::write(std::env::var_os("KERATIN_FORK_READY").unwrap(), _name).unwrap();
        loop {
            std::thread::park_timeout(std::time::Duration::from_secs(1));
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use crate::*;
    use std::{
        process::{Command, Stdio},
        time::{Duration, Instant},
    };

    #[tokio::test]
    async fn crash_child() {
        let Some(root) = std::env::var_os("KERATIN_FORK_ROOT") else {
            return;
        };
        let root = std::path::PathBuf::from(root);
        let log = Keratin::open(root.join("source"), KeratinConfig::test_default())
            .await
            .unwrap();
        let message = || Message {
            payload: vec![42; 128],
            flags: 0,
            headers: vec![],
        };
        log.append_batch(vec![message(), message()], Some(KDurability::AfterFsync))
            .await
            .unwrap();
        log.freeze();
        log.fork_frozen(root.join("target"), 0, 0, 2).await.unwrap();
        let fork =
            Keratin::open_preserving_history(root.join("target"), KeratinConfig::test_default())
                .await
                .unwrap();
        fork.become_follower();
        fork.repair_suffix_at_epoch(1, 0).await.unwrap();
        panic!("child missed interruption boundary");
    }

    #[tokio::test]
    async fn abrupt_fork_and_private_copy_interruptions_preserve_source() {
        for point in [
            "source-sealed",
            "linked",
            "target-tail",
            "target-manifest",
            "private-copied",
            "private-renamed",
        ] {
            let dir = test_dir!("fork_crash");
            let ready = dir.root.join("ready");
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "shared_segment::tests::crash_child",
                    "--nocapture",
                ])
                .env("KERATIN_FORK_ROOT", &dir.root)
                .env("KERATIN_FORK_CRASH", point)
                .env("KERATIN_FORK_READY", &ready)
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(15);
            while !ready.exists() && Instant::now() < deadline {
                if child.try_wait().unwrap().is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let reached = ready.exists();
            let _ = child.kill();
            child.wait().unwrap();
            assert!(reached, "child failed before {point}");
            let source = Keratin::open_preserving_history(
                dir.root.join("source"),
                KeratinConfig::test_default(),
            )
            .await
            .unwrap();
            assert_eq!(source.next_offset(), 2, "{point}");
            assert_eq!(source.reader().scan_from_disk(0, 10).unwrap().len(), 2);
            source.shutdown().await.unwrap();
            if point == "target-manifest" || point.starts_with("private-") {
                if point.starts_with("private-") {
                    assert!(
                        Keratin::open_preserving_history(
                            dir.root.join("target"),
                            KeratinConfig::test_default()
                        )
                        .await
                        .is_err()
                    );
                }
                // The explicit repair journal authorizes ordinary repair; strict
                // history open correctly refuses it until repair completes.
                let fork = Keratin::open(dir.root.join("target"), KeratinConfig::test_default())
                    .await
                    .unwrap();
                assert_eq!(
                    fork.next_offset(),
                    if point.starts_with("private-") { 1 } else { 2 },
                    "{point}"
                );
                fork.shutdown().await.unwrap();
            }
        }
    }
}
