#![cfg(unix)]
use keratin_log::*;
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt},
};

fn message(value: u8) -> Message {
    Message {
        payload: vec![value; 128],
        flags: 0,
        headers: vec![],
    }
}

#[tokio::test]
async fn fork_shares_only_closed_segments_and_repairs_are_private() {
    let dir = test_dir!("fork_private_repair");
    let source = dir.root.join("source");
    let target = dir.root.join("target");
    let cfg = KeratinConfig::test_default();
    let log = Keratin::open(&source, cfg).await.unwrap();
    log.append_batch(
        (0..10).map(message).collect(),
        Some(KDurability::AfterFsync),
    )
    .await
    .unwrap();
    assert!(log.fork_frozen(target.clone(), 0, 0, 10).await.is_err());
    log.freeze();
    assert!(log.fork_frozen(target.clone(), 1, 0, 10).await.is_err());
    let stats = log.fork_frozen(target.clone(), 0, 0, 10).await.unwrap();
    assert!(stats.shared_bytes > 1280);
    assert_eq!(stats.shared_segments, 1);
    let prefix = "segments/00000000000000000000.log";
    fs::set_permissions(source.join(prefix), fs::Permissions::from_mode(0o600)).unwrap();
    let original = fs::read(source.join(prefix)).unwrap();
    assert_eq!(
        fs::metadata(source.join(prefix)).unwrap().ino(),
        fs::metadata(target.join(prefix)).unwrap().ino()
    );
    assert_eq!(
        u16::from_be_bytes(
            fs::read(source.join("manifest.bin")).unwrap()[8..10]
                .try_into()
                .unwrap()
        ),
        3
    );
    let fork = Keratin::open_preserving_history(&target, cfg)
        .await
        .unwrap();
    fork.become_follower();
    fork.repair_suffix_at_epoch(5, 0).await.unwrap();
    assert_eq!(fs::read(source.join(prefix)).unwrap(), original);
    assert_ne!(
        fs::metadata(source.join(prefix)).unwrap().ino(),
        fs::metadata(target.join(prefix)).unwrap().ino()
    );
    assert_eq!(
        fs::metadata(target.join(prefix))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    fork.become_owner();
    fork.append_batch(vec![message(99)], Some(KDurability::AfterFsync))
        .await
        .unwrap();
    log.become_owner();
    log.append_batch(vec![message(100)], Some(KDurability::AfterFsync))
        .await
        .unwrap();
    assert_eq!(log.reader().scan_from_disk(0, 20).unwrap().len(), 11);
    assert_eq!(fork.reader().scan_from_disk(0, 20).unwrap().len(), 6);
    fork.shutdown().await.unwrap();
    log.shutdown().await.unwrap();
    let fork = Keratin::open_preserving_history(target, cfg).await.unwrap();
    assert_eq!(fork.next_offset(), 6);
    fork.shutdown().await.unwrap();
}

#[tokio::test]
async fn empty_and_nonzero_heads_have_private_tails() {
    let dir = test_dir!("fork_empty_and_trimmed");
    let mut cfg = KeratinConfig::test_default();
    cfg.segment_max_bytes = 512;
    let source = dir.root.join("source");
    let log = Keratin::open(&source, cfg).await.unwrap();
    log.freeze();
    let empty = dir.root.join("empty");
    assert_eq!(
        log.fork_frozen(empty.clone(), 0, 0, 0)
            .await
            .unwrap()
            .shared_segments,
        0
    );
    let fork = Keratin::open(empty, cfg).await.unwrap();
    fork.append_batch(vec![message(50)], Some(KDurability::AfterFsync))
        .await
        .unwrap();
    assert_eq!(log.next_offset(), 0);
    fork.shutdown().await.unwrap();
    log.become_owner();
    for i in 0..20 {
        log.append_batch(vec![message(i)], Some(KDurability::AfterFsync))
            .await
            .unwrap();
    }
    log.truncate_before(10).await.unwrap();
    assert!(log.head_offset() > 0);
    log.freeze();
    let target = dir.root.join("trimmed");
    let stats = log
        .fork_frozen(target.clone(), 0, log.head_offset(), 20)
        .await
        .unwrap();
    assert!(stats.shared_segments > 1);
    assert!(
        log.fork_frozen(target.clone(), 0, log.head_offset(), 20)
            .await
            .is_err()
    );
    let fork = Keratin::open_preserving_history(target, cfg).await.unwrap();
    assert_eq!(fork.head_offset(), log.head_offset());
    assert_eq!(fork.next_offset(), 20);
    log.truncate_before(20).await.unwrap();
    assert!(
        !fork
            .reader()
            .scan_from_disk(fork.head_offset(), 100)
            .unwrap()
            .is_empty()
    );
    fork.shutdown().await.unwrap();
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn unclean_scan_privatizes_a_shared_tail_before_truncating() {
    let dir = test_dir!("fork_scan_private");
    let source = dir.root.join("source");
    let target = dir.root.join("target");
    let cfg = KeratinConfig::test_default();
    let log = Keratin::open(&source, cfg).await.unwrap();
    log.append_batch(vec![message(1), message(2)], Some(KDurability::AfterFsync))
        .await
        .unwrap();
    log.freeze();
    log.fork_frozen(target.clone(), 0, 0, 2).await.unwrap();
    let prefix = "segments/00000000000000000000.log";
    // Simulate padding surviving an unclean close. The scan must repair only
    // its own name even though the inode is still referenced by another log.
    use std::io::Write;
    fs::OpenOptions::new()
        .append(true)
        .open(target.join(prefix))
        .unwrap()
        .write_all(&[0; 37])
        .unwrap();
    let shared_before = fs::read(source.join(prefix)).unwrap();
    let fork = Keratin::open(
        &target,
        KeratinConfig {
            force_recovery_scan: true,
            ..cfg
        },
    )
    .await
    .unwrap();
    assert_eq!(fs::read(source.join(prefix)).unwrap(), shared_before);
    assert_eq!(
        fs::metadata(target.join(prefix)).unwrap().len() + 37,
        shared_before.len() as u64
    );
    fork.shutdown().await.unwrap();
    log.shutdown().await.unwrap();
}

#[tokio::test]
async fn cross_filesystem_fork_copies_and_keeps_source_independent() {
    if !std::path::Path::new("/dev/shm").is_dir() {
        return;
    }
    let dir = test_dir!("fork_cross_filesystem");
    let target_root =
        std::path::PathBuf::from(format!("/dev/shm/keratin-fork-{:016x}", fastrand::u64(..)));
    fs::create_dir(&target_root).unwrap();
    let target_guard = util::TempDir { root: target_root };
    if fs::metadata(&dir.root).unwrap().dev() == fs::metadata(&target_guard.root).unwrap().dev() {
        return;
    }
    let cfg = KeratinConfig::test_default();
    let source = Keratin::open(dir.root.join("source"), cfg).await.unwrap();
    source
        .append_batch(vec![message(1), message(2)], Some(KDurability::AfterFsync))
        .await
        .unwrap();
    source.freeze();
    let target = target_guard.root.join("log");
    let stats = source.fork_frozen(target.clone(), 0, 0, 2).await.unwrap();
    assert_eq!(stats.shared_segments, 0);
    assert_eq!(stats.shared_bytes, 0);
    assert!(stats.copied_bytes >= 256);
    let fork = Keratin::open_preserving_history(target, cfg).await.unwrap();
    fork.become_follower();
    fork.repair_suffix_at_epoch(1, 0).await.unwrap();
    assert_eq!(source.reader().scan_from_disk(0, 10).unwrap().len(), 2);
    fork.shutdown().await.unwrap();
    source.shutdown().await.unwrap();
}
