use super::*;
use keratin_log::test_dir;

fn identity(bytes: &[u8]) -> RecoverySnapshotIdentity {
    RecoverySnapshotIdentity {
        plan: [1; 32],
        digest: *blake3::hash(bytes).as_bytes(),
        bytes: bytes.len() as u64,
    }
}
fn open(root: &Path, bytes: &[u8]) -> RecoverySnapshotFile {
    RecoverySnapshotFile::open(root, identity(bytes), Default::default()).unwrap()
}
#[test]
fn snapshot_transfer_crosses_wire_limit_and_resumes_exactly() {
    let root = test_dir!("large");
    let bytes: Vec<_> = (0..17 * 1024 * 1024 + 13)
        .map(|i| (i % 251) as u8)
        .collect();
    let mut file = open(&root.root, &bytes);
    file.append(0, &bytes[..SNAPSHOT_CHUNK_BYTES]).unwrap();
    assert!(file.verify_complete().is_err());
    assert!(file.read_chunk(0, 1).is_err());
    drop(file);
    let mut file = open(&root.root, &bytes);
    assert_eq!(file.next_offset(), SNAPSHOT_CHUNK_BYTES as u64);
    assert_eq!(
        file.append(0, &bytes[..SNAPSHOT_CHUNK_BYTES]).unwrap(),
        SNAPSHOT_CHUNK_BYTES as u64
    );
    assert!(file.append(0, b"wrong").is_err());
    for chunk in bytes[SNAPSHOT_CHUNK_BYTES..].chunks(SNAPSHOT_CHUNK_BYTES) {
        file.append(file.next_offset(), chunk).unwrap();
    }
    file.verify_complete().unwrap();
    assert_eq!(file.read_complete().unwrap(), bytes);
    assert_eq!(
        file.read_chunk(bytes.len() as u64 - 13, 99).unwrap(),
        bytes[bytes.len() - 13..]
    );
    assert!(file.read_chunk(0, SNAPSHOT_CHUNK_BYTES + 1).is_err());
    drop(file);
    assert_eq!(open(&root.root, &bytes).read_complete().unwrap(), bytes);
}
#[test]
fn snapshot_rejects_gaps_corruption_and_preserves_durable_prefix() {
    let root = test_dir!("corruption");
    let bytes = vec![3; 100];
    let mut file = open(&root.root, &bytes);
    assert!(file.append(1, &[3]).is_err());
    file.append(0, &bytes[..50]).unwrap();
    assert!(file.append(49, &bytes[..2]).is_err());
    let path = file.root.clone();
    assert!(RecoverySnapshotFile::open(&root.root, identity(&bytes), Default::default()).is_err());
    drop(file);
    OpenOptions::new()
        .append(true)
        .open(path.join("data"))
        .unwrap()
        .write_all(b"unpublished tail")
        .unwrap();
    let mut file = open(&root.root, &bytes);
    assert_eq!(file.data.metadata().unwrap().len(), 50);
    file.append(50, &bytes[50..]).unwrap();
    drop(file);
    let mut corrupt = File::options().write(true).open(path.join("data")).unwrap();
    corrupt.write_all(&[4]).unwrap();
    corrupt.sync_all().unwrap();
    assert!(open(&root.root, &bytes).verify_complete().is_err());
    corrupt.set_len(50).unwrap();
    assert!(RecoverySnapshotFile::open(&root.root, identity(&bytes), Default::default()).is_err());
}
#[test]
fn snapshot_disk_admission_and_identity_are_independent_of_progress() {
    let root = test_dir!("limits");
    let bytes = vec![7; 20];
    let mut file = open(&root.root, &bytes);
    file.append(0, &bytes[..10]).unwrap();
    drop(file);
    let mut file = RecoverySnapshotFile::open(
        &root.root,
        identity(&bytes),
        RecoverySnapshotLimits {
            reserve_free_bytes: u64::MAX,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        file.append(10, &bytes[10..]).unwrap_err().kind(),
        io::ErrorKind::StorageFull
    );
    assert_eq!(file.next_offset(), 10);
    drop(file);
    let mut wrong = identity(&bytes);
    wrong.plan = [2; 32];
    assert_eq!(
        RecoverySnapshotFile::open(&root.root, wrong, Default::default())
            .unwrap()
            .next_offset(),
        0
    );
    assert!(
        RecoverySnapshotFile::open(
            &root.root,
            identity(&bytes),
            RecoverySnapshotLimits {
                max_bytes: 19,
                ..Default::default()
            }
        )
        .is_err()
    );
    let mut file = open(&root.root, &bytes);
    file.append(10, &bytes[10..]).unwrap();
    assert_eq!(file.read_complete().unwrap(), bytes);
}
#[test]
fn snapshot_process_crash_keeps_only_published_progress() {
    use std::{
        process::Command,
        time::{Duration, Instant},
    };
    if let Ok(root) = std::env::var("STROMA_TEST_SNAPSHOT_CHILD_ROOT") {
        let mut file = open(Path::new(&root), &[5; 100]);
        file.append(50, &[5; 50]).unwrap();
        panic!("crash gate did not stop child");
    }
    for phase in ["data_synced", "progress_synced"] {
        let root = test_dir!(phase);
        let mut file = open(&root.root, &[5; 100]);
        file.append(0, &[5; 50]).unwrap();
        drop(file);
        let marker = &root.root.join("crashed");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "recovery_snapshot::tests::snapshot_process_crash_keeps_only_published_progress",
                "--nocapture",
            ])
            .env("STROMA_TEST_SNAPSHOT_CHILD_ROOT", &root.root)
            .env("STROMA_TEST_SNAPSHOT_CRASH_PHASE", phase)
            .env("STROMA_TEST_SNAPSHOT_CRASH_MARKER", &marker)
            .spawn()
            .unwrap();
        let start = Instant::now();
        while !marker.exists() && start.elapsed() < Duration::from_secs(10) {
            assert!(child.try_wait().unwrap().is_none());
            std::thread::sleep(Duration::from_millis(10));
        }
        let hit = marker.exists();
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(hit);
        let mut file = open(&root.root, &[5; 100]);
        assert_eq!(
            file.next_offset(),
            if phase == "data_synced" { 50 } else { 100 }
        );
        file.append(50, &[5; 50]).unwrap();
        assert_eq!(file.read_complete().unwrap(), vec![5; 100]);
    }
}
