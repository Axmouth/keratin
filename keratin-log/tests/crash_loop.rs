//! Kill a writing process at random moments and check every acknowledged
//! record survives. Small segments, truncation and preallocation keep rolls,
//! reclaims, reused segments and zeroing ahead in play at every kill.
//!
//! The kill is SIGKILL, so the page cache survives: this checks recovery logic
//! and on-disk ordering under a process crash, not power loss. Ignored by
//! default. Run with
//! `cargo test -p keratin-log --test crash_loop -- --ignored --nocapture`,
//! and `KERATIN_CRASH_LOOP_ROUNDS` for more rounds.

use keratin_log::*;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn config() -> KeratinConfig {
    KeratinConfig {
        segment_max_bytes: 256 * 1024,
        index_stride_bytes: 4096,
        segment_preallocate_bytes: 128 * 1024,
        ..KeratinConfig::test_default()
    }
}

/// The payload names its offset, so a record read back can be checked
/// against the offset it was read at.
fn payload(offset: u64) -> Vec<u8> {
    let mut bytes = format!("o{offset:012}-").into_bytes();
    bytes.resize(200 + (offset % 97) as usize, b'x');
    bytes
}

struct Rng(u64);
impl Rng {
    fn seeded() -> Self {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        Self(nanos | 1)
    }
    fn next(&mut self, below: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % below
    }
}

fn acked_path(root: &Path) -> PathBuf {
    root.with_extension("acked")
}

/// Child: append until killed, writing the acknowledged frontier after each
/// durable batch. A process kill keeps the page cache, so a plain write of
/// that file is enough.
#[tokio::test]
async fn crash_loop_child() {
    let Some(root) = std::env::var_os("KERATIN_CRASH_LOOP_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let log = Keratin::open(&root, config()).await.unwrap();
    let mut rng = Rng::seeded();
    loop {
        let next = log.next_offset();
        let count = 1 + rng.next(20);
        let messages = (next..next + count)
            .map(|offset| Message {
                payload: payload(offset),
                flags: 0,
                headers: vec![],
            })
            .collect();
        // Most batches wait for a sync, some only for the write, so an
        // unsynced tail is often in flight at the kill.
        let durable = rng.next(5) != 0;
        let durability = if durable {
            KDurability::AfterFsync
        } else {
            KDurability::AfterWrite
        };
        log.append_batch(messages, Some(durability)).await.unwrap();
        if durable {
            let mut file = std::fs::File::create(acked_path(&root)).unwrap();
            write!(file, "{}", next + count).unwrap();
        }
        // Keep about 3000 records so truncation frees segments for reuse.
        if rng.next(50) == 0 && next > 3000 {
            log.truncate_before(next - 3000).await.unwrap();
        }
    }
}

#[tokio::test]
#[ignore]
async fn acknowledged_records_survive_repeated_kills() {
    let rounds: u32 = std::env::var("KERATIN_CRASH_LOOP_ROUNDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let dir = test_dir!("crash_loop");
    let root = dir.root.join("log");
    let mut rng = Rng::seeded();
    let mut checked = 0u64;
    for round in 0..rounds {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_loop_child", "--nocapture"])
            .env("KERATIN_CRASH_LOOP_ROOT", &root)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200 + rng.next(1300))).await;
        assert!(child.try_wait().unwrap().is_none(), "child exited early in round {round}");
        child.kill().unwrap();
        child.wait().unwrap();

        let acked: u64 = std::fs::read_to_string(acked_path(&root))
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0);
        let log = Keratin::open(&root, config()).await.unwrap();
        let head = log.head_offset();
        let next = log.next_offset();
        assert!(next >= acked, "round {round}: next {next} below acknowledged {acked}");
        let mut offset = head;
        while offset < next {
            let records = log.reader().scan_from(offset, 4096).unwrap();
            assert!(!records.is_empty(), "round {round}: nothing readable at {offset} of {head}..{next}");
            for record in records {
                assert_eq!(record.offset, offset, "round {round}: offset gap");
                assert_eq!(record.payload, payload(offset), "round {round}: wrong record at {offset}");
                offset += 1;
            }
        }
        checked += next - head;
        log.shutdown().await.unwrap();
    }
    println!("{rounds} kills, {checked} records checked");
}
