//! Integration coverage for the in-memory tail cache: the records it serves
//! must be indistinguishable from a file scan, and it must never surface stale
//! (rewound-away) records after a follower reset. The cache is transparent to
//! `scan_from`, so these drive the real public surface (append / replicated
//! append / reset / read) with the cache both enabled and disabled.

use keratin_log::*;

fn cfg_with_cache(bytes: usize) -> KeratinConfig {
    KeratinConfig {
        tail_cache_bytes: bytes,
        ..KeratinConfig::test_default()
    }
}

fn message(payload: impl Into<Vec<u8>>) -> Message {
    Message {
        payload: payload.into(),
        flags: 0,
        headers: vec![],
    }
}

fn batch(range: std::ops::Range<u64>) -> Vec<Message> {
    range.map(|i| message(format!("msg-{i}"))).collect()
}

/// Read [from, from+max) and return (offset, payload) pairs for easy comparison.
fn read_pairs(k: &Keratin, from: u64, max: usize) -> Vec<(u64, Vec<u8>)> {
    k.reader()
        .scan_from(from, max)
        .unwrap()
        .into_iter()
        .map(|r| (r.offset, r.payload))
        .collect()
}

/// The cache must serve byte-identical records to a file scan. Write the same
/// data to a cache-enabled and a cache-disabled log and compare every read.
#[tokio::test]
async fn cache_read_matches_file_read() {
    let dir_on = test_dir!("tail_cache_parity_on");
    let dir_off = test_dir!("tail_cache_parity_off");

    let k_on = Keratin::open(&dir_on.root, cfg_with_cache(64 * 1024 * 1024))
        .await
        .unwrap();
    let k_off = Keratin::open(&dir_off.root, cfg_with_cache(0))
        .await
        .unwrap();

    // Several separate appends so the cache holds multiple flush batches.
    for start in (0..500).step_by(50) {
        k_on
            .append_batch(batch(start..start + 50), None)
            .await
            .unwrap();
        k_off
            .append_batch(batch(start..start + 50), None)
            .await
            .unwrap();
    }
    k_on.sync().await.unwrap();
    k_off.sync().await.unwrap();

    // Full scan, tail scan, mid-range window, and a single record all agree.
    for (from, max) in [(0usize, 500usize), (450, 50), (123, 40), (499, 1), (0, 1)] {
        let on = read_pairs(&k_on, from as u64, max);
        let off = read_pairs(&k_off, from as u64, max);
        assert_eq!(on, off, "cache vs file mismatch at from={from} max={max}");
    }

    k_on.shutdown().await.unwrap();
    k_off.shutdown().await.unwrap();
}

/// After a follower reset rewinds the log, a read must serve the fresh records
/// written post-reset and never the stale pre-reset ones the cache had held.
#[tokio::test]
async fn read_after_reset_serves_new_not_stale() {
    let dir = test_dir!("tail_cache_reset");
    let k = Keratin::open(&dir.root, cfg_with_cache(64 * 1024 * 1024))
        .await
        .unwrap();
    k.become_follower();

    // Replicate an initial run and read it back (populates the cache).
    let out = k
        .append_replicated_batch(1, 0, batch(0..100), None)
        .await
        .unwrap();
    assert!(matches!(out, ReplicatedAppendOutcome::Applied(_)));
    k.sync().await.unwrap();
    let before = read_pairs(&k, 0, 100);
    assert_eq!(before.len(), 100);
    assert_eq!(before[0].1, b"msg-0");

    // Rewind the whole log back to offset 0, then replicate different content.
    k.destructive_reset_to_checkpoint(0).await.unwrap();

    let fresh: Vec<Message> = (0..100).map(|i| message(format!("fresh-{i}"))).collect();
    let out = k
        .append_replicated_batch(2, 0, fresh, None)
        .await
        .unwrap();
    assert!(matches!(out, ReplicatedAppendOutcome::Applied(_)));
    k.sync().await.unwrap();

    let after = read_pairs(&k, 0, 100);
    assert_eq!(after.len(), 100);
    for (off, payload) in &after {
        assert_eq!(payload, format!("fresh-{off}").as_bytes());
    }

    k.shutdown().await.unwrap();
}

/// Contiguous replicated suffix appends (the follower catch-up path) must read
/// back as one contiguous, correct run spanning several cached flush batches.
#[tokio::test]
async fn replicated_suffix_then_read_is_contiguous() {
    let dir = test_dir!("tail_cache_suffix");
    let k = Keratin::open(&dir.root, cfg_with_cache(64 * 1024 * 1024))
        .await
        .unwrap();
    k.become_follower();

    // Apply the log as a sequence of contiguous suffix batches, syncing between
    // them so each lands as its own cached flush batch.
    let mut next = 0u64;
    for _ in 0..5 {
        let out = k
            .append_replicated_batch(1, next, batch(next..next + 40), None)
            .await
            .unwrap();
        assert!(matches!(out, ReplicatedAppendOutcome::Applied(_)));
        k.sync().await.unwrap();
        next += 40;
    }

    // A single read spanning multiple cached batches is contiguous and correct.
    let all = read_pairs(&k, 0, next as usize);
    assert_eq!(all.len(), next as usize);
    for (i, (off, payload)) in all.iter().enumerate() {
        assert_eq!(*off, i as u64);
        assert_eq!(payload, format!("msg-{i}").as_bytes());
    }

    // A window straddling a batch boundary also reads clean.
    let straddle = read_pairs(&k, 35, 10);
    assert_eq!(straddle.len(), 10);
    assert_eq!(straddle[0].0, 35);
    assert_eq!(straddle[9].0, 44);

    k.shutdown().await.unwrap();
}
