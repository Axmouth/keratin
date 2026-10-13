//! Replicated retries over records that are written but not durable yet. An
//! AfterFsync reply of AlreadyPresent tells the leader the range is durable,
//! so it must never cover records above the durable frontier.
use keratin_log::*;

fn messages(range: std::ops::Range<u8>) -> Vec<Message> {
    range
        .map(|value| Message {
            flags: 0,
            headers: vec![],
            payload: vec![value; 64],
        })
        .collect()
}

async fn follower(dir: &util::TempDir, tail_cache_bytes: usize) -> Keratin {
    let cfg = KeratinConfig {
        tail_cache_bytes,
        // Several cases check that written overlaps stay non-durable.
        background_sync_interval: std::time::Duration::ZERO,
        ..KeratinConfig::test_default()
    };
    let log = Keratin::open(&dir.root, cfg).await.unwrap();
    log.become_follower();
    log
}

/// AlreadyPresent under AfterFsync must imply the whole range is durable.
fn assert_durable_if_present(log: &Keratin, result: &Result<ReplicatedAppendOutcome, IoError>) {
    if let Ok(ReplicatedAppendOutcome::AlreadyPresent {
        first_offset,
        count,
        ..
    }) = result
    {
        assert!(
            log.durable_next() >= first_offset + u64::from(*count),
            "AlreadyPresent {first_offset}+{count} above durable_next {}",
            log.durable_next()
        );
    }
}

fn assert_not_durable_error(result: &Result<ReplicatedAppendOutcome, IoError>) {
    let error = result.as_ref().expect_err("retry over a non-durable range must fail");
    assert!(error.to_string().contains("not durable yet"), "{error}");
}

#[tokio::test]
async fn after_fsync_retry_over_unsynced_range_never_succeeds() {
    for first in [KDurability::AfterWrite, KDurability::AfterFlush] {
        for cache in [0usize, 1 << 20] {
            let dir = test_dir!("overlap_unsynced_retry");
            let log = follower(&dir, cache).await;
            let epoch = log.current_epoch();
            let retry = Some(KDurability::AfterFsync);
            log.append_replicated_batch(epoch, 0, messages(0..3), Some(first))
                .await
                .unwrap();
            assert_eq!(log.durable_next(), 0);

            let exact = log.append_replicated_batch(epoch, 0, messages(0..3), retry).await;
            assert_not_durable_error(&exact);
            let contained = log.append_replicated_batch(epoch, 1, messages(1..2), retry).await;
            assert_not_durable_error(&contained);
            let suffix = log
                .append_replicated_batch_with_mode(
                    epoch,
                    1,
                    messages(1..5),
                    ReplicatedAppendMode::AppendSuffixAfterKnownPrefix,
                    retry,
                )
                .await;
            assert_not_durable_error(&suffix);

            let partial = log.append_replicated_batch(epoch, 1, messages(1..5), retry).await;
            assert!(
                matches!(partial, Ok(ReplicatedAppendOutcome::Overlap { .. })),
                "{first:?} cache={cache}: {partial:?}"
            );
            assert_eq!(log.durable_next(), 0);
            assert_eq!(log.next_offset(), 3);
            log.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn after_fsync_already_present_implies_durable() {
    for cache in [0usize, 1 << 20] {
        let dir = test_dir!("overlap_durable_retry");
        let log = follower(&dir, cache).await;
        let epoch = log.current_epoch();
        let fsync = Some(KDurability::AfterFsync);
        log.append_replicated_batch(epoch, 0, messages(0..3), fsync)
            .await
            .unwrap();

        for (first, batch) in [(0, messages(0..3)), (1, messages(1..2)), (2, messages(2..3))] {
            let result = log.append_replicated_batch(epoch, first, batch, fsync).await;
            assert!(
                matches!(result, Ok(ReplicatedAppendOutcome::AlreadyPresent { .. })),
                "{result:?}"
            );
            assert_durable_if_present(&log, &result);
        }
        let suffix = log
            .append_replicated_batch_with_mode(
                epoch,
                1,
                messages(1..5),
                ReplicatedAppendMode::AppendSuffixAfterKnownPrefix,
                fsync,
            )
            .await;
        assert!(
            matches!(
                suffix,
                Ok(ReplicatedAppendOutcome::AppliedSuffix { skipped_count: 2, .. })
            ),
            "{suffix:?}"
        );
        assert_eq!(log.durable_next(), 5);
        log.shutdown().await.unwrap();
    }
}

// An AfterFlush retry checks the overlap against written records byte for
// byte and answers AlreadyPresent without making them durable. AfterFlush
// never promises durability, so this does not break the AfterFsync rule.
#[tokio::test]
async fn after_flush_retry_accepts_written_overlap_without_advancing_durability() {
    for cache in [0usize, 1 << 20] {
        let dir = test_dir!("overlap_after_flush_retry");
        let log = follower(&dir, cache).await;
        let epoch = log.current_epoch();
        let flush = Some(KDurability::AfterFlush);
        log.append_replicated_batch(epoch, 0, messages(0..3), Some(KDurability::AfterWrite))
            .await
            .unwrap();

        for (first, batch) in [(0, messages(0..3)), (1, messages(1..2))] {
            let result = log.append_replicated_batch(epoch, first, batch, flush).await;
            assert!(
                matches!(result, Ok(ReplicatedAppendOutcome::AlreadyPresent { .. })),
                "{result:?}"
            );
        }
        let suffix = log
            .append_replicated_batch_with_mode(
                epoch,
                1,
                messages(1..5),
                ReplicatedAppendMode::AppendSuffixAfterKnownPrefix,
                flush,
            )
            .await;
        assert!(
            matches!(
                suffix,
                Ok(ReplicatedAppendOutcome::AppliedSuffix { skipped_count: 2, .. })
            ),
            "{suffix:?}"
        );
        assert_eq!(log.durable_next(), 0);
        assert_eq!(log.next_offset(), 5);

        // Different bytes over the written range are still refused.
        let diverged = log
            .append_replicated_batch(epoch, 0, messages(9..12), flush)
            .await;
        assert!(diverged.is_err(), "{diverged:?}");

        log.sync().await.unwrap();
        assert_eq!(log.durable_next(), 5);
        log.shutdown().await.unwrap();
    }
}

#[cfg(feature = "io-fault-injection")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn after_fsync_retry_while_sync_is_held() {
    use keratin_log::io_fault_injection::{Fault, arm_gated};
    use std::time::{Duration, Instant};

    let dir = test_dir!("overlap_held_sync_retry");
    let log = std::sync::Arc::new(follower(&dir, 1 << 20).await);
    let epoch = log.current_epoch();
    let fsync = Some(KDurability::AfterFsync);
    log.append_replicated_batch(epoch, 0, messages(0..3), Some(KDurability::AfterWrite))
        .await
        .unwrap();

    let gate = arm_gated(
        &dir.root.join("segments/00000000000000000000.log"),
        Fault::FsyncPause,
    )
    .unwrap();
    let syncing = log.clone();
    let sync = tokio::spawn(async move { syncing.sync().await });
    let deadline = Instant::now() + Duration::from_secs(5);
    while gate.hits() == 0 {
        assert!(Instant::now() < deadline, "sync never reached the file");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let retry = tokio::time::timeout(
        Duration::from_secs(5),
        log.append_replicated_batch(epoch, 0, messages(0..3), fsync),
    )
    .await
    .expect("retry must not wait on the held sync");
    assert_not_durable_error(&retry);
    assert_eq!(log.durable_next(), 0);

    gate.release();
    sync.await.unwrap().unwrap();
    assert_eq!(log.durable_next(), 3);
    let after = log.append_replicated_batch(epoch, 0, messages(0..3), fsync).await;
    assert!(
        matches!(after, Ok(ReplicatedAppendOutcome::AlreadyPresent { .. })),
        "{after:?}"
    );
    assert_durable_if_present(&log, &after);
    log.shutdown().await.unwrap();
}
