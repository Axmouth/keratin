use std::sync::Arc;

use keratin_log::*;

fn msg(payload: impl Into<Vec<u8>>) -> Message {
    Message {
        flags: 0,
        headers: vec![],
        payload: payload.into(),
    }
}

#[tokio::test]
async fn open_fails_when_already_open() {
    let dir = test_dir!("keratin_lock");

    let cfg = KeratinConfig::test_default();

    // First open should succeed
    let k1 = Keratin::open(&dir.root, cfg)
        .await
        .expect("first open should succeed");

    // Second open on same directory should fail
    let k2 = Keratin::open(&dir.root, cfg).await;

    dbg!(&k2);

    assert!(k2.is_err(), "second open on same root must fail");

    // Drop first instance
    drop(k1);

    // Now it should succeed again
    let k3 = Keratin::open(&dir.root, cfg)
        .await
        .expect("open after drop should succeed");

    drop(k3);
}

#[tokio::test]
async fn replicated_append_exact_fit_applies_offsets() {
    let dir = test_dir!("replicated_exact_fit");
    let k = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    k.become_follower();

    let outcome = k
        .append_replicated_batch(0, vec![msg("a"), msg("b")], Some(KDurability::AfterFsync))
        .await
        .unwrap();

    assert_eq!(
        outcome,
        ReplicatedAppendOutcome::Applied(AppendResult {
            base_offset: 0,
            count: 2
        })
    );
    assert_eq!(k.next_offset(), 2);

    let got = k.reader().scan_from(0, 10).unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].offset, 0);
    assert_eq!(got[0].payload, b"a");
    assert_eq!(got[1].offset, 1);
    assert_eq!(got[1].payload, b"b");
}

#[tokio::test]
async fn replicated_append_rejects_gap_without_writing() {
    let dir = test_dir!("replicated_gap");
    let k = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    k.become_follower();

    let outcome = k
        .append_replicated_batch(3, vec![msg("late")], None)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        ReplicatedAppendOutcome::Gap {
            expected_offset: 0,
            first_offset: 3,
        }
    );
    assert_eq!(k.next_offset(), 0);
    assert!(k.reader().scan_from(0, 10).unwrap().is_empty());
}

#[tokio::test]
async fn replicated_append_reports_already_present_batch() {
    let dir = test_dir!("replicated_already_present");
    let k = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    k.become_follower();

    k.append_replicated_batch(0, vec![msg("a"), msg("b")], None)
        .await
        .unwrap();

    let outcome = k
        .append_replicated_batch(0, vec![msg("a"), msg("b")], None)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        ReplicatedAppendOutcome::AlreadyPresent {
            first_offset: 0,
            count: 2,
            next_offset: 2,
        }
    );
    assert_eq!(k.reader().scan_from(0, 10).unwrap().len(), 2);
}

#[tokio::test]
async fn replicated_append_reports_partial_overlap_without_writing() {
    let dir = test_dir!("replicated_partial_overlap");
    let k = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    k.become_follower();

    k.append_replicated_batch(0, vec![msg("a"), msg("b")], None)
        .await
        .unwrap();

    let outcome = k
        .append_replicated_batch(1, vec![msg("b"), msg("c")], None)
        .await
        .unwrap();

    assert_eq!(
        outcome,
        ReplicatedAppendOutcome::Overlap {
            first_offset: 1,
            count: 2,
            next_offset: 2,
        }
    );
    assert_eq!(k.next_offset(), 2);
    assert_eq!(k.reader().scan_from(0, 10).unwrap().len(), 2);
}

#[tokio::test]
async fn replicated_append_can_append_suffix_after_known_prefix() {
    let dir = test_dir!("replicated_suffix_overlap");
    let k = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    k.become_follower();

    k.append_replicated_batch(0, vec![msg("a"), msg("b")], None)
        .await
        .unwrap();

    let outcome = k
        .append_replicated_batch_with_mode(
            1,
            vec![msg("b"), msg("c"), msg("d")],
            ReplicatedAppendMode::AppendSuffixAfterKnownPrefix,
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        outcome,
        ReplicatedAppendOutcome::AppliedSuffix {
            requested_first_offset: 1,
            skipped_count: 1,
            result: AppendResult {
                base_offset: 2,
                count: 2
            },
        }
    );

    let got = k.reader().scan_from(0, 10).unwrap();
    assert_eq!(got.len(), 4);
    assert_eq!(got[0].payload, b"a");
    assert_eq!(got[1].payload, b"b");
    assert_eq!(got[2].payload, b"c");
    assert_eq!(got[3].payload, b"d");
}

#[tokio::test]
async fn reset_to_checkpoint_starts_empty_log_at_offset() {
    let dir = test_dir!("reset_to_checkpoint");
    let k = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    k.become_follower();

    k.append_replicated_batch(0, vec![msg("old-a"), msg("old-b")], None)
        .await
        .unwrap();
    k.reset_to_checkpoint(10).await.unwrap();

    assert_eq!(k.head_offset(), 10);
    assert_eq!(k.next_offset(), 10);
    assert!(k.reader().scan_from(0, 20).unwrap().is_empty());

    let old_outcome = k
        .append_replicated_batch(8, vec![msg("too-old")], None)
        .await
        .unwrap();
    assert_eq!(
        old_outcome,
        ReplicatedAppendOutcome::AlreadyPresent {
            first_offset: 8,
            count: 1,
            next_offset: 10,
        }
    );

    let outcome = k
        .append_replicated_batch(10, vec![msg("new-a"), msg("new-b")], None)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        ReplicatedAppendOutcome::Applied(AppendResult {
            base_offset: 10,
            count: 2
        })
    );

    let got = k.reader().scan_from(10, 20).unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].offset, 10);
    assert_eq!(got[0].payload, b"new-a");
    assert_eq!(got[1].offset, 11);
    assert_eq!(got[1].payload, b"new-b");
}

#[tokio::test]
async fn reset_to_checkpoint_persists_across_reopen() {
    let dir = test_dir!("reset_to_checkpoint_reopen");

    {
        let k = Keratin::open(&dir.root, KeratinConfig::test_default())
            .await
            .unwrap();
        k.become_follower();
        k.append_replicated_batch(0, vec![msg("old")], None)
            .await
            .unwrap();
        k.reset_to_checkpoint(5).await.unwrap();
        k.append_replicated_batch(5, vec![msg("new")], Some(KDurability::AfterFsync))
            .await
            .unwrap();
    }

    let k = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    assert_eq!(k.head_offset(), 5);
    assert_eq!(k.next_offset(), 6);

    let got = k.reader().scan_from(0, 20).unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].offset, 5);
    assert_eq!(got[0].payload, b"new");
}

#[tokio::test]
async fn keratin_role_guards_normal_and_replicated_writes() {
    let dir = test_dir!("keratin_role_guards");
    let k = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();

    assert_eq!(k.role(), KeratinRole::Owner);
    assert!(
        k.append_replicated_batch(0, vec![msg("replicated")], None)
            .await
            .is_err(),
        "owner mode must not accept replicated appends"
    );

    k.become_follower();
    assert_eq!(k.role(), KeratinRole::Follower);
    assert!(
        k.append(msg("owner-write"), None).await.is_err(),
        "follower mode must not accept owner appends"
    );
    assert!(
        k.append_replicated_batch(0, vec![msg("replicated")], None)
            .await
            .is_ok(),
        "follower mode should accept replicated appends"
    );

    k.freeze();
    assert_eq!(k.role(), KeratinRole::Frozen);
    assert!(
        k.append(msg("owner-write"), None).await.is_err(),
        "frozen mode must not accept owner appends"
    );
    assert!(
        k.append_replicated_batch(1, vec![msg("replicated")], None)
            .await
            .is_err(),
        "frozen mode must not accept replicated appends"
    );

    k.become_owner();
    assert_eq!(k.role(), KeratinRole::Owner);
    assert!(k.append(msg("owner-write"), None).await.is_ok());
}

#[tokio::test]
async fn wal_append_scan_identity() {
    let dir = test_dir!("wal_identity");

    let cfg = KeratinConfig::test_default();
    let k = Keratin::open(&dir.root, cfg).await.unwrap();

    let mut sent = Vec::new();

    for _ in 0..5 {
        let mut to_send = vec![];
        for i in 0..2_000 {
            let payload = format!("msg-{i}").into_bytes();
            let m = Message {
                flags: 0,
                headers: vec![],
                payload: payload.clone(),
            };

            to_send.push(m);
            sent.push(payload);
        }
        k.append_batch(to_send, None).await.unwrap();
    }

    let reader = k.reader();
    let got = reader.scan_from(0, 20_000).unwrap();

    assert_eq!(got.len(), sent.len());
    for (i, r) in got.iter().enumerate() {
        assert_eq!(r.offset as usize, i);
        assert_eq!(r.payload, sent[i]);
    }
}

#[tokio::test]
async fn wal_recovery_identity() {
    let dir = test_dir!("wal_recovery");

    {
        let k = Keratin::open(&dir.root, KeratinConfig::test_default())
            .await
            .unwrap();

        for i in 0..10 {
            let mut batch = vec![];
            for j in 0..500 {
                batch.push(Message {
                    flags: 0,
                    headers: vec![],
                    payload: format!("msg-{}-{}", i, j).into_bytes(),
                });
            }
            k.append_batch(batch, None).await.unwrap();
        }
    } // drop without clean shutdown

    let k = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    let got = k.reader().scan_from(0, 100_000).unwrap();

    assert_eq!(got.len(), 5000);
    for (i, r) in got.iter().enumerate() {
        assert_eq!(r.offset as usize, i);
    }
}

#[tokio::test]
async fn scan_past_end_must_not_hang() {
    let dir = test_dir!("scan_past_end");

    let k = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();

    let mut batch = vec![];
    for i in 0..100 {
        batch.push(Message {
            flags: 0,
            headers: vec![],
            payload: format!("msg-{i}").into_bytes(),
        });
    }
    k.append_batch(batch, None).await.unwrap();

    // Ask for more than exist
    let got = k.reader().scan_from(0, 10_000).unwrap();
    assert_eq!(got.len(), 100);
}

#[tokio::test]
async fn wal_multi_producer_ordering() {
    let dir = test_dir!("wal_multi");

    let k = Arc::new(
        Keratin::open(&dir.root, KeratinConfig::test_default())
            .await
            .unwrap(),
    );

    let mut handles = vec![];
    for p in 0..4 {
        let k = k.clone();
        handles.push(tokio::spawn(async move {
            let mut batch = vec![];
            for i in 0..1000 {
                batch.push(Message {
                    flags: 0,
                    headers: vec![],
                    payload: format!("p{}-{}", p, i).into_bytes(),
                });
            }
            k.append_batch(batch, None).await.unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let got = k.reader().scan_from(0, 10_000).unwrap();
    assert_eq!(got.len(), 4000);

    for (i, r) in got.iter().enumerate() {
        assert_eq!(r.offset as usize, i);
    }
}

#[tokio::test]
async fn wal_segment_roll_continuity() {
    let dir = test_dir!("wal_roll");

    let mut cfg = KeratinConfig::test_default();
    cfg.segment_max_bytes = 64 * 1024; // force many segments

    let k = Keratin::open(&dir.root, cfg).await.unwrap();

    let mut batch = vec![];
    for i in 0..10_000 {
        batch.push(Message {
            flags: 0,
            headers: vec![],
            payload: vec![i as u8; 128],
        });
    }
    k.append_batch(batch, None).await.unwrap();

    let got = k.reader().scan_from(0, 20_000).unwrap();
    assert_eq!(got.len(), 10_000);

    for (i, r) in got.iter().enumerate() {
        assert_eq!(r.offset as usize, i);
    }
}

#[tokio::test]
async fn wal_high_contention_storm() {
    let dir = test_dir!("wal_storm");

    let k = Arc::new(
        Keratin::open(&dir.root, KeratinConfig::test_default())
            .await
            .unwrap(),
    );

    let mut handles = vec![];
    for p in 0..8 {
        let k = k.clone();
        handles.push(tokio::spawn(async move {
            let mut batch = vec![];
            for i in 0..5000 {
                batch.push(Message {
                    flags: 0,
                    headers: vec![],
                    payload: format!("p{p}-{i}").into_bytes(),
                });
            }
            k.append_batch(batch, Some(KDurability::AfterFsync))
                .await
                .unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let got = k.reader().scan_from(0, 100_000).unwrap();
    assert_eq!(got.len(), 40_000);

    for (i, r) in got.iter().enumerate() {
        assert_eq!(r.offset as usize, i);
    }
}
