use keratin_log::*;

async fn seeded(root: &std::path::Path, epoch: u64) -> Keratin {
    let k = Keratin::open(root, KeratinConfig::test_default())
        .await
        .unwrap();
    k.advance_epoch(epoch).await.unwrap();
    k.become_follower();
    k.append_replicated_batch(
        epoch,
        0,
        vec![Message {
            flags: 0,
            headers: vec![],
            payload: b"preserve".to_vec(),
        }],
        Some(KDurability::AfterFsync),
    )
    .await
    .unwrap();
    k
}

#[tokio::test]
async fn checkpoint_recovery_preflights_every_epoch_and_lock_before_deletion() {
    let a = test_dir!("checkpoint_preflight_a");
    let b = test_dir!("checkpoint_preflight_b");
    let first = seeded(&a.root, 7).await;
    first.shutdown().await.unwrap();
    drop(first);
    let second = seeded(&b.root, 8).await;
    let plan = || vec![(a.root.clone(), 4, 7), (b.root.clone(), 4, 7)];
    assert!(
        Keratin::rebuild_for_checkpoint_recovery(plan())
            .await
            .is_err()
    );
    second.shutdown().await.unwrap();
    drop(second);
    assert!(
        Keratin::rebuild_for_checkpoint_recovery(plan())
            .await
            .is_err()
    );
    for (root, epoch) in [(&a.root, 7), (&b.root, 8)] {
        let k = Keratin::open(root, KeratinConfig::test_default())
            .await
            .unwrap();
        assert_eq!(k.current_epoch(), epoch);
        assert_eq!(k.next_offset(), 1);
        assert_eq!(k.reader().scan_from(0, 1).unwrap()[0].payload, b"preserve");
        k.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn checkpoint_recovery_rebuilds_partial_files_preserving_epoch() {
    let dir = test_dir!("checkpoint_rebuild_partial");
    let k = seeded(&dir.root, 7).await;
    k.shutdown().await.unwrap();
    drop(k);
    for entry in std::fs::read_dir(dir.root.join("segments")).unwrap() {
        std::fs::write(entry.unwrap().path(), b"incomplete").unwrap();
    }
    Keratin::rebuild_for_checkpoint_recovery(vec![(dir.root.clone(), 4, 7)])
        .await
        .unwrap();
    let k = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    assert_eq!(k.current_epoch(), 7);
    assert_eq!(k.head_offset(), 4);
    assert_eq!(k.next_offset(), 4);
    assert!(k.reader().scan_from(4, 10).unwrap().is_empty());
    k.shutdown().await.unwrap();
}
