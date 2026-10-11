//! Single-log early-read fault acceptance. The gated owner-fsync/follower-durable
//! matrix lives in combined_tests. Separate owner-writer completion orders no
//! longer exist. These cases retain partial-write and abrupt-exit coverage.
use super::*;
use keratin_log::test_dir;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_write_fault_keeps_partial_bytes_out_of_early_replication() {
    use keratin_log::io_fault_injection::{Fault, arm_repeated};
    for prefix in [0, 1] {
        for fault in [Fault::Write, Fault::PartialWrite] {
            let label = format!("prefix={prefix} fault={fault:?}");
            eprintln!("WRITE CASE {label}");
            let dir = test_dir!("write_fault_owner");
            let config = StromaKeratinConfig::from_message_log(KeratinConfig {
                flush_target_bytes: 8,
                fsync_interval_ms: 20,
                min_fsync_interval_ms: 5,
                ..KeratinConfig::test_default()
            });
            let owner = Stroma::open(&dir.root, config, SnapshotConfig::default())
                .await
                .unwrap();
            let qh = owner.queue_handle("q", 0, None).await.unwrap();
            let h = qh.resolve().unwrap();
            for offset in 0..=prefix {
                let path = owner
                    .tp_part_dir("q", 0, None)
                    .join("segments")
                    .join("00000000000000000000.log");
                let armed =
                    (offset == prefix).then(|| arm_repeated(&path, fault, usize::MAX).unwrap());
                let (completion, receipt) = KeratinAppendCompletion::pair();
                owner
                    .append_message_batch(
                        "q",
                        0,
                        None,
                        vec![PublishItem {
                            headers: MessageHeaders {
                                published: Default::default(),
                                publish_received: Default::default(),
                                content_type: None,
                                extra: Default::default(),
                            },
                            payload: vec![offset as u8; 128],
                            completion,
                            not_before: None,
                            expire_at: None,
                        }],
                    )
                    .await
                    .unwrap();
                let outcome = tokio::time::timeout(Duration::from_secs(5), receipt).await;
                eprintln!("WRITE OUTCOME {label}: {outcome:?}");
                let outcome = outcome.expect("write error must complete receipt").unwrap();
                if offset < prefix {
                    outcome.unwrap();
                    continue;
                }
                assert!(outcome.is_err(), "false confirmation: {label}");
                assert!(armed.as_ref().unwrap().hits() > 0);
                assert!(!owner.is_ready("q", 0, None, offset).await.unwrap());
                let failed = h.event_log();
                assert_eq!(
                    failed.reader().scan_from(0, 10).unwrap().len() as u64,
                    prefix
                );

                let OwnerReplicationRead::Batch(batch) = owner
                    .read_owner_queue_records_early("q", 0, None, prefix, 10, usize::MAX)
                    .await
                    .unwrap()
                else {
                    panic!()
                };
                assert!(
                    batch.records.is_empty(),
                    "partial write entered tentative cache"
                );

                assert_eq!(h.event_log().next_offset(), prefix + 1);
                drop(armed);
            }
            let shutdown = owner.shutdown().await;
            if let Err(error) = shutdown {
                // Immediate staging fences the writer on a buffer-write error.
                // Shutdown reports that failure and still closes the failed log.
                assert_eq!(
                    std::env::var("KERATIN_STAGE_THEN_COMMIT").as_deref(),
                    Ok("1"),
                    "unexpected shutdown error: {error:?} ({label})"
                );
                assert!(
                    matches!(error, crate::StromaError::Io(ref message) if message == "writer gone" || message == "writer dropped"),
                    "unexpected shutdown error: {error:?} ({label})"
                );
            }
            for log in [h.event_log()] {
                let error = tokio::time::timeout(Duration::from_secs(5), log.sync())
                    .await
                    .expect("shutdown must finish the writer")
                    .expect_err("shutdown must close the writer");
                assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe, "{label}");
            }
            drop(h);
            drop(qh);
            drop(owner);
            let reopened = Stroma::open(
                &dir.root,
                StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
                SnapshotConfig::default(),
            )
            .await
            .unwrap();
            for confirmed in 0..prefix {
                assert!(
                    reopened.is_ready("q", 0, None, confirmed).await.unwrap(),
                    "confirmed prefix lost: {label}"
                );
            }
            eprintln!(
                "WRITE REOPEN {label}: uncertain_ready={}",
                reopened.is_ready("q", 0, None, prefix).await.unwrap()
            );
            reopened.shutdown().await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_io_fault_process_exit_preserves_confirmed_prefix() {
    use keratin_log::io_fault_injection::{Fault, arm_repeated};
    if let Ok(root) = std::env::var("KERATIN_IO_FAULT_CHILD_ROOT") {
        let prefix: u64 = std::env::var("KERATIN_IO_FAULT_PREFIX")
            .unwrap()
            .parse()
            .unwrap();
        let fault = if std::env::var("KERATIN_IO_FAULT_KIND").unwrap() == "partial" {
            Fault::PartialWrite
        } else {
            Fault::Fsync
        };
        let config = StromaKeratinConfig::from_message_log(KeratinConfig {
            flush_target_bytes: 8,
            min_fsync_interval_ms: 5,
            fsync_interval_ms: 20,
            ..KeratinConfig::test_default()
        });
        let owner = Stroma::open(root, config, SnapshotConfig::default())
            .await
            .unwrap();
        let qh = owner.queue_handle("q", 0, None).await.unwrap();
        for offset in 0..=prefix {
            let path = owner
                .tp_part_dir("q", 0, None)
                .join("segments")
                .join("00000000000000000000.log");
            let armed = (offset == prefix).then(|| arm_repeated(&path, fault, usize::MAX).unwrap());
            let (completion, receipt) = KeratinAppendCompletion::pair();
            owner
                .append_message_batch(
                    "q",
                    0,
                    None,
                    vec![PublishItem {
                        headers: MessageHeaders {
                            published: Default::default(),
                            publish_received: Default::default(),
                            content_type: None,
                            extra: Default::default(),
                        },
                        payload: vec![offset as u8; 128],
                        completion,
                        not_before: None,
                        expire_at: None,
                    }],
                )
                .await
                .unwrap();
            let result = tokio::time::timeout(Duration::from_secs(5), receipt)
                .await
                .unwrap()
                .unwrap();
            if offset < prefix {
                result.unwrap();
            } else {
                assert!(result.is_err());
                assert!(armed.as_ref().unwrap().hits() > 0);
                assert!(!owner.is_ready("q", 0, None, offset).await.unwrap());
                // Bypass Rust destructors and all graceful Stroma/Keratin shutdown.
                // The parent reopens real files left at this error boundary.
                std::process::exit(77);
            }
        }
        drop(qh);
        panic!("child did not reach error boundary");
    }
    for prefix in [0, 1] {
        for log in ["event"] {
            for kind in ["partial", "fsync"] {
                let dir = test_dir!("fault_process_exit");
                let output = tokio::time::timeout(Duration::from_secs(15), tokio::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", "stroma::early_replication_tests::owner_io_fault_process_exit_preserves_confirmed_prefix", "--nocapture"])
                    .env("KERATIN_IO_FAULT_CHILD_ROOT", &dir.root)
                    .env("KERATIN_IO_FAULT_PREFIX", prefix.to_string())
                    .env("KERATIN_IO_FAULT_LOG", log)
                    .env("KERATIN_IO_FAULT_KIND", kind)
                    .kill_on_drop(true).output()).await.unwrap().unwrap();
                assert_eq!(
                    output.status.code(),
                    Some(77),
                    "child prefix={prefix} log={log} kind={kind}: {} {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                let reopened = Stroma::open(
                    &dir.root,
                    StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
                    SnapshotConfig::default(),
                )
                .await
                .unwrap();
                for confirmed in 0..prefix {
                    assert!(reopened.is_ready("q", 0, None, confirmed).await.unwrap());
                }
                eprintln!(
                    "PROCESS EXIT prefix={prefix} log={log} kind={kind} uncertain_ready={}",
                    reopened.is_ready("q", 0, None, prefix).await.unwrap()
                );
                reopened.shutdown().await.unwrap();
            }
        }
    }
}
