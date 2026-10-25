use super::*;
use crate::writer::test_hooks;
use std::time::Duration;

const REPLY_TIMEOUT: Duration = Duration::from_secs(5);

fn message(n: u8) -> Message {
    Message {
        flags: 0,
        headers: vec![],
        payload: vec![n; 32],
    }
}

fn queue_append(log: &Keratin) -> oneshot::Receiver<Result<AppendResult, IoError>> {
    let (result_tx, reply) = oneshot::channel();
    log.tx
        .send(WriterCmd::Append(AppendReq {
            records: AppendPayload::One(message(1)),
            durability: Some(KDurability::AfterFsync),
            completion: AppendCompletionTarget::Oneshot(result_tx),
            staged_offset_tx: None,
        }))
        .unwrap();
    reply
}

// Commands that reach the channel after the writer handled Shutdown, but
// before it stopped receiving, must still get an answer.
#[tokio::test]
async fn commands_queued_behind_shutdown_are_answered() {
    let dir = crate::test_dir!("queued_behind_shutdown");
    let log = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    let (reached, release) = test_hooks::pause_after_shutdown(&dir.root);
    log.shutdown().await.unwrap();
    reached.recv_timeout(REPLY_TIMEOUT).unwrap();

    // Bypass the handle's shutdown check to land in the race window.
    let append = queue_append(&log);
    let (sync_tx, sync) = oneshot::channel();
    log.tx
        .send(WriterCmd::Sync {
            respond_to: sync_tx,
        })
        .unwrap();
    let (settings_tx, settings) = oneshot::channel();
    log.tx
        .send(WriterCmd::ReadSettings {
            respond_to: settings_tx,
        })
        .unwrap();
    let (again_tx, again) = oneshot::channel();
    log.tx
        .send(WriterCmd::Shutdown {
            notify_tx: again_tx,
        })
        .unwrap();
    release.send(()).unwrap();

    let error = tokio::time::timeout(REPLY_TIMEOUT, append)
        .await
        .expect("append queued behind shutdown never resolved")
        .expect("append reply dropped instead of answered")
        .unwrap_err();
    assert!(error.to_string().contains("shut down"), "{error}");
    let error = tokio::time::timeout(REPLY_TIMEOUT, sync)
        .await
        .expect("sync queued behind shutdown never resolved")
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    // Dropping the reply is the answer for a settings read.
    assert!(
        tokio::time::timeout(REPLY_TIMEOUT, settings)
            .await
            .unwrap()
            .is_err()
    );
    // A repeated shutdown is acknowledged.
    tokio::time::timeout(REPLY_TIMEOUT, again)
        .await
        .unwrap()
        .unwrap();
}

// Appends racing a shutdown from other tasks must all resolve, either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn appends_racing_shutdown_all_resolve() {
    let dir = crate::test_dir!("appends_racing_shutdown");
    for round in 0..300 {
        let root = dir.root.join(round.to_string());
        let log = Arc::new(
            Keratin::open(&root, KeratinConfig::test_default())
                .await
                .unwrap(),
        );
        let mut tasks = Vec::new();
        for task in 0..4u8 {
            let log = log.clone();
            tasks.push(tokio::spawn(async move {
                for _ in 0..8 {
                    if log.append(message(task), None).await.is_err() {
                        break;
                    }
                }
            }));
        }
        let closer = {
            let log = log.clone();
            tokio::spawn(async move { log.shutdown().await })
        };
        for task in tasks {
            tokio::time::timeout(REPLY_TIMEOUT, task)
                .await
                .unwrap_or_else(|_| panic!("append hung in round {round}"))
                .unwrap();
        }
        tokio::time::timeout(REPLY_TIMEOUT, closer)
            .await
            .unwrap_or_else(|_| panic!("shutdown hung in round {round}"))
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn commands_after_shutdown_fail_without_waiting() {
    let dir = crate::test_dir!("commands_after_shutdown");
    let log = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    log.shutdown().await.unwrap();

    // The enqueue step itself refuses, so nothing waits on the writer.
    let error = log.append_enqueue_receiver(message(1), None).unwrap_err();
    assert!(error.to_string().contains("shut down"), "{error}");
    assert!(log.append_batch(vec![message(2)], None).await.is_err());
    let error = log.sync().await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    assert!(log.estimate_disk_used().await.is_err());
    assert!(log.writer_settings().await.is_err());
    assert!(log.truncate_before(0).await.is_err());
    // Shutdown stays idempotent.
    log.shutdown().await.unwrap();
}

// After shutdown the writer thread keeps answering until the last sender is
// gone, then exits.
#[tokio::test]
async fn writer_thread_exits_when_last_handle_drops() {
    let dir = crate::test_dir!("writer_exit_after_shutdown");
    let log = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    let exited = test_hooks::on_writer_exit(&dir.root);
    log.shutdown().await.unwrap();
    assert_eq!(
        exited.recv_timeout(Duration::from_millis(50)),
        Err(crossbeam_channel::RecvTimeoutError::Timeout)
    );
    drop(log);
    exited.recv_timeout(REPLY_TIMEOUT).unwrap();

    // Dropping without an explicit shutdown also ends the thread.
    let log = Keratin::open(&dir.root, KeratinConfig::test_default())
        .await
        .unwrap();
    let exited = test_hooks::on_writer_exit(&dir.root);
    drop(log);
    exited.recv_timeout(REPLY_TIMEOUT).unwrap();
}
