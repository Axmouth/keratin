#![cfg(feature = "io-fault-injection")]
use keratin_log::io_fault_injection::{Fault, arm};
use keratin_log::*;

fn message(payload: impl Into<Vec<u8>>) -> Message {
    Message {
        payload: payload.into(),
        flags: 0,
        headers: vec![],
    }
}

fn manifest_clean_flag(root: &std::path::Path) -> bool {
    let bytes = std::fs::read(root.join("manifest.bin")).unwrap();
    let flags = u16::from_be_bytes(bytes[10..12].try_into().unwrap());
    flags & 0x0001 != 0
}

// After a failed fsync the kernel may drop the dirty pages and report success
// on the next fsync, so the handle must stop instead of letting a later sync
// cover the failed range.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_fsync_stops_the_log_until_reopen() {
    for fault in [Fault::Fsync, Fault::FsyncAfterSuccess] {
        let dir = test_dir!("failed_fsync_stops_log");
        let log = Keratin::open(&dir.root, KeratinConfig::test_default())
            .await
            .unwrap();
        log.append(message(vec![1; 64]), Some(KDurability::AfterFsync))
            .await
            .unwrap();
        let durable = log.durable_offset();
        let armed = arm(&dir.root.join("segments/00000000000000000000.log"), fault).unwrap();
        assert!(
            log.append(message(vec![2; 64]), Some(KDurability::AfterFsync))
                .await
                .is_err()
        );
        assert_eq!(armed.hits(), 1);
        drop(armed);

        let later = log
            .append(message(vec![3; 64]), Some(KDurability::AfterFsync))
            .await;
        assert!(later.is_err(), "{fault:?}: append after failure must fail");
        assert!(log.sync().await.is_err());
        assert!(log.truncate_before(1).await.is_err());
        assert_eq!(log.durable_offset(), durable);
        assert!(log.reader().scan_from(1, 10).unwrap().is_empty());

        // Shutdown still releases the handle but leaves the log dirty, so the
        // next open runs the recovery scan.
        log.shutdown().await.unwrap();
        assert!(!manifest_clean_flag(&dir.root));
        drop(log);

        let reopened = Keratin::open(&dir.root, KeratinConfig::test_default())
            .await
            .unwrap();
        let next = reopened
            .append(message(vec![4; 64]), Some(KDurability::AfterFsync))
            .await
            .unwrap();
        assert!(next.base_offset >= 1);
        reopened.shutdown().await.unwrap();
    }
}
