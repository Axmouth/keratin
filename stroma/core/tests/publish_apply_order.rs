//! Drive the later durability continuation first without speculative registration.
use std::{sync::Arc, time::Duration};

use keratin_log::{CompletionPair, KeratinAppendCompletion, KeratinConfig, test_dir};
use stroma_core::{
    ExperimentalStageObserver, MessageHeaders, PublishItem, QueueHandle, SnapshotConfig, Stroma,
    StromaKeratinConfig,
};
use tokio::sync::Notify;

#[derive(Default)]
struct PauseApply {
    entered: Notify,
    release: Notify,
}

#[async_trait::async_trait]
impl ExperimentalStageObserver for PauseApply {
    async fn staged(
        &self,
        _queue: QueueHandle,
        _base: u64,
        _count: usize,
        _eligible: bool,
    ) -> stroma_core::Result<()> {
        // Deliberately no speculative actor registration: ordinary delivery.
        Ok(())
    }

    async fn before_apply(&self) -> bool {
        self.entered.notify_one();
        self.release.notified().await;
        true
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn later_durable_batch_cannot_overtake_paused_earlier_apply() {
    let dir = test_dir!("publish_apply_order");
    let st = Stroma::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    let pause = Arc::new(PauseApply::default());
    let mut confirms = Vec::new();
    for id in 0..2 {
        let (completion, rx) = KeratinAppendCompletion::pair();
        st.append_message_batch_observed(
            "ordered",
            0,
            None,
            vec![PublishItem {
                headers: MessageHeaders {
                    published: Default::default(),
                    publish_received: Default::default(),
                    content_type: None,
                    extra: Default::default(),
                },
                payload: vec![id],
                completion,
                not_before: None,
                expire_at: None,
            }],
            if id == 0 { Some(pause.clone()) } else { None },
        )
        .await
        .unwrap();
        confirms.push(rx);
        if id == 0 {
            tokio::time::timeout(Duration::from_secs(5), pause.entered.notified())
                .await
                .unwrap();
        }
    }
    // Both disk writers can finish batch 1 while application of batch 0 is paused.
    let queue = st.queue_handle("ordered", 0, None).await.unwrap();
    let handle = queue.resolve().unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while handle.msg_log().durable_offset() < 1 || handle.event_log().durable_offset() < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut second = confirms.pop().unwrap();
    let overtook = tokio::time::timeout(Duration::from_millis(100), &mut second)
        .await
        .is_ok();
    let visible = st.is_ready("ordered", 0, None, 1).await.unwrap();
    pause.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), confirms.pop().unwrap())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    if !overtook {
        tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    st.shutdown().await.unwrap();
    assert!(
        !overtook && !visible,
        "later batch overtook paused earlier apply: confirmed={overtook}, visible={visible}"
    );
}
