//! Experimental ordering of durable applications; disk appends remain concurrent.
//! Abandoning an admitted operation poisons the sequence until recovery.
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

#[derive(Debug)]
pub(crate) struct OrderedApply {
    state: Mutex<State>,
    changed: Notify,
}
#[derive(Debug)]
struct State {
    generation: u64,
    next: u64,
    active: bool,
    failed: bool,
}

impl OrderedApply {
    pub(crate) fn new(next: u64) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                generation: 0,
                next,
                active: false,
                failed: false,
            }),
            changed: Notify::new(),
        })
    }
    pub(crate) fn reset(&self, next: u64) {
        let mut s = self.state.lock().unwrap();
        s.generation += 1;
        s.next = next;
        s.active = false;
        s.failed = false;
        drop(s);
        self.changed.notify_waiters();
    }
    pub(crate) fn scope(self: &Arc<Self>) -> Scope {
        Scope {
            order: self.clone(),
            generation: self.state.lock().unwrap().generation,
            complete: false,
        }
    }
    pub(crate) fn applied_next(&self) -> crate::Result<u64> {
        let s = self.state.lock().unwrap();
        if s.failed || s.active {
            return Err(crate::StromaError::Io(
                "application boundary is not quiescent".into(),
            ));
        }
        Ok(s.next)
    }
    pub(crate) fn failed(&self) -> bool {
        self.state.lock().unwrap().failed
    }

    /// The permit travels with the actor command so cancellation cannot let a
    /// queued capture race with a later durable application.
    pub(crate) async fn checkpoint(self: &Arc<Self>) -> crate::Result<CheckpointPermit> {
        let generation = self.state.lock().unwrap().generation;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut s = self.state.lock().unwrap();
                if s.failed || s.generation != generation {
                    return Err(crate::StromaError::Io(
                        "checkpoint application boundary invalidated".into(),
                    ));
                }
                if !s.active {
                    s.active = true;
                    return Ok(CheckpointPermit {
                        order: self.clone(),
                        generation,
                        next: s.next,
                    });
                }
            }
            changed.await;
        }
    }
}

#[derive(Debug)]
pub struct CheckpointPermit {
    order: Arc<OrderedApply>,
    generation: u64,
    next: u64,
}
impl CheckpointPermit {
    pub(crate) fn event_next(&self) -> crate::Result<u64> {
        let s = self.order.state.lock().unwrap();
        if s.failed || s.generation != self.generation || !s.active || s.next != self.next {
            return Err(crate::StromaError::Io(
                "checkpoint application boundary invalidated".into(),
            ));
        }
        Ok(self.next)
    }
}
impl Drop for CheckpointPermit {
    fn drop(&mut self) {
        let mut s = self.order.state.lock().unwrap();
        if s.generation == self.generation {
            s.active = false;
        }
        drop(s);
        self.order.changed.notify_waiters();
    }
}

pub(crate) struct Scope {
    order: Arc<OrderedApply>,
    generation: u64,
    complete: bool,
}
impl Scope {
    pub(crate) async fn enter(&self, first: u64, count: u64) -> crate::Result<Turn> {
        let next = first
            .checked_add(count)
            .filter(|_| count > 0)
            .ok_or_else(|| crate::StromaError::Io("invalid ordered application range".into()))?;
        loop {
            let changed = self.order.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut s = self.order.state.lock().unwrap();
                if s.generation != self.generation || s.failed || first < s.next {
                    return Err(crate::StromaError::Io(
                        "ordered application interrupted; recovery required".into(),
                    ));
                }
                if first == s.next && !s.active {
                    s.active = true;
                    return Ok(Turn {
                        order: self.order.clone(),
                        generation: self.generation,
                        next,
                        complete: false,
                    });
                }
            }
            changed.await;
        }
    }
    pub(crate) fn complete(mut self) {
        self.complete = true;
    }
}
fn poison(order: &OrderedApply, generation: u64) {
    let mut s = order.state.lock().unwrap();
    if s.generation == generation {
        s.failed = true;
    }
    drop(s);
    order.changed.notify_waiters();
}
impl Drop for Scope {
    fn drop(&mut self) {
        if !self.complete {
            poison(&self.order, self.generation);
        }
    }
}
pub(crate) struct Turn {
    order: Arc<OrderedApply>,
    generation: u64,
    next: u64,
    complete: bool,
}
impl Turn {
    pub(crate) fn complete(mut self) -> crate::Result<()> {
        let mut s = self.order.state.lock().unwrap();
        if s.generation != self.generation || s.failed {
            return Err(crate::StromaError::Io(
                "ordered application invalidated; recovery required".into(),
            ));
        }
        s.next = self.next;
        s.active = false;
        self.complete = true;
        drop(s);
        self.order.changed.notify_waiters();
        Ok(())
    }
}
impl Drop for Turn {
    fn drop(&mut self) {
        if !self.complete {
            poison(&self.order, self.generation);
        }
    }
}

pub(crate) async fn enter(
    scope: &Option<Scope>,
    first: u64,
    count: u64,
) -> crate::Result<Option<Turn>> {
    match scope {
        Some(s) => s.enter(first, count).await.map(Some),
        None => Ok(None),
    }
}
pub(crate) fn finish(turn: Option<Turn>, scope: Option<Scope>) -> crate::Result<()> {
    if let Some(t) = turn {
        t.complete()?;
    }
    if let Some(s) = scope {
        s.complete();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn checkpoint_fences_application_and_distinguishes_event_zero() {
        let order = OrderedApply::new(0);
        let permit = order.checkpoint().await.unwrap();
        assert_eq!(permit.event_next().unwrap(), 0);
        let scope = order.scope();
        let mut enter = Box::pin(scope.enter(0, 1));
        assert!(futures::poll!(&mut enter).is_pending());
        drop(permit);
        let turn = enter.await.unwrap();
        let mut capture = Box::pin(order.checkpoint());
        assert!(futures::poll!(&mut capture).is_pending());
        finish(Some(turn), Some(scope)).unwrap();
        assert_eq!(capture.await.unwrap().event_next().unwrap(), 1);
    }

    #[tokio::test]
    async fn cancelled_capture_releases_fence_but_stale_capture_cannot_release_new_turn() {
        let order = OrderedApply::new(0);
        let old = order.checkpoint().await.unwrap();
        order.reset(10);
        let scope = order.scope();
        let turn = scope.enter(10, 1).await.unwrap();
        assert!(old.event_next().is_err());
        drop(old);
        let mut capture = Box::pin(order.checkpoint());
        assert!(futures::poll!(&mut capture).is_pending());
        finish(Some(turn), Some(scope)).unwrap();
        drop(capture.await.unwrap());
        assert!(!order.failed());
        assert_eq!(order.applied_next().unwrap(), 11);
    }

    #[tokio::test]
    async fn interruption_invalidates_queued_capture() {
        let order = OrderedApply::new(0);
        let operation = order.scope();
        let permit = order.checkpoint().await.unwrap();
        drop(operation);
        assert!(permit.event_next().is_err());
        drop(permit);
        assert!(order.checkpoint().await.is_err());
    }
    #[tokio::test]
    async fn waits_for_contiguous_predecessor_and_fails_on_abandonment() {
        let order = OrderedApply::new(10);
        let later = order.scope();
        let mut wait = Box::pin(later.enter(12, 1));
        assert!(futures::poll!(&mut wait).is_pending());
        let first = order.scope();
        finish(Some(first.enter(10, 2).await.unwrap()), Some(first)).unwrap();
        let turn = wait.await.unwrap();
        drop(turn);
        assert!(order.scope().enter(13, 1).await.is_err());
    }
    #[tokio::test]
    async fn cancelled_operation_wakes_waiter_and_old_generation_cannot_poison_reset() {
        let order = OrderedApply::new(0);
        let first = order.scope();
        let later = order.scope();
        let mut wait = Box::pin(later.enter(1, 1));
        assert!(futures::poll!(&mut wait).is_pending());
        drop(first);
        assert!(wait.await.is_err());
        order.reset(4);
        drop(later);
        let current = order.scope();
        finish(Some(current.enter(4, 1).await.unwrap()), Some(current)).unwrap();
    }
}
