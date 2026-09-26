//! Ordered durable application. Disk appends remain concurrent.
//! Abandoning an admitted operation poisons the sequence until recovery.
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
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
    waiters: BTreeMap<u64, Vec<Arc<Notify>>>,
}

impl OrderedApply {
    pub(crate) fn new(next: u64) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                generation: 0,
                next,
                active: false,
                failed: false,
                waiters: BTreeMap::new(),
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
        let waiters = std::mem::take(&mut s.waiters)
            .into_values()
            .flatten()
            .collect();
        drop(s);
        wake(waiters);
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
        let waiters = if s.generation == self.generation {
            s.active = false;
            ready_waiters(&mut s)
        } else {
            Vec::new()
        };
        drop(s);
        wake(waiters);
        self.order.changed.notify_waiters();
    }
}

// Wake the next range and stale duplicate ranges, not every future operation.
// Checkpoint waiters use the separate boundary notification above.
fn ready_waiters(s: &mut State) -> Vec<Arc<Notify>> {
    let mut ready = Vec::new();
    while s
        .waiters
        .first_key_value()
        .is_some_and(|(first, _)| *first <= s.next)
    {
        ready.extend(s.waiters.pop_first().unwrap().1);
    }
    ready
}
fn wake(waiters: Vec<Arc<Notify>>) {
    for waiter in waiters {
        waiter.notify_one();
    }
}
struct WaitRegistration {
    order: Arc<OrderedApply>,
    first: u64,
    changed: Arc<Notify>,
}
impl Drop for WaitRegistration {
    fn drop(&mut self) {
        let mut s = self.order.state.lock().unwrap();
        if let Some(waiters) = s.waiters.get_mut(&self.first) {
            // Identity matters across reset and re-registration at the same offset.
            waiters.retain(|n| !Arc::ptr_eq(n, &self.changed));
            if waiters.is_empty() {
                s.waiters.remove(&self.first);
            }
        }
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
            let registration = {
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
                let changed = Arc::new(Notify::new());
                s.waiters.entry(first).or_default().push(changed.clone());
                WaitRegistration {
                    order: self.order.clone(),
                    first,
                    changed,
                }
            };
            // Each registration has one waiter. notify_one stores a permit if
            // completion races the first poll, avoiding a lost wake-up.
            registration.changed.notified().await;
        }
    }
    pub(crate) fn complete(mut self) {
        self.complete = true;
    }
}
fn poison(order: &OrderedApply, generation: u64) {
    let mut s = order.state.lock().unwrap();
    if s.generation != generation {
        return;
    }
    s.failed = true;
    let waiters = std::mem::take(&mut s.waiters)
        .into_values()
        .flatten()
        .collect();
    drop(s);
    wake(waiters);
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
        let waiters = ready_waiters(&mut s);
        drop(s);
        wake(waiters);
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
    use std::{
        future::Future,
        sync::atomic::{AtomicUsize, Ordering},
        task::Context,
    };

    #[derive(Default)]
    struct WakeCount(AtomicUsize);
    impl futures::task::ArcWake for WakeCount {
        fn wake_by_ref(arc: &Arc<Self>) {
            arc.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn progress_wakes_only_the_next_range() {
        let order = OrderedApply::new(0);
        let scopes: Vec<_> = (0..1024).map(|_| order.scope()).collect();
        let mut pending: Vec<_> = scopes
            .iter()
            .enumerate()
            .map(|(i, s)| Box::pin(s.enter(i as u64 + 1, 1)))
            .collect();
        let counts: Vec<_> = (0..1024).map(|_| Arc::new(WakeCount::default())).collect();
        for (future, count) in pending.iter_mut().zip(&counts) {
            let waker = futures::task::waker_ref(count);
            assert!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
        }
        let first = order.scope();
        finish(Some(first.enter(0, 1).await.unwrap()), Some(first)).unwrap();
        assert_eq!(counts[0].0.load(Ordering::Relaxed), 1);
        assert!(counts[1..].iter().all(|c| c.0.load(Ordering::Relaxed) == 0));
        drop(pending);
        assert!(order.state.lock().unwrap().waiters.is_empty());
    }

    #[tokio::test]
    async fn stale_duplicates_wake_and_reset_cleans_all_registrations() {
        let order = OrderedApply::new(0);
        let first = order.scope();
        let turn = first.enter(0, 1).await.unwrap();
        let duplicate = order.scope();
        let later = order.scope();
        let mut old = Box::pin(duplicate.enter(0, 1));
        let mut future = Box::pin(later.enter(10, 1));
        assert!(futures::poll!(&mut old).is_pending());
        assert!(futures::poll!(&mut future).is_pending());
        finish(Some(turn), Some(first)).unwrap();
        assert!(old.await.is_err());
        order.reset(10);
        assert!(future.await.is_err());
        assert!(order.state.lock().unwrap().waiters.is_empty());
        drop(duplicate);
        drop(later);
        let current = order.scope();
        finish(Some(current.enter(10, 1).await.unwrap()), Some(current)).unwrap();
    }

    #[tokio::test]
    async fn cancelled_wait_unregisters_and_checkpoint_release_wakes_successor() {
        let order = OrderedApply::new(0);
        let checkpoint = order.checkpoint().await.unwrap();
        let scope = order.scope();
        let mut wait = Box::pin(scope.enter(0, 1));
        assert!(futures::poll!(&mut wait).is_pending());
        drop(wait);
        assert!(order.state.lock().unwrap().waiters.is_empty());
        let mut wait = Box::pin(scope.enter(0, 1));
        assert!(futures::poll!(&mut wait).is_pending());
        drop(checkpoint);
        let turn = wait.await.unwrap();
        finish(Some(turn), Some(scope)).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_ranges_make_contiguous_progress_after_checkpoint_release() {
        let order = OrderedApply::new(0);
        let checkpoint = order.checkpoint().await.unwrap();
        let mut tasks = tokio::task::JoinSet::new();
        for offset in (0..512).rev() {
            let scope = order.scope();
            tasks.spawn(async move {
                let turn = scope.enter(offset, 1).await.unwrap();
                finish(Some(turn), Some(scope)).unwrap();
            });
        }
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            while order.state.lock().unwrap().waiters.len() != 512 {
                tokio::task::yield_now().await;
            }
            drop(checkpoint);
            while let Some(result) = tasks.join_next().await {
                result.unwrap();
            }
        })
        .await
        .unwrap();
        assert_eq!(order.applied_next().unwrap(), 512);
        assert!(order.state.lock().unwrap().waiters.is_empty());
    }

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
