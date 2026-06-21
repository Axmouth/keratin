# Stroma QueueHandle -> ticket/re-resolve redesign (execution-ready plan)

Status: IMPLEMENTED + green (2026-06-20). Full keratin workspace 285 passed; fibril
builds across the boundary; mouse 5/5 + bear 3/3 deterministic. Bench below.
Full problem analysis + rationale: fibril DESIGN_NOTES.md "Stroma queue-handle lifecycle".

================================================================================
## IMPLEMENTED: shape, flow diagrams, lessons, bench (2026-06-20)

### Ownership / resolution shape
```
              registry: Arc<ArcSwap<Registry>>
                       |
   key -> QueueSlot { handle: OnceCell<Arc<QueueHandleInner>> }   <- SOLE strong owner
                                   ^   ^
                                   |   | Arc::downgrade  (Weak, no pin)
   QueueHandle (ticket) { registry, key, Weak<Inner> } -----------+   cheap to clone, 'static
        |  .resolve()  (Weak::upgrade, else registry re-lookup by key)
        v
   Resolved<'a>   strong Arc<Inner>, but lifetime-bound to the ticket (PhantomData<&'a>)
        |  Deref                       -> cannot escape into a 'static task / longer-lived field
        v
   QueueHandleInner   the log-owning state (msg_log, event_log, command tx, atomics)
```
The control task + periodic_snapshot task hold the TICKET (Weak), so they never pin.
The command tx lives in Inner, so when the slot drops Inner the control loop's rx
closes and it exits on its own.

### Flow in each case
- A. Normal op: `resolve()` -> Weak.upgrade OK -> Resolved -> use -> drop. ~4 ns.
- B. Incarnation rotated (destroy+recreate, same key): upgrade FAILS -> registry
  re-lookup by key -> NEW slot's Inner -> Resolved. Transparent failover to the live one.
- C. Partition gone (evicted/destroyed, no live incarnation): upgrade FAILS ->
  registry lookup finds no slot / empty handle -> Err(ActorGone). No pin, clean error.
- D. Orphaned background task: holds the ticket (Weak); per-tick `resolve()` -> Err ->
  task self-exits. Cannot keep logs open / hold the flock. (Kills the original leak.)
- E. The guard (by construction):
    let g = ticket.resolve()?;            // Resolved<'a> borrows the ticket
    tokio::spawn(async move { g ... });   // COMPILE ERROR: g is not 'static
  => you MUST move the ticket (Weak) into the task and resolve inside -> impossible to
  park a strong handle somewhere and pin the logs forever.

### Lessons learned (good / bad in practice)
GOOD:
- The lifetime-bound `Resolved` caught real misuse at COMPILE time: the displaced-handle
  test literally could not move a resolved handle into a thread - exactly the leak we
  set out to forbid. The friction == the guarantee. Cross-thread/`'static` use must go
  through the ticket and re-resolve.
- Shadowing `let qh = qh.resolve()?;` made most call sites a ONE-LINE change; method
  bodies were untouched because `Resolved`/`Arc<Inner>` Deref to the full API.
- Identity (topic/partition/group) is served from the ticket's key with NO resolve, so
  logging/routing/cache-key uses stay zero-cost.
- The ticket is CHEAPER to hand out than the old handle: 1 registry Arc clone + 2
  Box<str> + 1 Weak downgrade, vs the old clone-bundle of ~25 Arc clones + 3 String clones.

BAD / friction (gotchas worth knowing):
- `x.queue_handle(...).await.unwrap().resolve()` does NOT compile: queue_handle returns
  a TEMPORARY ticket and `Resolved` cannot outlive it. MUST bind first:
  `let t = ...await?; let h = t.resolve()?;`. This bit every chained call site (and the
  first sed pass, which produced exactly this and had to be re-split into two lines).
- Deref coercion does NOT fire through `&expr?`: `f(&t.resolve()?)` fails with
  "expected QueueHandleInner, found Resolved" because the expected type is pushed inward
  past the `&`. A named binding `let h = t.resolve()?; f(&h)` coerces fine.
- Tasks/closures that need a handle must hold the TICKET and resolve INSIDE (cannot
  pre-resolve and move the guard in) - more verbose, but it is the correct shape.
- A function that passes a handle into a spawned task must take the ticket
  (`&QueueHandle`); a leaf helper that operates synchronously takes `&QueueHandleInner`
  and callers deref-coerce from `Resolved`/`Arc`. Choosing per function was the main
  judgement call of the whole change.
- `gen` is a reserved keyword in Rust 2024 - the Weak field is named `incarnation`.
- Global regex edits (perl `s///` without /g, multi-similar lines) leaked into the wrong
  function twice. Per-site Edits with unique context were safer. (User flagged this.)

### Bench (cargo test --release ticket_resolve_overhead_bench -- --ignored --nocapture)
- resolve(): ~3.9 ns/op (one Weak::upgrade). Hot paths (append/ack batch, poll) resolve
  ONCE per batch/op scope and reuse -> amortized ~0 per record.
- queue_handle(): ~50.8 ns/op (ArcSwap load + raw_entry lookup + key alloc + ticket
  build) - same order as / cheaper than the old clone-bundle.
- No measurable regression; full-suite runtime unchanged.
================================================================================

## Why (one line)
`QueueHandle` is a Clone bundle-of-Arcs that holds the Keratin logs strong, so any
clone (returned handle + periodic_snapshot task) pins the flock; destroy can orphan/
leak it. Fix: the log-owning state lives ONLY in the registry slot; the handed-out
handle is a ticket that re-resolves per op, so it can never pin a dead incarnation.

## Scope (measured)
- Contained to stroma-core: state.rs (QueueHandle def @978, 84 refs) + stroma.rs
  (31 refs, 40 msg_log()/event_log() calls) + replication.rs (38 calls) +
  tests/roles.rs. NOT used by broker/fibril/ganglion - the public Stroma API does not
  expose QueueHandle ops.
- msg_log()/event_log() already return OWNED Arc<Keratin> (state.rs:2507/2511), so
  resolution happens inside the accessor; only 2 internal self.msg_log/self.event_log
  field uses in state.rs.

## Design
1. Split: `QueueHandleInner` = today's QueueHandle fields (command_sender, msg_log,
   event_log, applied_upto, atomics, notifies, ...). The registry SLOT owns the strong
   `Arc<QueueHandleInner>` (QueueSlot.handle: OnceCell<Arc<QueueHandleInner>>).
2. `QueueHandle` (handed out) = a TICKET: { registry: Arc<ArcSwap<Registry>>, key:
   (Box<str>,u32,Option<Box<str>>), gen: Weak<QueueHandleInner> }. Cheap to clone.
3. Resolution: `QueueHandle::resolve(&self) -> Result<Arc<QueueHandleInner>, StromaError>`
   = upgrade the Weak; on failure re-look-up the slot by key in the registry. If the
   key is gone -> Err(QueueActorGone-style). This is the ONLY fallible step; the
   resolved Arc<QueueHandleInner> has infallible accessors (msg_log() etc. as today).
   Offer both `resolve()` (ergonomic) and `with(|inner| ...)` (borrow can't escape).
4. periodic_snapshot + any long-lived task hold the TICKET (or a Weak), never a strong
   Inner -> they don't pin. The snapshot task resolves per tick; if gone, it exits.
5. Stroma ops (stroma.rs / replication.rs) that take `qh: &QueueHandle` and call
   qh.msg_log() N times: change to resolve ONCE at the top (`let h = qh.resolve()?;`)
   then use h.msg_log() (infallible). ~20-30 methods.
6. destroy_partition / evict: drop the slot's strong Inner -> Inner's Drop closes logs
   -> flock released. Keep an explicit shutdown for promptness, BUT first fix the
   Keratin shutdown/Drop lock-leak (below) so Drop reliably releases even if shutdown
   was partial.

## Prerequisite Keratin fix - DROPPED (2026-06-20): it is moot
Re-examined: `_lock: Option<File>` uses fs2 (BSD flock), which the kernel releases when
the fd closes. Even if shutdown() sets shutdown_started then errors before its explicit
unlock() (keratin.rs:367), and Drop early-returns on shutdown_started (:458), the struct's
`_lock` field still drops AFTER Drop::drop returns -> fd closes -> flock released. So there
is NO permanent flock leak from the shutdown/Drop path; the explicit unlock is only for
promptness. The flock can only stay held forever if some Arc<Keratin> never drops - i.e.
the ORPHAN-PIN (a leaked periodic_snapshot/control task holding a QueueHandle clone for a
dead incarnation). That is not a Keratin bug; it is exactly what this ticket redesign
fixes. So no Keratin change is needed (and the user wants that layer left alone).

NB the per-key lifecycle mutex (last fix, keratin 7dd6c18) is UNRELATED to this: it fixed
the concurrent open/close RACE (two builds opening the same dir), not handle pinning.

## Chosen approach (2026-06-20, user): FULL ticket conversion, one focused pass
Not the beachhead-commit nor the snapshot-only fix - the end state directly:
QueueHandle = ticket {registry, key, Weak<Inner>}; Inner lives only in the slot; resolve()
/ with(); convert ~84 refs; bench. Compile incrementally, commit once green.
KEY CYCLE-BREAK: today BOTH the "queue control" task (state.rs:1086) AND periodic_snapshot
hold a strong QueueHandle clone -> both pin. The control task owns `rx`; since the command
tx lives in Inner, the control task can hold a Weak (not strong): when the slot drops Inner,
tx drops -> rx closes -> recv()->None -> loop exits cleanly, no pin. Snapshot task holds the
ticket/Weak and resolves per tick, exits when gone. Slot is the SOLE strong owner.

## Verification
Re-add the graduated trip-wires (one parameterized helper, two entry points):
- mouse: ~8 rounds x 24 tasks (8 destroy / 8 materialize / 8 queue_handle readers
  that must never spuriously fail), 4 worker threads. Reproduced the error in round 0.
- bear: ~40 x 96, 8 threads. Also exposed the hang.
Run each many times; both must be deterministically green. Then full `cargo test
-p stroma-core` + rebuild fibril across the boundary.

## Order of work (each step compiles)
1. Keratin flock-release fix + its own test. (independent, commit)
2. Introduce QueueHandleInner = current fields; make QueueHandle = thin Arc<Inner>
   wrapper that Derefs (NO behavior change yet, just the indirection). Build green.
3. Move the strong Arc<Inner> into the slot; make QueueHandle hold Weak+registry+key;
   add resolve(); convert the handful of accessors + the ~20-30 op methods to resolve.
   Build green. (this is the big step - no sub-checkpoint)
4. Re-add mouse+bear tests; iterate to deterministic green.

## Refinement (the concrete permanent-pin culprit)
Keratin's flock is released when the last Arc<Keratin> drops (the _lock File field
closes its fd on drop, even via Drop's early-return) OR earlier via shutdown()'s
explicit unlock. So a leak is only PERMANENT while some Arc<Keratin> stays alive
forever. That holder is the ORPHANED periodic_snapshot task: when destroy misses an
in-flight build (the Dekker check is not airtight), nobody calls cancel_background_
tasks on the resulting handle, so its snapshot task runs forever holding the handle
(and flock). => The single highest-value change is making periodic_snapshot NOT pin:
hold a ticket/Weak, resolve per tick, and exit when the incarnation is gone. That can
land as an early, mostly-isolated step of the redesign (the snapshot task already
takes qh.clone(); give it a weak/ticket + a per-tick resolve), and it removes the
permanent leak even before the full ticket conversion of the op methods.

## Crux (2026-06-20, user): no in-memory serialization of the Keratin lifecycle
The persistent "Keratin already open" is NOT fixed by the ticket redesign (that fixes
the orphan-PINNING leak) nor by retry/event-driven cleanup alone. Root cause: the
filesystem flock on `<dir>/.keratin.lock` is the ONLY coordination between Keratin
opens, and it is advisory + fail-fast - it cannot WAIT for or coordinate with an
in-flight open/close. The in-memory registry (slot + get_or_try_init + tombstone)
serializes the HANDLE but NOT the underlying Keratin open/close lifecycle across the
destroy -> recreate -> failed-build-retry boundary. So a build's `open` collides with
a prior incarnation's not-yet-finished release and just errors.

FIX (targeted, likely smaller than the full ticket refactor; do this for the bug):
Add an explicit per-partition-key LIFECYCLE MUTEX in Stroma (e.g.
DashMap<Key, Arc<tokio::sync::Mutex<()>>>). Acquire it around the operations that
open/close a partition's Keratin: queue_handle's BUILD (cold path only - the
materialized fast path skips it), destroy_partition, and evict. With it held, the
prior incarnation's Keratin is fully shut down + unlocked (and the dir renamed)
before the next build opens -> no flock collision, deterministically. The flock
becomes a redundant safety net.
  Watch out: avoid reentrancy deadlock - the build path (recover_one_log_with_handle,
  periodic_snapshot -> write_snapshots_for_partition -> queue_handle) must not
  re-acquire the same key's lifecycle mutex while held. Either hold the lock only
  around open/close (not recovery) with care, or make recovery not re-enter
  queue_handle. The ticket redesign + this lifecycle lock are complementary:
  lifecycle lock kills the open/close race; the ticket kills the orphan-pin leak.

## Experiments tried (2026-06-20) - captured before reverting for a clean trace
All UNCOMMITTED and reverted to baseline for clean observation. None fully greened
the mouse test; the failure stayed "Keratin already open" with a flock apparently
held on the SAME inode for >1s (a lingering holder I could not derive by reasoning -
hence the decision to trace). Keep these ideas; they are likely part of the final fix:
  1. Event-driven orphan cleanup: a Stroma-level `queue_retired: Arc<Notify>` fired
     on destroy + evict; the periodic_snapshot loop pre-arms `queue_retired.notified()`
     and `select!`s on it (+ background_tasks cancel + ticker), and a
     `handle_is_current_incarnation(qh)` check (compare the registry slot's live handle
     msg_log Arc::ptr_eq vs qh's) so an orphaned snapshot task exits promptly instead
     of pinning logs. (Replaces a coarse 10s poll - event-driven, no hardcoded wait.)
  2. shutdown-on-build-failure: in queue_handle's build closure, if event_log_init or
     recovery fails AFTER msg_log opened, `shutdown().await` the opened log(s) (release
     the flock synchronously) before returning Err, instead of a lazy Drop. Also moved
     periodic_snapshot to AFTER recovery so a failed build never spawns an orphan
     snapshot task.
  3. Bounded Stroma-level retry: queue_handle retries the build on transient error
     (re-acquire slot, short sleep), bounded. (Made the test SLOW (8-16s) without
     greening it - which is itself a clue: the holder persists >1s.)
  4. (earlier, reverted) Dekker begin_init/wait_init_done serialization (not airtight -
     registry CAS is ArcSwap Acquire/AcqRel, not SeqCst) and queue_handle self-clean.
LIKELY FINAL FIX: per-key lifecycle mutex (serialize build/destroy/evict open+close)
+ keep (1) and (2). But TRACE FIRST to confirm the actual >1s holder before building it.

## RESOLVED (2026-06-20): per-key lifecycle mutex
Tracing (eprintln on Keratin open/lock/fail/unlock/drop + the destroy rename) finally
showed the mechanism: after a destroy renames the dir, TWO builds run concurrently and
both `create_dir_all` + open the SAME path - one locks the fresh inode, the other hits
"Keratin already open". `msg_log_init` recreates+opens the dir regardless of slot state,
so a build whose slot was retired mid-flight (or a churning second build) collides.
i.e. exactly the missing in-memory serialization.

FIX (landed): a per-partition-key lifecycle mutex (`Stroma.lifecycle_locks:
DashMap<key, Arc<tokio::Mutex<()>>>`). queue_handle's BUILD slow-path, destroy_partition,
and evict acquire it around the open/close, so no two ever race on the same dir. Notes:
  - HOT PATH UNTOUCHED: queue_handle's fast path (materialized handle) returns before
    taking the lock; only the cold build/destroy/evict paths lock.
  - Reentrancy avoided: recover_one_log_with_handle does not re-enter queue_handle;
    evict takes the lock AFTER its pre-swap snapshot (that snapshot calls queue_handle).
  - The flock stays as a redundant safety net.
  - Result: mouse 10/10 and bear 3/3 deterministically green (the bear DEADLOCK is gone
    too - it was a cascade of the same concurrent-open chaos); mouse ~0.2s (no slow
    retrying). Full stroma-core suite green.
This was the targeted fix; the broader ticket/re-resolve redesign (orphan-pin leak) and
the per-key lifecycle-mutex unbounded-growth pruning remain as separate future items.

## Scenario assessment (2026-06-20): completion fires after eviction/destroy
User asked: with non-pinning tickets, what happens if a durable append's completion
fires after the partition was evicted/destroyed - should the handle re-materialize?
Decision: DON'T add rematerialization now (deferred follow-up).

WHAT HAPPENS TODAY (pre-ticket), traced from the actual code:
  - evict (stroma.rs:1483) and destroy_partition (:1574) guard ONLY on
    `qh.inflight_len() > 0` (leased, un-acked DELIVERIES). They do NOT check
    active_owner_operations / drain the OwnerOperationLease. An append that is
    durable-but-not-yet-applied holds a lease (begin_owner_operation :2108) but is NOT in
    the inflight delivery set, so neither teardown path waits for it.
  - Both then FORCE-shutdown: qh.cancel_background_tasks(); qh.shutdown();
    qh.event_log().shutdown(); qh.msg_log().shutdown() - releasing the flock and stopping
    the control task REGARDLESS of any strong handle clones a completion holds.
  - => A completion racing evict/destroy ALREADY fails the client today: the event-log
    append hits a shut-down log, or enqueue_event_inmem sends to a dead control task
    (channel closed), even though the msg append was durable. The completion's strong
    handle does NOT protect it; the explicit shutdown already released the flock. The
    strong handle's only real effect was the ORPHAN-PIN leak (an un-cancelled snapshot
    task holding a handle whose logs were never shut down -> flock held forever).

CORRECTION of an earlier wrong note: destroy does NOT drain owner operations.
freeze_owner_and_wait_operations() is only in freeze_queue_for_transition (:1688), the
graceful pre-transition path - not in evict/destroy.

IMPLICATION FOR THE TICKET CHANGE: it does NOT regress this scenario (already
unprotected). If anything resolve()'s re-lookup gives the eviction case (disk intact, a
recovered current incarnation present) a better outcome than the old strong clone, which
just operated on a dead Inner.

THE TEARDOWN CONTRACT (why no change is needed): evict/destroy are only safe on a
QUIESCED partition, and that is the CALLER's precondition:
  1. Quiesce owner ops: caller runs the freeze path (freeze_queue_for_transition ->
     qh.freeze_owner_and_wait_operations()) which admits no new owner op and waits
     active_owner_operations -> 0, then freezes the logs. After it returns no append /
     in-flight append completion can exist or begin.
  2. No leased deliveries: caller drains consumers; evict/destroy also self-guard with
     inflight_len()==0 (-> HasInflight) as a backstop.
Under (1)+(2) the force-shutdown is safe: nothing is in flight to race, so no completion
is stranded and no client is falsely errored. destroy_partition already DOCUMENTS this
("already deregistered, drained, no longer routed"); evict rests on only evicting idle
partitions + the inflight_len backstop. The ticket change does not touch this contract -
completions still finish before freeze returns - it only removes the orphan-pin leak and
degrades more gracefully (ActorGone, no flock leak) if the contract is ever violated.
OPTIONAL belt-and-suspenders (not required): make evict + destroy_partition themselves
freeze_owner_and_wait_operations() before shutdown, so the contract is self-enforced
rather than caller-enforced. Low priority.

If we ever DO add rematerialization instead (eviction-only, disk-intact): resolve_or_
materialize(ticket) = resolve() else queue_handle(tp,part,group) (recovers from disk iff
the dir exists); route the sync ApplyThenComplete path through the runtime spawn the msg
path already uses. Do NOT resurrect destroyed (dir-gone) partitions.

================================================================================
## RESUME HERE (post-compaction, 2026-06-20): start the ticket/re-resolve redesign

CURRENT STATE (all committed, tree green):
  - The concurrent destroy/create race is FIXED (keratin 7dd6c18) via the per-key
    lifecycle mutex (Stroma.lifecycle_locks, cold paths only: queue_handle build /
    destroy_partition / evict; hot path untouched; flock kept as safety net).
  - Tests: mouse 10/10 + bear 3/3 deterministic; full keratin workspace 285 passed
    (exit 0); fibril builds across the boundary. fibril docs updated (048caf1).

NEXT TASK = the ticket/re-resolve redesign (the SEPARATE, lower-urgency architectural
item: handles must not PIN logs; the lifecycle mutex did NOT address this, it fixed
the open/close race). Plan + design are in the sections above ("PROPER FIX",
"## Crux", "## Experiments tried"). Summary to execute:
  - Split QueueHandle (state.rs:978, Clone bundle-of-Arcs, ~84 refs there + ~31 in
    stroma.rs + replication.rs + tests/roles.rs) into a TICKET {Arc<ArcSwap<Registry>>
    + key + Weak<Inner>} and an Inner that lives ONLY in the registry slot.
  - Resolve per op: `resolve() -> Option<Inner>` (Weak upgrade, ergonomic) AND a
    `with(|inner| ...)` closure form. Ops re-resolve; a stale ticket -> current
    incarnation or "gone", never pins dead logs.
  - Contained to stroma-core (broker/fibril/ganglion never touch QueueHandle).
  - NO clean partial-compile checkpoint for the struct split -> needs one focused pass
    with budget headroom. Optional beachhead first: QueueHandle(Arc<QueueHandleInner>)
    + Deref (pure indirection, compiles green) to de-risk, then the resolve conversion.

PERFORMANCE (must verify; user flagged - expected fine but check):
  - Per-resolve cost = one ArcSwap::load (lock-free) + one hashbrown lookup. Cheap, but
    "per op" on hot paths (append/ack) adds up.
  - MITIGATION: resolve ONCE per operation/batch scope (resolve at the top of an
    append/ack batch, reuse Inner within it) - not per record.
  - BENCH: append + ack throughput before/after the conversion; confirm no regression.

FUTURE ITEM (low priority): lifecycle_locks map pruning. CORRECTED estimate (the prior
"~10MB at 1M keys" was ~10-15x too low): each entry is a DashMap slot (~55B) + an
Arc<tokio::sync::Mutex<()>> heap alloc (~55-80B; tokio Mutex wraps a batch-semaphore,
not a bare futex) + the key's Box<str> topic/group heap (~30B) ~= ~150-200B/entry ->
~150-200MB at 1M DISTINCT keys. Lookup speed is NOT a concern: hashbrown/DashMap stays
O(1) as it grows (resizes to keep load factor); size costs memory, not speed, and
DashMap contention is about concurrent-access spread across shards, not entry count. (Shard contention is set by the shard count (~4x ncpu) + the hash spread of keys ACCESSED CONCURRENTLY, not by total stored entries; 1M entries just means more entries per shard, still O(1) and zero added contention. And lifecycle_locks is touched only on COLD paths - build/destroy/evict - never hot publish/consume - so access frequency is low regardless.)
The map only grows per DISTINCT (topic,part,group) EVER seen - recreating the same
partitions reuses keys (bounded). So the only real exposure is unbounded distinct-key
churn (e.g. ever-growing unique topic names); for normal workloads it is trivial. Prune
a key's entry on destroy when no waiter holds it, IF it ever shows up. Not urgent.
  BENCH TODO (do when this is picked up): a minimal benchmark that populates
  lifecycle_locks with millions of distinct keys (e.g. 1M / 5M / 10M) and measures (a)
  actual process memory delta (RSS) vs the ~150-200B/entry estimate, and (b) lookup +
  acquire latency at that size vs near-empty (to confirm O(1) - speed should be flat).
  An #[ignore]'d test or a small criterion bench is enough. The numbers decide whether
  pruning is ever worth it.
================================================================================
