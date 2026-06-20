# Stroma QueueHandle -> ticket/re-resolve redesign (execution-ready plan)

Status: scoped, NOT yet implemented. Tree is at a clean baseline (partial patches
reverted). User chose the full redesign (over interim mitigation). Full problem
analysis + rationale: fibril DESIGN_NOTES.md "Stroma queue-handle lifecycle".

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

## Prerequisite Keratin fix (independent, do first - small)
keratin.rs: shutdown() (351) sets shutdown_started then unlocks at 367, but if it
errors between, Drop (456) early-returns on shutdown_started and never unlocks ->
permanent flock leak. Fix: release the flock unconditionally (e.g. unlock before the
writer-ack await, or have Drop release the lock regardless of shutdown_started). This
alone removes the "permanent already-open" leak and is worth landing on its own.

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
