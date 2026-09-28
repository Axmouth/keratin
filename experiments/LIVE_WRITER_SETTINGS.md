# Live per-log writer settings

The experimental handle API applies writer policy through the same FIFO as local
and replicated appends. It adds no shared configuration lookup to the append hot
path. The writer owns an ordinary local configuration snapshot.

```rust
let current = log.writer_settings().await?;
let mut next = current.settings;
next.max_batch_bytes = 512 * 1024;
next.max_batch_records = 2048;
let applied = log.replace_writer_settings(current.revision, next).await?;
```

`WriterSettings` currently covers default durability, batch byte and record
limits, adaptive linger ceiling, fsync ceiling and minimum fsync interval. An
explicit append durability overrides the default. Zero linger disables adaptive
linger. Positive batch limits still allow one oversized request to progress.

## Adoption boundary

An update is ordered when its command enters the writer channel. Concurrent
producers have channel admission order, not wall-clock call order. Requests
before the update retain the previous default. Requests after it use the new
one. An accepted update closes the current batch using its old configuration,
drains outstanding sync work, and completes a fresh persistence barrier before
publishing its new revision. The fsync worker performs that barrier and the
writer continues draining its completion channel, avoiding a full-channel
send/wait cycle.

This conservative first version briefly pauses this log's writer during the
update. It never extends a pending receipt's timeout with a new interval. A new
timing period starts once the old persistence work finishes. Future size-only
updates could change an active batch without this barrier. That optimization is
not implemented here.

The applied response covers the old log prefix's persistence, not necessarily
delivery of every old completion callback through the notifier. Physical sync
and completion/manifest failures reject adoption. The writer then rejects
subsequent requests instead of continuing with uncertain configuration/storage
state. Validation and stale-revision errors leave the writer usable and do not
perform an update barrier.

Calls yield when the mailbox is full. Cancellation after submission can still
admit or apply an update. A read can overtake an update sender that has not yet
entered the channel. Reconcile with the expected revision and retry using CAS
rather than treating one read after cancellation as proof that no update can
still arrive. A dropped reply to an admitted command never retracts that command.

## Scope and persistence

Revisions belong to one open handle incarnation. These updates are not saved to
disk and reset to the startup policy on reopen. A durable controller should own
saved policy and incarnation identity, then report per-log adoption. Do not add a
second independently editable copy of the same setting in Fibril.

Existing shared `LogRuntimeSettings` continues to own segment preallocation and
its open/rollover adoption status. Channel capacity, fsync pipeline capacity,
index stride, segment sizing and staging-buffer allocation are outside this
writer-settings API. The separate AfterFlush periodic-sync probe's interval is
also unchanged by this API. Its scheduler adoption/wakeup needs a later slice.

Changing one Keratin log's default is not a coordinated Fibril queue policy
change. Fibril explicitly requests durability in additional paths. Its written
versus durable replication progress, consumer visibility, checkpoint safety,
crash recovery, saved settings and dashboard controls still need integration.
A warning does not replace those changes. Control metadata must retain its own
durability policy when queue-data confirmation is weakened.

## Validation

Tests exercise a weak-to-durable upgrade including offset zero, explicit
per-append overrides, follower default changes, stale and invalid revisions,
a lost response, a downgrade behind a held physical fsync, an injected fsync
failure, and a full mailbox on a single-thread async runtime. The full Keratin
unit suite and the trace-enabled focused tests are run for this experiment.
