# Keratin

**Durability, Layer by Layer.**

*The tough outer layer for your inner state.*

[![Rust CI](https://github.com/Axmouth/keratin/actions/workflows/rust-ci.yaml/badge.svg)](https://github.com/Axmouth/keratin/actions/workflows/rust-ci.yaml)

<img src="./bannerlogo.svg" width="240em">

Keratin is a small durability layer built around a simple append-only segmented log.

It is designed to provide reliable persistence primitives that can be reused across systems, without pulling in higher-level concerns like broker semantics or coordination.

## What it is

At its core, Keratin is:

* an **append-only log**
* split into **segments**
* focused on **durability and performance**

The goal is to keep the design minimal and predictable, while still being fast enough for practical use.

## Append completion

`KDurability` picks when an append completes:

| Mode | Completes when |
| --- | --- |
| `AfterWrite` | Local appends may complete while still staged in Keratin's buffers. Replicated appends write to the OS first. |
| `AfterFlush` | Opt-in. The batch is written through Keratin's buffers to the OS. No fsync. |
| `AfterFsync` | The append has been fsynced. This is the default. |

`AfterFlush` acknowledges after the write reaches the OS, not after fsync. An
acknowledged record survives a process crash but is not durable across power
loss or an OS crash until a later sync. It writes once per staging batch and
does not request an fsync of its own, though segment rollover or other durable
work in the same log may still sync. It does not advance the durable frontier,
so ordinary scans still see only durable records. Raw `reader().fetch(offset)`
can read the record back. Call `sync()` when persistence is needed. Setting
`AfterFlush` does not schedule periodic syncs.

A failed `AfterFlush` write fails its waiters and stops the log like any other
storage failure. Later commands are refused until the log is reopened, which
runs the recovery scan. Dropping an append's receiver does not cancel the
append.

## Live writer settings

`Keratin::writer_settings`, `replace_writer_settings` and
`update_writer_settings` read and change one open log's default durability,
batch limits, linger ceiling and fsync intervals without reopening it.

```rust
let applied = log.update_writer_settings(|next| {
    next.max_batch_bytes = 512 * 1024;
    next.max_batch_records = 2048;
}).await?;
```

A change is ordered with appends by when it enters the writer channel.
Appends before it keep the old default durability and appends after it use
the new one. Before adopting, the writer stages its open batch and fsyncs
everything written so far, so the log pauses briefly. If that sync fails the
log stops like any other storage failure and must be reopened. Changes are
checked against a revision, so a concurrent change makes the later one fail
with `WouldBlock` instead of overwriting it. Settings are not saved and reset
on reopen. A system built on Keratin that wants saved or coordinated policy
should own it and push it to each log, rather than keep a second editable copy.

## What it is not

Keratin does not attempt to be:

* a Write-Ahead Log (WAL) with complex indexing and recovery logic
* a full-fledged log-structured storage engine
* a distributed log with built-in replication and consensus
* a database
* a message broker

It is a building block, intended to sit underneath systems that need durable state. Any layer implementing more complex semantics can be built on top of it as a separate component/project/repo.

## Usage

Keratin is currently used by **Stroma** as its durability layer.

Other use cases may emerge over time as the design stabilizes and it is meant to be reusable.

## Diagnostics

[Batch tracing](experiments/BATCH_TRACING.md) captures staging, commit and fsync
overlap with optional buffered diagnostics.

## Direction

Areas that may be explored further:

* segment management and compaction
* crash recovery guarantees
* performance characteristics under load
* tooling for inspection and debugging

The focus will remain on keeping the core simple and understandable.

## Naming

Named after **keratin**, the durable material found in hair, nails, and similar structures.

The analogy is loose, but points to:

* durability
* layered growth (append-only)
* eventual shedding (compaction/cleanup)

## Status

Early and evolving. The core ideas are in place, but details and guarantees may change. Performance is considered satisfactory for intended use cases, but optimizations and improvements are likely as the design matures and more use cases are explored.

Historical design records are indexed in the [archive](archive/README.md).
