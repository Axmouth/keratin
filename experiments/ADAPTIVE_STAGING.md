# Adaptive staging buffer prototype

`keratin_log::reusable_buffer::ReusableBuffer<T>` owns a reusable vector and an
optional retention policy. Callers fill it through a scoped edit, consume and
clear the batch, then service its maintenance deadline from their existing loop.
Time is supplied by the caller, making the policy independently testable.

The Keratin prototype requires `early-replication-experiment` at build time and
`FIBRIL_EXPERIMENT_ADAPTIVE_STAGING=1` at process startup. It is independent of the
runtime early-replication switch and takes precedence over
`FIBRIL_EXPERIMENT_SMALL_STAGING`. Ordinary builds retain the existing capacities.

Startup environment controls:

- `FIBRIL_EXPERIMENT_STAGING_DECAY_SECS`: shrink check interval, default 10.
- `FIBRIL_EXPERIMENT_STAGING_IDLE_RELEASE_SECS`: full idle release delay, default
  six times the decay interval (60 seconds with defaults).

Values must be positive whole seconds, with idle release at least the decay
interval. Invalid values fail log opening when adaptive staging is enabled.
For bursts roughly a minute apart, an example experiment is decay 120 seconds
and idle release 1800 seconds. Disabling adaptive staging and enabling the small
initial-buffer switch retains grown allocations indefinitely. These controls are
process-start settings; production/runtime configuration remains an adoption task.

The prototype starts without staging allocations. Reservations have a 64 KiB
payload floor and a 4 KiB index floor, and grow immediately to fit larger batches.
There is no 16 MiB hard cap. With the defaults, every ten seconds, an empty buffer may shrink by up to
half, preserving at least twice the largest batch observed since the previous
check. Sixty seconds without use permits full release. A delayed maintenance call
performs one decay step. Nonempty buffers are never reclaimed.

The writer includes the next maintenance deadline in its existing receive wait.
There are no additional threads or tasks. Once an idle allocation is released,
its maintenance wakeups stop. Tail-cache allocations, pending fsync jobs, channel
capacities and durability frontiers are independent of this policy. Allocator
page retention means released capacity need not produce an immediate RSS drop.

Empty-buffer resizing is selectable. The generic abstraction defaults to Vec
reallocation. This broker-focused prototype selects allocation replacement,
which avoids copying unused capacity with Fibril's mimalloc allocator. A separate
system-allocator control showed higher cold refill cost with replacement, so this
choice requires allocator-specific testing when reusing the abstraction.

Tests cover demand retention, decay, idle release, burst boundaries, error returns,
large records, both resize strategies and growth with live elements. A log-level
test releases staging between file write and fsync, verifies early-cache and
ordinary durable reads, and appends another record. Adoption still requires broker
burst/steady-load comparisons, first-message tail latency after idle, and aggregate
RSS/CPU measurements with many queues. Standalone resize timings establish no
broker latency guarantee.
