# Adaptive staging buffers

`keratin_log::reusable_buffer::ReusableBuffer<T>` owns a reusable vector and an
optional retention policy. Callers fill it through a scoped edit, consume and
clear the batch, then service its maintenance deadline from their existing loop.
Time is supplied by the caller, making the policy independently testable.

## Configuration

Ordinary builds support `KeratinConfig::adaptive_staging`. `None` (the default)
preserves eager 16 MiB write and 256 KiB sparse-index reservations per log.
`Some(AdaptiveStagingConfig::default())` enables lazy staging with 64 KiB write
and 4 KiB index reservation floors, a 10-second decay interval and 60-second idle
release. Both message and event logs can use this policy. Standalone callers can
configure the floors, durations and empty-buffer resize strategy independently.
No replication experiment feature or process-global environment variable is needed.

Capacities must be positive and representable; decay must be positive and idle
release must be at least as long as decay and representable on the monotonic clock.
Invalid settings fail before opening or creating a log. The policy is local
startup configuration and does not change the persisted format.

## Retention

Buffers start without staging allocations and grow immediately to fit larger
batches. Neither the adaptive floors nor the retained 16 MiB reservation is a
hard cap. At each decay check an empty buffer may shrink by up to half, preserving
at least twice the largest batch observed since the previous check. The idle
release delay permits full release. Delayed maintenance performs one decay step;
nonempty buffers are never reclaimed, and maintenance never increases capacity.

The writer includes the maintenance deadline in its existing receive wait. There
are no additional threads or tasks; wakeups stop once idle allocations are released.
Tail caches, pending fsync jobs, channel capacities and durability frontiers are
independent. Released vector capacity need not produce an immediate RSS drop:
allocator retention, allocation layout and OS page accounting also matter.

The generic abstraction and standalone Keratin default to Vec reallocation.
Fibril selects replacement for empty allocations to avoid copying unused capacity
with its mimalloc allocator. A system-allocator control showed higher cold refill
cost with replacement, so the strategy remains selectable in Keratin.

## Validation and tradeoffs

Tests cover demand retention, decay, idle release, burst boundaries, error returns,
large records, both resize strategies and growth with live elements. A log test
opens with the policy, releases staging between file write and fsync, verifies
cache and disk reads remain gated by durability, and appends another record.
Writer tests exercise durable completion and reopening with the retained policy.

A Linux broker screen covered steady rates, saturation, replicated RPC and
many-queue bursts. Latency generally remained comparable and several steady-load
cases used less anonymous memory; the replicated follow-up used more CPU, and the
large-payload burst case used more RSS despite releasing vector capacity. The
policy remains opt-in. Capacity measurements alongside allocator/OS accounting
are needed to explain the burst result; allocator retention alone is unproven.
