# Changelog

All notable changes to the Keratin repo (the Keratin append-only log store and
the Stroma queue and stream state layer built on it) are recorded here. Fibril
consumes these crates and records only the user-visible effects in its own
changelog.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project follows [Semantic Versioning](https://semver.org/). There are no
tagged releases yet; earlier history predates this changelog.

## [Unreleased]

### Added

- A `min_fsync_interval_ms` config floor on group-commit cadence, for storage
  where per-fsync cost dominates (consumer SATA class). Default `0` keeps the
  self-clocking behavior.

### Changed

- Self-clocking group commit: a staged append commits as soon as the fsync
  worker is idle instead of waiting out the fsync interval tick, so the
  durability latency floor is no longer interval-bound. The interval remains
  the ceiling while an fsync is in flight.
- Ack tracking uses a settled `RangeSet` instead of a bounded bitset, removing
  the ack-window size limit.
- Work-queue and stream partition handles are split at the type level, so a
  stream partition cannot be driven through queue-only operations.

### Fixed

- The partition kind marker write used one shared temp file, so concurrent
  identical stream declares raced renames and failed spuriously. The temp
  name is unique per writer and a matching marker short-circuits.
