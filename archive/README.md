# Historical designs

The current [repository overview](../README.md), the [feature notes](../docs/)
and the [changelog](../CHANGELOG.md) describe maintained behavior.

- [Implemented Stroma handle redesign](implementation-notes/STROMA_HANDLE_REDESIGN.md)
- [Combined queue log adoption notes](implementation-notes/COMBINED_QUEUE_LOG.md):
  notes from the September 2026 adoption of one log per work queue, since merged
- [Tail cache plan](implementation-notes/TAIL_CACHE_PLAN.md): the shipped plan.
  The rules the cache keeps live in `keratin-log/src/tail_cache.rs`.
- [Performance notes, June and July 2026](performance/PERFORMANCE_NOTES_2026H1.md):
  baselines, the writer pipeline experiments and the July hot-path audit
- [Original workspace roadmap](planning/SAMPLE_ROADMAP.md)
- [Original format notes](planning/ORIGINAL_FORMAT_NOTES.md): the April 2026 v0
  file format sketch. Manifest versions, recovery and the writer workers have
  moved on since, see the code for the current format.

The prototype roadmap contains superseded proposals. Open performance and tail
cache follow-ups are tracked in the Fibril optimization log.
