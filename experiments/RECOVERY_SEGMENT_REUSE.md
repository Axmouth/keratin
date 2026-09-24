# Recovery segment reuse

Recovery can fork a frozen message log into an unreferenced generation. The writer
first drains staged writes and fsync jobs, verifies the expected epoch/head/tail,
and rolls a nonempty active segment. Closed payload files can then be hard-linked
on Unix; indexes, metadata and the empty active segment are independent. Failed
links use ordinary copying, including across filesystems. No sharing is attempted
on other platforms; they reject version 3 logs because they lack the mutation
barrier. This does not change acknowledgement or fsync guarantees.

Stroma only selects local reuse under its partition lifecycle and application
guards, with a matching durable seal and resource incarnation. Exact retained
bounds, canonical payload digest and live-payload digest must match the selected
plan. Actual records are scanned, rather than trusting a cached receipt. Divergent
payloads use the existing verified transfer path. Installation also forks the
completed stage; old source generations and completed stages remain readable.
Snapshot selection, storage-session receipts and quorum activation remain separate.

## Mutation and restart boundaries

Every shared segment is closed before linking. Append, preallocation and shutdown
therefore touch a private active file. Opening a formerly shared file as active,
unclean-open truncation and suffix repair first check its link count. A shared file
is copied to private scratch, fsynced, atomically renamed and directory-synced before
mutation. Indexes are never shared. Retention and generation cleanup unlink names;
they cannot truncate another generation's inode. The check is outside the normal
per-record append path. No production unsafe code is introduced.

The fork destination must be new and unreferenced. Its owning recovery stage or
installation journal controls retries and cleanup. Caller cancellation does not
cancel the writer command or release the surrounding storage lifecycle operation.
A partial fork cannot be admitted. The source remains usable if the destination
fails; ordinary storage failures still require successful verification/reopen.

## Storage compatibility

**A fork that may share segments upgrades both participating manifests to version
3. Older binaries reject these logs.** The source upgrade is durable before any
link is created. Its checksum-covered extension also changes the manifest length,
so corrupting only the version header cannot disguise it as version 2. This binary accepts existing version 2 manifests as well as version
3; ordinary unshared logs retain version 2. An empty fork with no closed segment
does not require an upgrade. The guard remains after shared files are reclaimed.
Downgrading a directory by editing its manifest is unsupported and unsafe.

## Costs and limits

Digest verification remains proportional to retained history; this is not an
agreed-checkpoint implementation. Exact matching is required, with no prefix
splicing. A repair that cuts a shared segment copies that segment once before
truncation. Ordinary post-install appends use the private tail immediately.
Log events report shared payload bytes, copied bytes and shared segment count.
Hard links share an underlying corruption fault domain; every accepted recovery
baseline is still independently CRC/digest checked.

Tests cover shared inode identity, private tails, repair/retention/reopen isolation,
empty and nonzero heads, multiple segments, mismatched boundaries, actual divergent
payloads, cold/live sources, and process kills during fork, private-copy, staging
and route publication. End-to-end performance evidence is kept outside the repo.
