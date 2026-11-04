# P3 Metadata Arena, Retire Queue and Reader Generations

The metadata device uses 4 KiB pages. The allocatable Arena is
`[ArenaStart, ArenaStart + ArenaPages)`. A lifecycle region is reserved
outside the Arena through `Layout.LifecycleStart` and
`Layout.LifecyclePages`. Its first page is the lifecycle checkpoint; every
remaining page belongs to the persistent Retire Queue. Layouts with
`LifecyclePages == 0` retain the legacy process-local behavior.

## Lifecycle and multi-page Retire Queue

Initialize the region with `metastore.FormatLifecycle(store)` while the
metadata device is exclusively owned. Formatting clears the complete
configured lifecycle region. When converting an existing volume from the
process-local Arena, stop writers, fence old readers, drain I/O, and perform
an offline reachability/reclamation pass first; an empty queue cannot recover
retirement records that existed only in process memory.

- `BMARNA01`: lifecycle generation and the root Mapping Sequence covered by
  the checkpoint.
- `BMRETR01`: one or more CRC32C-protected queue pages. Each page records
  the queue generation, page index/count, and retired page IDs with their
  retire generations.

The queue uses a fixed page range, not a linked mutable list. All queue pages
and the lifecycle checkpoint are included in the same WAL transaction as a
root publication or queue collection. Every page carries the same generation;
a missing page, duplicate page ID, mismatched generation, invalid index, or
bad CRC fails closed. Queue capacity is
`(LifecyclePages - 1) * MaxPersistentRetiredPages` records (253 records per
4 KiB queue page). A full queue rejects root publication with
`ErrRetireQueueFull`; records are never dropped to make room.

P3-3.5's one-page queue encoding remains readable when the first queue page
uses the legacy version and any additional reserved queue pages are blank.
New writes use the paged encoding. Lifecycle superblocks remain format version
2; the queue page encoding has its own version.

## Allocation and recovery

Use `btree.OpenPersistent(store, journal, readerCoordinator)` after the
per-AG WAL has been recovered when multi-node reader generation management is
enabled. The coordinator is optional only for single-node use. The legacy `btree.Open` and `metastore.NewArena` deliberately
reject lifecycle-enabled layouts. The Arena's Live set is reconstructed from
the current B+Tree graph plus every durable retire record. Reservations remain
process-local: uncommitted, unreachable COW pages can be reused after restart.

A root transaction contains:
1. all newly written immutable COW pages;
2. the lifecycle checkpoint after-image;
3. every page in the updated Retire Queue;
4. the inactive superblock root descriptor as the final after-image.

Thus a recovered root cannot become visible without its corresponding
retirement records. Queue collection similarly WAL-commits the shortened queue
before pages become Free in memory.

## Reader Generation Table

`metastore.NewReaderTable(store, startPage, slots, committer, readerCoordinator)` creates a
persistent table with one dedicated 4 KiB page per reader slot. The caller
must reserve this range outside the Arena and lifecycle region and must also
ensure it does not overlap WAL/checkpoint regions (the WAL validates that
condition on commit). The geometry and slot assignment policy are deployment
configuration and must be identical on every node. A cluster lock/lease
service must serialize slot assignment; two nodes must never register into the
same slot concurrently.

Each record stores NodeID, BootID, monotonically increasing Reader Epoch,
Root Generation, heartbeat sequence, and state. A joining reader must call
`Tree.Refresh()`, then register at exactly the current Mapping Sequence
before it begins issuing reads; it cannot join using a stale root generation.
An existing reader calls `Tree.Refresh()` before heartbeating an advanced
generation. Refresh rebuilds the in-memory B+Tree view and persistent Arena
from the published root; it rejects trees with outstanding local reservations.
- Active: participates in the minimum safe generation.
- Leaving: still pins its generation until clean I/O drain or fencing.
- Fenced: excluded only after the SAN fencer succeeds.
- Closed: excluded only after the graceful close path confirms the caller has
  stopped new reads and drained all I/O.

A new process instance must use a new BootID and a strictly higher Epoch before
reusing a fenced or gracefully Closed slot. Heartbeats refresh the local redundant superblock view
before accepting a new Root Generation; they may advance that generation but
cannot move it backwards. The caller is responsible for refreshing the B+Tree
view first, so the recorded generation matches the root actually used for reads. `SafeGeneration()` returns the minimum Root Generation across
all Active/Leaving slots, or MaxUint64 if there are none.

Use `FenceAndClose` to call the configured `fence.Fencer`; the durable slot
is marked Fenced only after fencing succeeds. A heartbeat timeout alone never
changes a slot to Fenced. Graceful shutdown may use `CloseDrained`, but only
after all reads are stopped and in-flight I/O is drained.

`PersistentArena.CollectWithReaders(readers, ioDrained)` derives the safe
generation from this table and combines it with the I/O-drain condition. It
requires a `ReaderCoordinator` on both objects. The same cluster-wide
exclusive reader-set barrier is passed through `btree.OpenPersistent` to
`NewPersistentArena`;
root publication acquires it so a reader cannot register on the old root
between the generation snapshot and reclamation. Reader registration acquires
the same barrier and joins at the current root. This barrier must be backed by
the distributed lease/lock service; a process-local mutex is not sufficient.
Every reader that can access the LUN must be represented in the table, and
Fenced state must only be written by the trusted SAN fencer. The lower-level
`Collect(ReclaimProof)` remains a trusted integration API; callers can violate
safety if they fabricate its proof or bypass the coordinator.

## Remaining integration boundaries

This stage supplies durable reader slots, a ReaderCoordinator contract, and
generation calculations, not a complete distributed lease/slot-allocation
service. It does not implement
sanlock, SCSI Persistent Reservations, PREEMPT AND ABORT across multipath, or
automatic node-failure detection. Production deployment must provide:
- cluster-serialized reader slot allocation and ownership;
- owner/reader leases and monotonically increasing epochs;
- a real SAN fencer that revokes every path before marking a failed reader
  Fenced;
- per-volume I/O drain coordination before reclamation;
- configuration that reserves reader pages outside WAL/checkpoint and all
  other metadata regions.

The lifecycle checkpoint reconstructs page state from the published tree plus
retired pages rather than persisting a full free bitmap. This is currently
appropriate only for the B+Tree-owned metadata Arena; other metadata objects
must be included in the reachability walk or registered as pinned allocations.
