# P3 Metadata Arena and persistent retirement lifecycle

The metadata device uses 4 KiB pages. The allocatable Arena is
`[ArenaStart, ArenaStart + ArenaPages)`. P3-3.5 optionally reserves a
separate lifecycle region outside the Arena through `Layout.LifecycleStart`
and `Layout.LifecyclePages`. The first two pages are the Arena lifecycle
checkpoint and persistent Retire Queue. Existing layouts with
`LifecyclePages == 0` retain the legacy process-local behavior.

## Durable lifecycle format

The lifecycle pages are initialized explicitly with
`metastore.FormatLifecycle(store)` while the device is exclusively owned.
The two 4 KiB pages contain:
- `BMARNA01`: lifecycle generation and the root Mapping Sequence covered by
  the checkpoint.
- `BMRETR01`: a CRC32C-protected list of retired page IDs and their retire
  generations.

The on-disk superblock stores the lifecycle region geometry in previously
reserved bytes, preserving the existing format version. Old superblocks with
zero lifecycle geometry remain readable but cannot be opened with
`btree.OpenPersistent`.

## Allocation and recovery

Use `btree.OpenPersistent(store, journal)` after the per-AG WAL has been
recovered. It loads the current B+Tree graph, loads every durable retire
record, and reconstructs the Arena's Live set from both. Pages in the retire
queue remain Live and cannot be reserved.

Reservations remain process-local by design. If a process crashes before its
WAL Commit becomes durable, the reservation disappears; its COW pages are
unreachable and can safely be reused. If Commit is durable, the same WAL
transaction contains:
1. all newly written immutable COW pages;
2. the updated lifecycle generation and retire queue;
3. the new redundant superblock root descriptor as the final after-image.

Thus a recovered root cannot become visible without its corresponding
retirement records. After commit, the in-memory reservation transitions from
Reserved to Live.

## Retirement and reclamation

Old pages are added to the persistent Retire Queue in the same WAL transaction
that publishes the replacement root. `Tree.RetiredPages()` reads the durable
queue when the tree was opened with `OpenPersistent`.

`Tree.CollectRetired(proof)` requires all three conditions:
- `SafeGeneration > RetireGeneration`;
- `IOsDrained == true`;
- `FencingConfirmed == true`.

The shortened queue is WAL-committed and synced before pages are returned to
the in-memory free allocator. A crash after that durable queue update is safe:
the old pages are no longer reachable from the root and have passed the
reclamation proof. A crash before the update leaves them in the durable queue
and they remain pinned after reopen.

## Capacity and integration limits

The first implementation uses one 4 KiB Retire Queue page. It can hold at most
`MaxPersistentRetiredPages` (253) page records. When full, root publication
fails with `ErrRetireQueueFull`; it never drops records or frees pages to make
space. A future extension should use a multi-page COW queue or segmented
retirement log.

The lifecycle checkpoint reconstructs page state from the published tree plus
retired pages rather than persisting a full free bitmap. This is safe for the
current B+Tree-owned metadata Arena; additional metadata object types must be
included in the root reachability walk or registered as pinned allocations
before they can share this Arena.

The caller remains responsible for real reader-generation tracking, lease
expiry, SAN fencing, and in-flight I/O drain. The proof object is an
integration contract, not a fencing implementation. Heartbeat timeout alone
is not sufficient. Do not use the process-local `NewArena` / `RetireQueue`
path for multi-process crash-safe reclamation.
