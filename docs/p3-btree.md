# P3-2: fixed-page COW B+Tree

The first B+Tree implementation is in `internal/btree`. It stores an ordered
mapping from `uint64` keys to `uint64` values in immutable 4 KiB metadata
pages managed by `internal/metastore`.

## On-disk node format

Every node is a metastore page and therefore receives the outer `BMPG`
header, absolute page ID, generation and CRC32C protection.

The node payload starts with a 12-byte header:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | Node magic `BMTN` |
| 4 | 2 | Node format version (1 = legacy, 2 = path-copy) |
| 6 | 2 | Entry count |
| 8 | 2 | Level (0 for leaf, greater than 0 for internal) |
| 10 | 2 | Reserved, zero |

Leaf payload entries are fixed 16-byte little-endian pairs:
`key:u64, value:u64`. A leaf holds at most 253 entries.

An internal node stores `count` separator keys followed by `count+1`
absolute child page IDs. A separator is the minimum key in the child to its
right. Internal nodes hold at most 252 separators (253 children). New pages
are written as node format v2; v1 pages remain readable. Version 2 permits a
one-child internal node (`count=0`), which is used when deletion leaves an
underfull non-root internal node. Version 1 continues to require at least one
separator.

## Publication protocol

A mutation builds only the changed path in reserved arena pages:

1. Read the root and descend to the target leaf; untouched subtrees remain
   references to their existing immutable pages.
2. Apply the leaf change. If it overflows, split it and propagate the new child
   reference upward. Internal overflow is split in the same way.
3. Count dirty nodes, reserve exactly that many pages, and write dirty nodes
   bottom-up with the next mapping sequence as their generation.
4. Call `Store.Sync()`.
5. Publish the new root through the inactive redundant superblock.
6. Commit the arena reservation. If deletion collapses the root to an
   unchanged child, publish that existing child page without allocating a
   metadata page.

A failed page write or pre-publication Sync aborts the reservation. If
`Store.Publish` returns an error, the reservation is intentionally retained:
the inactive superblock may have been written even when its flush reports an
error, so reusing those page IDs before reopen/recovery would be unsafe.

## Current implementation scope and limitations

- API: `Open`, `Get`, `Put`, `Delete`, `Snapshot`, `RootPage`, `RetiredPages`,
  `CollectRetired`.
- The tree is validated recursively when opened, including node type, level,
  ordering, child ranges, separators, page checksums and duplicate/cyclic
  child references.
- Mutations use path-copy COW. A normal insert, replace or delete writes
  only the changed leaf and its ancestors. A leaf split creates two new leaves;
  an internal split creates two new internal pages and can propagate to the
  root. A root split creates one new root. All old pages remain immutable.
- Deletion rebalances underfull non-root nodes. Adjacent siblings are combined;
  if their contents fit, they are merged, otherwise their entries/children are
  redistributed evenly. Internal separators are regenerated from right-child
  subtree minima. One-child roots are repeatedly collapsed and an empty tree
  publishes `NoPage`.
- `Get` still uses the sorted in-memory item cache, and `Put`/`Delete`
  still copy that slice to maintain `Snapshot`. Disk-page write amplification
  is O(tree height), but CPU/memory mutation cost remains O(N).
- After successful root publication, pages present in the old reachable graph
  but absent from the new graph are placed in a process-local retire queue with
  the old mapping sequence as their retire generation. `RetireQueue.Collect`
  requires `SafeGeneration > RetireGeneration`, drained old I/O, and explicit
  fencing confirmation. Heartbeat expiry alone is not a fencing proof.
- The retire queue is volatile and is not crash-safe. Until WAL persists retire
  batches and the Reader Lease/SAN fencing layer supplies trustworthy proofs,
  production callers must not collect retired pages. A restart reconstructs
  only the currently published graph; persistence/recovery of pending retire
  batches remains a later WAL responsibility.
- The arena allocator is process-local. `Open` rebuilds its live set from the
  currently published root. Reopen/reclamation with concurrent old-root
  readers is not production-safe until the persistent retire queue and reader
  fencing protocol are implemented.
- This package is a metadata-engine building block; it is not yet wired into
  the JSON reference `metadata.Engine`, WAL transaction layer, or DM target.
