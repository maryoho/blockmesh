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

1. Read the current root and descend to the target leaf; untouched subtrees
   remain references to their existing immutable pages.
2. Apply the leaf change. Splits, merges and rebalancing propagate COW
   references toward the root.
3. Reserve exactly the number of dirty pages and encode their complete 4 KiB
   after-images bottom-up. Encoding does not write target pages.
4. With a WAL committer supplied to `Open(store, journal)`, submit all new
   page images and the inactive root superblock as one per-AG WAL transaction.
   The root descriptor is the final after-image in that transaction.
5. WAL persists Intent, then Commit, then applies the page images in order
   (COW pages first, root descriptor last) and syncs the metadata device.
6. Only after WAL commit/application succeeds does the process-local Arena
   reservation become Live and the in-memory tree state advance.

This ordering provides an old-root-or-new-root recovery property. Before the
WAL Commit is durable, no target after-image is applied. After Commit is
durable, recovery replays every COW page and then the root descriptor; a crash
during application is idempotently repaired. A root descriptor can therefore
never become durable through this path before its referenced new pages.

For compatibility, `Open(store)` retains the original non-WAL publication
path. Production use of the P3-3.3 guarantee must open the tree with the
matching per-AG Journal: `Open(store, journal)`. The Journal must be recovered
before `metastore.Open` and before the tree is opened, so a replayed root
descriptor is reflected in memory.

If the WAL reports `ErrJournalFull`, the transaction did not commit and the
new page reservation is aborted; checkpoint the AG WAL and retry. For other
WAL I/O errors, commit outcome may be uncertain, so reserved page IDs are
retained until the Journal is reopened and recovery determines the durable
root. The process must stop using the old in-memory Store/Tree after such an
error.

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
- This package is a metadata-engine building block. Its COW page/root publication
  can use the per-AG WAL, but it is not yet wired into the JSON reference
  `metadata.Engine`, bitmap transactions, distributed lease/fencing layer, or DM target.
