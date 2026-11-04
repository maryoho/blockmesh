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
| 4 | 2 | Node format version (1) |
| 6 | 2 | Entry count |
| 8 | 2 | Level (0 for leaf, greater than 0 for internal) |
| 10 | 2 | Reserved, zero |

Leaf payload entries are fixed 16-byte little-endian pairs:
`key:u64, value:u64`. A leaf holds at most 253 entries.

An internal node stores `count` separator keys followed by `count+1`
absolute child page IDs. A separator is the minimum key in the child to its
right. Internal nodes hold at most 252 separators (253 children).

## Publication protocol

A mutation builds a new immutable tree in reserved arena pages:

1. Reserve all pages needed for the new tree.
2. Write leaf pages and then internal pages, each tagged with the next mapping
   sequence as its generation.
3. Call `Store.Sync()`.
4. Publish the new root through the inactive redundant superblock.
5. Commit the arena reservation.

A failed page write or pre-publication Sync aborts the reservation. If
`Store.Publish` returns an error, the reservation is intentionally retained:
the inactive superblock may have been written even when its flush reports an
error, so reusing those page IDs before reopen/recovery would be unsafe.

## Current implementation scope and limitations

- API: `Open`, `Get`, `Put`, `Delete`, `Snapshot`, `RootPage`.
- The tree is validated recursively when opened, including node type, level,
  ordering, child ranges, separators, page checksums and duplicate/cyclic
  child references.
- Mutations use path-copy COW. A normal insert, replace or delete writes
  only the changed leaf and its ancestors. A leaf split creates two new leaves;
  an internal split creates two new internal pages and can propagate to the
  root. A root split creates one new root. All old pages remain immutable.
- Deletion removes empty leaves and prunes empty child references. Underfull
  non-root nodes are currently allowed; internal nodes with one child are
  encoded with zero separators. A one-child root is collapsed to its child.
  Merge/redistribution balancing is deferred.
- `Get` still uses the sorted in-memory item cache, and `Put`/`Delete`
  still copy that slice to maintain `Snapshot`. Disk-page write amplification
  is O(tree height), but CPU/memory mutation cost remains O(N).
- Old roots and their pages are not reclaimed. This is intentional until the
  Reader Lease, generation tracking and SAN fencing/reclamation stages are
  integrated. Do not reuse old-root pages while any reader may still hold an
  older root. Repeated updates therefore consume arena pages; a persistent
  retire queue and safe reclamation are required before long-running production
  use.
- The arena allocator is process-local. `Open` rebuilds its live set from the
  currently published root. Reopen/reclamation with concurrent old-root
  readers is not production-safe until the persistent retire queue and reader
  fencing protocol are implemented.
- This package is a metadata-engine building block; it is not yet wired into
  the JSON reference `metadata.Engine`, WAL transaction layer, or DM target.
