# P3-1 Metadata Arena allocation lifecycle

The metadata device uses 4 KiB pages. The arena is the page range described by
the selected superblock: [ArenaStart, ArenaStart + ArenaPages).

## Page states

The P3-1 allocator in internal/metastore/arena.go tracks three process-local
states:

* **Free**: available for reservation.
* **Reserved**: exclusively held by one in-flight metadata update.
* **Live**: reachable from the published metadata root and not eligible for
  allocation.

NewArena takes the complete set of pages reachable from the current root. It
also adds the current root page if omitted. Callers must walk all internal,
leaf, and overflow pages in the root graph; supplying only the root does not
protect descendants.

## Update protocol

For a COW metadata update:

1. Reserve(n) obtains unique page IDs. IDs may be non-contiguous.
2. Write all page images using Store.WritePage.
3. Call Store.Sync to make page images durable.
4. Call Store.Publish(newRoot, nextMappingSequence).
5. Call Reservation.Commit to transition reserved pages to live.
6. Retain the old root graph until every reader has advanced beyond its retire
   generation and all I/O using the old graph has drained.
7. Call Arena.ReleasePages(oldPageIDs) only after step 6.

If any step before publication fails, call Reservation.Abort. Never abort a
reservation after publishing a root that references one of its pages.
Serialize root publication and page release for each volume.

## Restart and durability boundary

Allocator state is volatile in P3-1. After restart, construct a new Arena by
traversing the published root and passing every reachable page to NewArena.
Unreachable pages are treated as free. This relies on COW writing and flushing
new pages before publishing the root, and on old pages not being released until
reader-generation safety has been established.

P3-1 does not persist a free-page bitmap or transaction intents. P3-3 will put
allocator bitmap updates in the same WAL transaction as B+Tree and descriptor
updates. Until then, this allocator is not suitable for concurrent
multi-process access or production crash recovery.

Reserve uses a rotating first-fit cursor and a LIFO free-page reuse list. It is
concurrency-safe within one process and does not promise contiguous page IDs.
