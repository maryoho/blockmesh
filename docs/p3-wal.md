# P3-3 WAL journal

## P3-3.1: per-AG redo journal

`internal/wal.Journal` implements a fixed-LBA redo WAL. Each AG owns one
16 MiB segment at:

`WALOffset + AGID * WALSegmentSize`

Records are little-endian and protected by CRC32C. An Intent contains complete
4 KiB page after-images. A Commit record contains the checksum of the Intent
payload. A transaction is committed only after the Commit record is synced.

Commit ordering:

1. Validate page offsets, alignment, image sizes, duplicate offsets, and
   protected metadata regions.
2. Append Intent and sync.
3. Append Commit and sync.
4. Apply all after-images and sync.

An error after WAL I/O poisons the in-memory Journal. The caller must stop
issuing transactions and reopen/recover; an I/O error does not prove that a
Commit record failed to reach stable storage.

Opening a Journal performs recovery before returning. Committed after-images
are replayed idempotently; an Intent without a matching Commit is ignored.
Complete-record framing/CRC errors fail closed. A torn final record is treated
as an incomplete tail.

## P3-3.2: dual-bank checkpoint rotation

Each AG's 16 MiB WAL segment is divided into two 8 MiB banks. The Checkpoint
region reserves two 4 KiB descriptor slots per AG at:

`CheckpointOffset + (AGID * 2 + slot) * 4096`

A descriptor contains magic, format version, AG ID, monotonically increasing
generation, highest checkpointed transaction ID, active bank, and CRC32C.
On open, the highest-generation valid descriptor wins. A torn/invalid newer
slot falls back to the older valid slot. If both slots are blank, the Journal
initializes generation 1 / bank 0 only when the WAL segment is also blank.
Existing non-empty WAL without descriptors is rejected with
`ErrCheckpointMissing`; it must not be silently interpreted as an empty log.

`Journal.Checkpoint()` performs the following protocol under the journal
mutex:

1. Select the inactive bank.
2. Zero the entire inactive 8 MiB bank and sync it.
3. Write the alternate checkpoint descriptor with generation + 1, the last
   fully-applied transaction ID, and the new active bank.
4. Sync the descriptor.
5. Switch the in-memory append position to the new bank.

The order is deliberate. Before the new descriptor is durable, the old
descriptor still selects the untouched old active bank. The inactive bank may
be cleared because it is not selected. After the new descriptor is durable,
the new descriptor selects a bank that was already fully cleared and synced.
The new descriptor is then mirrored to the other slot and synced BEFORE
Checkpoint returns and before another Commit can run. This prevents the
older slot from pointing at a stale bank after new transactions begin.

If power fails between the first descriptor sync and the mirror sync, the
new descriptor is already valid and the old descriptor still selects the
untouched old bank. On the next open, the highest valid generation is chosen
and copied to the other slot before the Journal is returned. If one copy is
later unreadable, the remaining mirrored copy still selects the active bank.

The checkpoint transaction ID advances only across transactions whose
after-images were applied and synced by Commit (or replayed and synced by
Recover). Recovery scans only the selected bank; all transactions at or below
the checkpoint high-water mark are represented by already-durable metadata
pages and need not be replayed.

A full active bank returns `ErrJournalFull`. This is a known no-write result
for the failed append and does not poison the Journal. The caller should
quiesce transactions, call Checkpoint (which discards any uncommitted Intent
tail while retaining the last committed high-water mark), then retry.
Checkpoint is explicit in this stage; automatic background checkpoint
scheduling is not included.

## Important boundaries

- The Journal is per-AG and serializes transactions within one process.
  The caller must hold AG/owner locks and fence stale writers.
- Checkpoint is safe only because this implementation syncs all target
  after-images before Commit returns. A future asynchronous apply pipeline
  must track an explicit durable applied-LSN before advancing the checkpoint.
- `btree.Open(store, journal)` routes COW page after-images and the inactive
  root superblock descriptor through one WAL transaction. `btree.Open(store)`
  remains a compatibility path without WAL protection. P3-3.4 adds the
  per-AG Bitmap/Mapping/Volume Descriptor transaction path; P3-3.5/3.6 add
  persistent Arena lifecycle, Retire Queue, and Reader Generation pages.
- WAL records are not yet bound to `volume_id`, `volume_sequence`, or
  `owner_epoch`; distributed fencing and multi-writer exclusion remain
  integration work.
- The existing 16 MiB on-disk segment is retained; its interpretation is now
  two 8 MiB banks. This is an on-disk format evolution. Non-empty pre-P3-3.2
  segments without checkpoint descriptors fail closed and need an explicit
  offline migration/format step.
- Checkpoint descriptors currently reserve the first 800 KiB of the 64 MiB
  Checkpoint region for 100 AGs. The rest remains reserved for future durable
  checkpoint payloads / metadata snapshots.

## Tests

The WAL tests cover committed redo, automatic recovery on open, ignoring an
Intent without Commit, checkpoint bank switching, recovery after reopening,
startup repair after an interrupted descriptor mirror, survival of a single descriptor-slot corruption, WAL-full checkpoint retry, and rejection of missing descriptors when either bank contains data.

## P3-3.3: COW B+Tree root publication

The B+Tree can be opened with its matching per-AG Journal:
`tree, err := btree.Open(store, journal)`. The Journal must be opened and
recovered before `metastore.Open` so the Store observes any root descriptor
replayed during recovery.

A COW mutation encodes dirty nodes as complete 4 KiB page after-images in
bottom-up order. The inactive metastore superblock containing the new root and
mapping sequence is appended as the final after-image. `Journal.CommitImages`
adapts these images to the redo WAL. The durable order remains Intent sync,
Commit sync, then application and sync. Since the root descriptor is part of
the same transaction and is applied last, recovery cannot expose a root whose
new COW pages were not also committed.

The two root descriptor slots are at offsets 0 and 4096. The WAL accepts an
update to either reserved slot only when the image has the metastore magic and
a valid CRC32C, and it must be the single final after-image in the transaction.
The Store API restricts COW page images to the metadata arena; the Journal
rejects overlap with WAL/checkpoint regions.

`ErrJournalFull` is a pre-commit result and allows the COW page reservation to
be aborted. Other I/O errors have an uncertain commit outcome; the process
must stop using its current Store/Tree, reopen the Journal for recovery, then
reopen the Store and Tree. The volatile Arena is reconstructed from the
recovered root.

This stage does not yet make retirement durable. Old COW pages remain in the
process-local retire queue, and they must not be reclaimed until the later
persistent retire queue + reader-generation + fencing work is complete.


## P3-3.5 lifecycle after-images

When a B+Tree is opened with `btree.OpenPersistent`, each COW root transaction
also includes the Arena lifecycle checkpoint and persistent Retire Queue
pages as WAL after-images. Ordering is COW data pages, lifecycle pages, then
the inactive metastore superblock as the final root publication image.
Retirement therefore cannot be lost between root publication and process
restart. Reclamation is a separate WAL transaction that first persists the
shortened queue; only then are the pages returned to the in-memory allocator.

The lifecycle pages must be configured outside the Arena and outside the
per-AG WAL/checkpoint regions. Recover the relevant WAL before opening the
persistent tree. The lifecycle checkpoint and queue pages carry matching
generations; a mismatch fails closed.


## P3-3.6: multi-page Retire Queue and Reader Generations

The lifecycle region reserves page 0 for the lifecycle checkpoint and pages
1..N-1 for a fixed-capacity paged Retire Queue. Every queue page carries the
same queue generation plus its page index/count and CRC32C. Root publication
and queue collection update the lifecycle page and all queue pages in one WAL
transaction. A partial replay cannot be accepted: mismatched generations,
missing pages, invalid indexes, or duplicate retired IDs fail closed. The
single queue page written by P3-3.5 remains readable for a two-page lifecycle
region.

Reader records use one dedicated 4 KiB page per slot in a separately reserved
metadata range. Reader page updates are WAL after-images. Records contain
NodeID, BootID, Reader Epoch, Root Generation, heartbeat sequence, and
Active/Leaving/Fenced state. Active and Leaving readers contribute to the
minimum safe generation. A slot becomes excluded only after a successful
fencer call or a caller-confirmed graceful I/O drain. The Reader Table does
not implement cluster-wide slot allocation or lease expiry; those must be
serialized by the deployment's lease/lock service.

The reader page range must be outside the Arena, lifecycle, WAL, checkpoint,
and other metadata regions. Its geometry is deployment configuration and
must be consistent on all nodes. A production fencer must revoke every SAN
path before a failed reader is marked Fenced. Heartbeat timeout alone never
permits reclamation.
