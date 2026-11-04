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
- The generic `internal/metastore.Store` / COW B+Tree path is not yet routed
  through this Journal. Do not treat B+Tree Put/Delete as WAL-protected.
- WAL records are not yet bound to `volume_id`, `volume_sequence`, or
  `owner_epoch`; distributed fencing and multi-writer exclusion remain
  integration work.
- The existing 16 MiB on-disk segment is retained; its interpretation is now
  two 8 MiB banks. This is an on-disk format evolution. Non-empty pre-P3-3.2
  segments without checkpoint descriptors fail closed and need an explicit
  offline migration/format step.
- Checkpoint descriptors currently reserve the first 80 KiB of the 64 MiB
  Checkpoint region for 10 AGs. The rest remains reserved for future durable
  checkpoint payloads / metadata snapshots.

## Tests

The WAL tests cover committed redo, automatic recovery on open, ignoring an
Intent without Commit, checkpoint bank switching, recovery after reopening,
and fallback to the older descriptor when the newest descriptor is torn.
