# P3-3 WAL journal (initial implementation)

## Scope

`internal/wal.Journal` is the first fixed-LBA redo-WAL implementation for the
pool layout in `internal/format`. It is separate from the older generic
`internal/metastore.Store` test format; the two superblock types/layouts are
not interchangeable.

Each allocation group owns one fixed 16 MiB segment:

`WALOffset + AGID * WALSegmentSize`

Records are little-endian and protected by CRC32C. An Intent contains a list
of complete 4 KiB page after-images. A Commit record contains the checksum of
the Intent payload. A transaction is committed only after both records have
been written and the metadata device has been synced after the Commit record.

## Commit ordering

1. Validate all page offsets, alignment, image sizes, duplicate offsets, and
   that updates do not overlap the WAL or superblock pages.
2. Append the Intent record and call `Device.Sync()`.
3. Append the Commit record and call `Device.Sync()`.
4. Apply every after-image to its target metadata page.
5. Call `Device.Sync()` before returning success.

An error after the Intent/Commit write poisons the in-memory Journal. The
caller must stop issuing transactions and reopen/recover the journal. It must
not assume that an I/O error means the transaction did not commit: if the
Commit record reached stable storage, recovery will replay it.

## Recovery

Opening a Journal scans the selected AG segment and replays every valid
committed transaction before returning. Redo is idempotent. An Intent without
a matching Commit is ignored. A short final header or payload is treated as a
torn tail and is not replayed; a checksum or framing error in a complete
record is treated as corruption and fails closed.

The WAL is not allowed to overwrite old records. When the fixed segment is
full, Commit returns `ErrJournalFull`; the caller must stop writes. This
initial slice deliberately does not implement checkpoint-based segment
recycling yet. Recycling must only be added together with a durable,
redundant checkpoint descriptor and crash-safe segment switching.

## Current boundaries

- The Journal is per-AG and serializes transactions within one process.
- The caller must hold the relevant AG/owner locks and fence stale writers.
- The caller is responsible for publishing the root/descriptor only after
  the committed after-images have been applied and synced.
- This initial implementation does not yet bind a WAL record to
  `volume_id`, `volume_sequence`, or `owner_epoch`; those fields and
  distributed writer fencing are part of the next integration step.
- The generic `internal/metastore.Store` / COW B+Tree path is not yet routed
  through this Journal. Do not treat the current B+Tree API as transactionally
  WAL-protected until that integration is complete.
- Checkpoint rotation, crash fault injection at every write boundary, bitmap
  transaction integration, and persistent allocator recovery remain open
  work.

## Tests

`internal/wal` tests cover durable redo and verify that opening a journal
replays committed transactions while ignoring an Intent without Commit.
