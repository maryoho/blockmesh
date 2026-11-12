# P3 fixed-LBA metadata layout (format version 1)

All metadata is addressed in **4 KiB pages**. Page N starts at byte offset
N * 4096 from the beginning of the metadata device. Integers are little-endian.
The on-disk format is encoded field-by-field; Go/C structure layout is never
written directly.

## Device layout

| Page range | Purpose |
|---|---|
| 0 | Superblock copy A |
| 1 | Superblock copy B |
| [arena_start, arena_start + arena_pages) | Metadata page arena |
| All other pages | Reserved for later WAL/checkpoint/AG layout |

The initial format requires arena_start >= 2, a non-empty arena, and a range
contained in total_pages. Page offsets must fit in signed 64-bit byte offsets.
The formatter is destructive and must only run while the metadata device is
exclusively owned.

## Superblock page

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | Magic BMMETA01 |
| 8 | 2 | Format version (1) |
| 10 | 2 | Page size (4096) |
| 12 | 4 | Reserved, zero |
| 16 | 8 | Superblock generation |
| 24 | 8 | Total device pages |
| 32 | 8 | Arena start page |
| 40 | 8 | Arena page count |
| 48 | 8 | Root page (UINT64_MAX means no root) |
| 56 | 8 | Mapping sequence |
| 64 | 64 | UTF-8 volume identifier, NUL padded |
| 128 | 3964 | Reserved, zero |
| 4092 | 4 | CRC32C of bytes [0, 4092) |

The two copies are independently checksummed. Open selects the valid copy with
the greatest generation. Publish writes the inactive copy and flushes it before
changing the in-memory selected root. A torn/corrupt newest copy therefore
leaves the previous copy available. A generation is advanced by exactly one
for every publication.

## Metadata arena page

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | Magic BMPG |
| 4 | 2 | Page format version (1) |
| 6 | 2 | Page type |
| 8 | 8 | Absolute page ID |
| 16 | 8 | Metadata generation |
| 24 | 4 | Payload length (0..4064) |
| 28 | 4 | CRC32C of the complete 4096-byte page with this field zeroed |
| 32 | 4064 | Payload, followed by zero padding |

A page ID is always its absolute LBA page number and must fall inside the
configured arena. The reader verifies both the page ID and checksum.

## Required write ordering

For a COW metadata update:

1. Allocate new arena pages without overwriting pages reachable from the
   currently published root.
2. Write all new page images.
3. Flush the metadata device (Store.Sync).
4. Publish the new root and mapping sequence (Store.Publish); this writes the
   inactive superblock and flushes it.
5. Only after publication and reader-generation safety may old pages be reused.

The process-local Metadata Arena allocator is implemented in internal/metastore/arena.go; see docs/p3-arena.md for its reservation and page-lifecycle contract. The allocator is not yet durable and does not implement WAL, AG locking, reader leases, fencing, or crash-safe reclamation. Those remain subsequent P3 stages. The caller must serialize writers and must not treat WritePage completion as stable storage until Sync succeeds.

## API status

internal/metastore is the first P3-1 building block. Format, Open, WritePage,
ReadPage, Sync, and Publish are implemented and unit-tested with a memory-backed
device. The existing JSON-based reference metadata.Engine is not yet migrated
to this on-disk format.
