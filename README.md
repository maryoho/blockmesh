# BlockMesh P3 metadata engine (reference implementation)

This repository is a buildable P3 control-plane reference for Linux 5.15 BlockMesh.

## Selected consistency semantics

* RO readers use an online-consistent view: committed root changes become visible after refresh.
* A failed reader is reclaimed only after its lease is declared dead **and** SAN fencing succeeds.
* Mapping updates are copy-on-write snapshots. Retired physical blocks and metadata generations are reclaimed only after every active reader has advanced beyond the retire generation or has been fenced.

## Important integration boundary

The repository includes a sanlock-backed distributed resource locker, per-reader sanlock lease sessions, a shared-WAL committer that reloads each AG journal under a sanlock lock, and an sg_persist-based SCSI PR registrar/fencer using PREEMPT AND ABORT with per-path verification. P3-3.12 adds a Linux block-device backend using BLKGETSIZE64/BLKSSZGET and fsync. P3-3.12.1 standardizes the disk layout: BMPOOL02 Pool Superblocks occupy LBA 0/1; BMDVOL02 Volume Descriptor pairs live in the Node Table; WAL refuses to overwrite Pool Superblocks. `MemoryLease`, `FileLocker`, and `NoopFencer` remain test/development-only. Hardware qualification, sanlock resource provisioning, trusted key management, local DM fail-stop callbacks, and global I/O-drain orchestration remain deployment responsibilities.

The included `metadata.Engine` is still a single-volume JSON snapshot reference engine with an append-only redo WAL. It is not safe for multiple nodes to open the same files. The new `internal/metastore` package implements fixed-LBA 4 KiB metadata pages, redundant superblocks, checksums, root publication ordering, and both legacy process-local and version-2 WAL-persisted Arena lifecycle modes. Persistent mode supports a multi-page Retire Queue and durable Reader Generation slots; Live pages are reconstructed from the published B+Tree graph plus queued retired pages. Reader slots are coupled to process-lifetime sanlock leases and SCSI PR registration. Failed-reader recovery must acquire the old slot lease, fence and verify every target PR key, then persist Fenced. See docs/p3-reader-fencing.md for provisioning and operational ordering. A deployment must still provide trusted key resolution, local DM fail-stop callbacks, and global I/O-drain coordination. `ReaderRecoverySupervisor` now owns one recovery loop per activated volume, rejects test-only or incomplete production wiring, and joins recovery workers before metadata resources may be closed. See [docs/p3-3.10.md](docs/p3-3.10.md).

## P3 implementation status

* **P3-1:** fixed-LBA metadata pages, dual superblocks, checksums, root publication ordering, and Arena allocation lifecycle.
* P3-2: page-level copy-on-write B+Tree.
* **P3-3 (in progress):** per-AG fixed-LBA redo WAL, recovery-before-open, dual-bank checkpoint rotation, atomic COW root publication, atomic Bitmap/Mapping/Volume Descriptor allocation, and optional WAL-persisted B+Tree Arena lifecycle, multi-page Retire Queue, and durable Reader Generation slots. Distributed lease/slot coordination and production SAN fencing remain open.
* P3-4: multi-node leases, AG coordination, shared reader visibility and SAN fencing.
* P3-5: crash recovery and fault-injection tests.
* **P3-3.10 / P3-3.11:** per-volume Recovery Controller supervisor and retryable `VolumeRuntimeManager` lifecycle orchestration.
* **P3-3.12 / P3-3.12.1:** Linux block-device access; unified BMPOOL02 Pool Superblock and BMDVOL02 Volume Descriptor v2; Pool-level `bm-meta format` / `inspect`; WAL protects LBA 0/1 on formatted Pool devices. The Pool-backed migration of COW B+Tree Store, Persistent Arena, Reader Table and Retire Queue is still pending; do not open a BMPOOL02 device with the legacy `metastore.Store`. See [docs/p3-3.12.md](docs/p3-3.12.md) and [docs/p3-3.12.1.md](docs/p3-3.12.1.md).
* P3-6: Linux DM Target integration.

See [docs/p3-metadata-layout.md](docs/p3-metadata-layout.md), [docs/p3-arena.md](docs/p3-arena.md), [docs/p3-wal.md](docs/p3-wal.md), and [docs/p3-reader-fencing.md](docs/p3-reader-fencing.md) for the on-disk layout, page lifecycle, WAL contract, and sanlock/SCSI PR integration.

## Layout

* `internal/device`: random-access durable device interface, file-backed test device, and Linux block-device backend.
* `internal/metastore`: fixed-LBA 4 KiB metadata pages, redundant superblocks, process-local and WAL-persisted Arena lifecycle, multi-page Retire Queue, durable Reader Generation slots, recovery supervision, and retryable per-Volume Runtime lifecycle.
* `internal/wal`: per-AG fixed-LBA CRC32C redo WAL with Intent/Commit, recovery-before-open, dual-bank checkpoint rotation, and `SharedCommitter` for cross-process AG serialization.
* `internal/metadata`: current JSON-backed reference engine, descriptor and retire queue.
* `internal/allocator`: 4 MiB allocation bitmap and per-AG WAL transaction publishing Bitmap, immutable Mapping COW page, and Volume Descriptor.
* `internal/fence`: SCSI PR registration, PREEMPT AND ABORT fencing, and test-only no-op implementation.
* `internal/lock`: local test locker and sanlock-backed distributed resource leases.
* `cmd/bm-lock-helper`: parent-death-bound helper used by sanlock client command.
* `internal/volume`: volume lease contract, test-only in-memory lease, and process-lifetime sanlock reader leases.

## Run

```bash
go test ./...

# Read-only fixed-LBA superblock inspection
go run ./cmd/bm-meta inspect --device /dev/mapper/mpath-meta

# Destructive initialization: calculate geometry for the actual LUN first
go run ./cmd/bm-meta format --device /dev/mapper/mpath-meta --data-start-sector 0 --confirm-format

# Isolated JSON/WAL reference demo
go run ./cmd/bm-meta demo -wal /var/lib/blockmesh/vol-a.wal -state /var/lib/blockmesh/vol-a.state
```

`format` / `inspect` operate on a real Linux block device and the BMPOOL02 Pool layout; `format` is destructive and its local `O_EXCL` open is not a distributed lease. The `demo` subcommand still uses JSON files, `MemoryLease`, and `NoopFencer`; it is not production-safe and does not activate a DM device. See [docs/p3-3.12.md](docs/p3-3.12.md) before using the block-device commands.
