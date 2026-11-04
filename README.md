# BlockMesh P3 metadata engine (reference implementation)

This repository is a buildable P3 control-plane reference for Linux 5.15 BlockMesh.

## Selected consistency semantics

* RO readers use an online-consistent view: committed root changes become visible after refresh.
* A failed reader is reclaimed only after its lease is declared dead **and** SAN fencing succeeds.
* Mapping updates are copy-on-write snapshots. Retired physical blocks and metadata generations are reclaimed only after every active reader has advanced beyond the retire generation or has been fenced.

## Important integration boundary

The repository now includes a sanlock-backed distributed resource locker, per-reader sanlock lease sessions, a shared-WAL committer that reloads each AG journal under a sanlock lock, and an sg_persist-based SCSI PR registrar/fencer using PREEMPT AND ABORT with per-path verification. `MemoryLease`, `FileLocker`, and `NoopFencer` remain test/development-only. Hardware qualification, sanlock resource provisioning, trusted key management, local DM fail-stop callbacks, and global I/O-drain orchestration remain deployment responsibilities.

The included `metadata.Engine` is still a single-volume JSON snapshot reference engine with an append-only redo WAL. It is not safe for multiple nodes to open the same files. The new `internal/metastore` package implements fixed-LBA 4 KiB metadata pages, redundant superblocks, checksums, root publication ordering, and both legacy process-local and version-2 WAL-persisted Arena lifecycle modes. Persistent mode supports a multi-page Retire Queue and durable Reader Generation slots; Live pages are reconstructed from the published B+Tree graph plus queued retired pages. Reader slots are coupled to process-lifetime sanlock leases and SCSI PR registration. Failed-reader recovery must acquire the old slot lease, fence and verify every target PR key, then persist Fenced. See docs/p3-reader-fencing.md for provisioning and operational ordering. A deployment must still provide trusted key resolution, local DM fail-stop callbacks, and global I/O-drain coordination. `ReaderRecoverySupervisor` now owns one recovery loop per activated volume, rejects test-only or incomplete production wiring, and joins recovery workers before metadata resources may be closed. See [docs/p3-3.10.md](docs/p3-3.10.md).

## P3 implementation status

* **P3-1:** fixed-LBA metadata pages, dual superblocks, checksums, root publication ordering, and Arena allocation lifecycle.
* P3-2: page-level copy-on-write B+Tree.
* **P3-3 (in progress):** per-AG fixed-LBA redo WAL, recovery-before-open, dual-bank checkpoint rotation, atomic COW root publication, atomic Bitmap/Mapping/Volume Descriptor allocation, and optional WAL-persisted B+Tree Arena lifecycle, multi-page Retire Queue, and durable Reader Generation slots. Distributed lease/slot coordination and production SAN fencing remain open.
* P3-4: multi-node leases, AG coordination, shared reader visibility and SAN fencing.
* P3-5: crash recovery and fault-injection tests.
* **P3-3.10:** per-volume Recovery Controller lifecycle supervisor, production dependency validation, and cancellable recovery scans/collection. P3-3.11 adds a retryable `VolumeRuntimeManager` that sequences device open, RW Owner Lease, WAL recovery, fixed-LBA Store/ReaderTable/PersistentArena construction, RO Reader Session, recovery supervision, request admission, I/O drain and reverse-order teardown. Its `VolumeRuntimeBackend` is an explicit production adapter contract; the real Linux block-device/sanlock/DM drain backend is not yet implemented. The current `cmd/bm-meta` remains a JSON/WAL demonstrator and is not yet wired to the fixed-LBA runtime. See [docs/p3-3.11.md](docs/p3-3.11.md).
* P3-6: Linux DM Target integration.

See [docs/p3-metadata-layout.md](docs/p3-metadata-layout.md), [docs/p3-arena.md](docs/p3-arena.md), [docs/p3-wal.md](docs/p3-wal.md), and [docs/p3-reader-fencing.md](docs/p3-reader-fencing.md) for the on-disk layout, page lifecycle, WAL contract, and sanlock/SCSI PR integration.

## Layout

* `internal/device`: random-access durable device interface and file-backed test device.
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
go run ./cmd/bm-meta -wal /var/lib/blockmesh/vol-a.wal -state /var/lib/blockmesh/vol-a.state
```

The CLI is a local demonstrator only. It does not activate a DM device or acquire real cluster leases.
