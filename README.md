# BlockMesh P3 metadata engine (reference implementation)

This repository is a buildable P3 control-plane reference for Linux 5.15 BlockMesh.

## Selected consistency semantics

* RO readers use an online-consistent view: committed root changes become visible after refresh.
* A failed reader is reclaimed only after its lease is declared dead **and** SAN fencing succeeds.
* Mapping updates are copy-on-write snapshots. Retired physical blocks and metadata generations are reclaimed only after every active reader has advanced beyond the retire generation or has been fenced.

## Important integration boundary

This code deliberately does not pretend to implement SAN fencing or a distributed lease service. `fence.Fencer` and `volume.Lease` are mandatory integration points. Production implementations must use sanlock (or an equivalent cluster lock service) and SCSI Persistent Reservations with PREEMPT AND ABORT across every multipath path. `MemoryLease` and `NoopFencer` are test-only and MUST NOT be used in production.

The included `metadata.Engine` is still a single-volume JSON snapshot reference engine with an append-only redo WAL. It is not safe for multiple nodes to open the same files. The new `internal/metastore` package starts the migration to a fixed-LBA, 4 KiB-page format with redundant superblocks and an immutable metadata page arena. It is not yet wired into `metadata.Engine` and does not yet provide WAL, allocation, AG locks, distributed reader slots, fencing, or crash-safe page reclamation.

## P3 implementation status

* **P3-1 (in progress):** fixed-LBA metadata pages, dual superblocks, checksums, root publication ordering.
* P3-2: page-level copy-on-write B+Tree.
* P3-3: transactional bitmap/B+Tree/descriptor updates and WAL.
* P3-4: multi-node leases, AG coordination, shared reader visibility and SAN fencing.
* P3-5: crash recovery and fault-injection tests.
* P3-6: Linux DM Target integration.

See [docs/p3-metadata-layout.md](docs/p3-metadata-layout.md) for the version-1 on-disk layout and required persistence ordering.

## Layout

* `internal/device`: random-access durable device interface and file-backed test device.
* `internal/metastore`: fixed-LBA 4 KiB metadata pages and redundant superblocks.
* `internal/wal`: framed CRC32C WAL with Intent and Commit records (reference implementation).
* `internal/metadata`: current JSON-backed reference engine, descriptor and retire queue.
* `internal/allocator`: 4 MiB allocation bitmap primitive.
* `internal/fence`: fencing contract and test-only no-op implementation.
* `internal/volume`: lease contract and test-only in-memory lease.

## Run

```bash
go test ./...
go run ./cmd/bm-meta -wal /var/lib/blockmesh/vol-a.wal -state /var/lib/blockmesh/vol-a.state
```

The CLI is a local demonstrator only. It does not activate a DM device or acquire real cluster leases.
