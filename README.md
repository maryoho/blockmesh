# BlockMesh P3 metadata engine (reference implementation)

This repository is a buildable P3 control-plane reference for Linux 5.15 BlockMesh.

## Selected consistency semantics

* RO readers use an online-consistent view: committed root changes become visible after refresh.
* A failed reader is reclaimed only after its lease is declared dead **and** SAN fencing succeeds.
* Mapping updates are copy-on-write snapshots. Retired physical blocks and metadata generations are reclaimed only after every active reader has advanced beyond the retire generation or has been fenced.

## Important integration boundary

This code deliberately does not pretend to implement SAN fencing or a distributed lease service. `fence.Fencer` and `volume.Lease` are mandatory integration points. Production implementations must use sanlock (or an equivalent cluster lock service) and SCSI Persistent Reservations with PREEMPT AND ABORT across every multipath path. `MemoryLease` and `NoopFencer` are test-only and MUST NOT be used in production.

The included engine is a single-volume reference metadata engine with an append-only, checksummed redo WAL and immutable mapping snapshots. It demonstrates the ordering and recovery contract. A production multi-volume deployment should shard WAL by AG, enforce AG locks, persist pool bitmap/metadata-page arena updates in the same transaction, and add crash-safe checkpoint/WAL truncation. The current implementation intentionally refuses WAL truncation rather than risking deletion of uncheckpointed commits.

## Layout

* `internal/device`: random-access durable device interface and file-backed test device.
* `internal/wal`: framed CRC32C WAL with Intent and Commit records.
* `internal/metadata`: engine, COW mapping snapshot, descriptor and retire queue.
* `internal/reader`: reader lifecycle and safe-generation calculation.
* `internal/allocator`: 4 MiB allocation bitmap primitive.
* `internal/fence`: fencing contract and test-only no-op implementation.
* `internal/volume`: lease contract and test-only in-memory lease.

## Run

```bash
go test ./...
go run ./cmd/bm-meta -wal /var/lib/blockmesh/vol-a.wal -state /var/lib/blockmesh/vol-a.state
```

The CLI is a local demonstrator only. It does not activate a DM device or acquire real cluster leases.
