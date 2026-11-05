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
* **P3-3.12 / P3-3.12.1 / P3-3.12.2 / P3-3.12.3 (in progress):** Linux block-device access; unified BMPOOL02 Pool Superblock and BMDVOL02 Volume Descriptor v2; Pool-level `bm-meta format/inspect` and per-Volume `volume-init/volume-inspect`; Pool-backed `metastore.OpenPoolVolume` uses descriptor-owned Arena/Lifecycle extents, a deterministic 128-page per-Volume Reader Table tail, and publishes the inactive Volume Descriptor as the final WAL after-image. The legacy standalone `metastore.Format/Open` remain for isolated compatibility tests only. P3-3.12.3.1 gates RW admission on durable Owner Epoch publication. P3-3.12.3.2 adds PoolVolumeRuntimeBackend composition for descriptor/AG discovery, cluster-safe committer injection, Reader Table/Arena construction, sanlock Reader Slot leases, SCSI PR registration, durable Reader heartbeats, fail-stop callbacks, and the default Reader Recovery Controller. P3-3.12.3.3 adds the Owner takeover coordinator: exclusive distributed sanlock lease, authoritative OwnerRecord CAS, old-owner all-path PR PREEMPT AND ABORT, old-owner I/O barrier, strictly increasing epoch, monitored lease loss, synchronous local fail-stop, and request/drain teardown. P3-3.12.3.4 originally prototyped a Kubernetes Lease OwnerRecordStore, but that adapter and its RBAC manifest have been removed: Kubernetes is not a BlockMesh consistency dependency. The current OwnerRecord is stored in two checksummed 4 KiB slots at the start of each Pool Volume's reserved metadata slot; Pool layout, Volume initialization and Pool-backed open validate the extent, and `Store.OwnerRecordRange()` supplies exact byte offsets. The Arena, Lifecycle and Reader Table are placed after/beside it without overlap. sanlock serializes cross-node CAS updates, and the inactive slot is flushed before publication. P3-3.12.3.5 tests two-node takeover against one shared SAN metadata device, including simultaneous acquisition, serialized sanlock ownership, multipath fencing failure, and ambiguous SAN write/flush outcomes. P3-3.12.3.6 adds the read-only `bm-san-qualify` evidence collector and all-listed-path old PR-key absence verifier; it captures multipath, DM, sanlock, WWID, PR registration and reservation state without changing SAN/host state. Actual power-loss, path-isolation, sanlock-helper and DM fail-stop fault injection remains operator-controlled and requires a disposable SAN LUN. See [docs/p3-3.12.3.6.md](docs/p3-3.12.3.6.md). P3-3.12.3.7 hardens SCSI PR path-scoped local keys, performs all-path preflight before mutation, rolls back partial Reader registrations on ambiguous command outcomes, and verifies local/target keys on the correct paths. See [docs/p3-3.12.3.7.md](docs/p3-3.12.3.7.md). P3-3.12.3.8 extends `bm-san-qualify` to parse PRIN READ RESERVATION results and verify the expected Owner holder key and reservation type on every multipath component path, reporting per-path state and common WWID; this remains read-only evidence verification. See [docs/p3-3.12.3.8.md](docs/p3-3.12.3.8.md). P3-3.12.3.9 requires the Owner takeover coordinator to register path-scoped keys before PREEMPT, fence/drain the previous Owner, CAS-publish the new identity before PR OUT RESERVE, and verify holder/type through every component path, and return an Owner lease only after successful verification; ambiguous RESERVE outcomes retain the durable identity and PR registrations for safe next-owner fencing. See [docs/p3-3.12.3.9.md](docs/p3-3.12.3.9.md). P3-3.12.3.10 adds an explicit monotonic RW/RO activation state machine and requires Pool RW leases to carry matching durable OwnerRecord, verified previous-owner fencing and I/O drain, all-path SCSI PR reservation, synchronous sanlock health and monitored lease-loss proof before WAL recovery and OwnerEpoch publication; final DM admission receives a guard that must be checked under the queue-admission lock immediately before enabling requests. See [docs/p3-3.12.3.10.md](docs/p3-3.12.3.10.md). OwnerRecord is authoritative on the shared SAN metadata device; sanlock provides cross-node serialization, while SCSI PR remains the actual fencing mechanism. Kubernetes/KubeVirt are optional deployment environments only. Deployments must still reserve non-overlapping OwnerRecord slots and provide a trusted PR key resolver, old-owner drain proof, WAL recovery/committer factory, complete B+Tree live-page enumeration, and real DM admission/fail-stop/drain hooks. See [docs/p3-3.12.md](docs/p3-3.12.md), [docs/p3-3.12.1.md](docs/p3-3.12.1.md), [docs/p3-3.12.2.md](docs/p3-3.12.2.md), [docs/p3-3.12.3.2.md](docs/p3-3.12.3.2.md), [docs/p3-3.12.3.3.md](docs/p3-3.12.3.3.md), [docs/p3-3.12.3.4.md](docs/p3-3.12.3.4.md), and [docs/p3-3.12.3.5.md](docs/p3-3.12.3.5.md).
* P3-6: Linux DM Target integration.

See [docs/p3-metadata-layout.md](docs/p3-metadata-layout.md), [docs/p3-arena.md](docs/p3-arena.md), [docs/p3-wal.md](docs/p3-wal.md), and [docs/p3-reader-fencing.md](docs/p3-reader-fencing.md) for the on-disk layout, page lifecycle, WAL contract, and sanlock/SCSI PR integration.

## Layout

* `internal/device`: random-access durable device interface, file-backed test device, and Linux block-device backend.
* `internal/metastore`: fixed-LBA 4 KiB metadata pages, legacy standalone and Pool-backed Volume Store open paths, BMDVOL02 root publication, process-local and WAL-persisted Arena lifecycle, multi-page Retire Queue, durable Reader Generation slots, recovery supervision, and retryable per-Volume Runtime lifecycle.
* `internal/wal`: per-AG fixed-LBA CRC32C redo WAL with Intent/Commit, recovery-before-open, dual-bank checkpoint rotation, and `SharedCommitter` for cross-process AG serialization.
* `internal/metadata`: current JSON-backed reference engine, descriptor and retire queue.
* `internal/allocator`: 4 MiB allocation bitmap and per-AG WAL transaction publishing Bitmap, immutable Mapping COW page, and Volume Descriptor.
* `internal/fence`: SCSI PR registration, PREEMPT AND ABORT fencing, Owner PR RESERVE establishment with all-path verification, and test-only no-op implementation.
* `internal/lock`: local test locker and sanlock-backed distributed resource leases.
* `cmd/bm-lock-helper`: parent-death-bound helper used by sanlock client command.
* `internal/volume`: volume lease contract, test-only in-memory lease, process-lifetime sanlock reader leases, monitored Owner takeover coordinator, and dual-slot SAN-backed OwnerRecordStore.

## Run

```bash
go test ./...

# Read-only Pool Superblock inspection
go run ./cmd/bm-meta inspect --device /dev/mapper/mpath-meta

# Initialize / inspect one Pool Volume descriptor (requires external quiesce/fencing)
go run ./cmd/bm-meta volume-init --device /dev/mapper/mpath-meta --index 3 --volume-id volume-0003 --owner-epoch 1 --confirm-init
go run ./cmd/bm-meta volume-inspect --device /dev/mapper/mpath-meta --index 3

# Destructive initialization: calculate geometry for the actual LUN first
go run ./cmd/bm-meta format --device /dev/mapper/mpath-meta --data-start-sector 0 --confirm-format

# Isolated JSON/WAL reference demo
go run ./cmd/bm-meta demo -wal /var/lib/blockmesh/vol-a.wal -state /var/lib/blockmesh/vol-a.state
```

`format` / `inspect` operate on a real Linux block device and the BMPOOL02 Pool layout; `format` is destructive and its local `O_EXCL` open is not a distributed lease. The `demo` subcommand still uses JSON files, `MemoryLease`, and `NoopFencer`; it is not production-safe and does not activate a DM device. See [docs/p3-3.12.md](docs/p3-3.12.md) before using the block-device commands.
