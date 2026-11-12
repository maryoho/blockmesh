# Dynamic single-device Pool geometry and extent-space foundation

BMPOOL03 is a versioned dynamic Pool format. One backing block device is divided into a Bootstrap reservation and typed physical extents. Metadata and Data extents never overlap, and all extent views share the same parent device.

## Capacity planning

poolspace.PlanDevice computes an initial Metadata budget from actual device capacity and Volume slot count. It budgets Bitmap, two COW Mapping generations, WAL, per-Volume control metadata and headroom, then rounds dynamic Metadata to 1GiB extents. The user does not specify a Metadata target size.

This estimator is a conservative budget model. It is not proof that the existing BMPOOL02 structures have already been migrated to a dynamic layout.

## Persistent Bootstrap and Extent Map

BMPOOL03 reserves 8MiB for two 4KiB Bootstrap records, two alternating 2MiB Extent Map slots, a two-page redo WAL and reserved growth space. Bootstrap records include Pool UUID, Generation, actual device capacity, Metadata capacity, logical sector size, OwnerEpoch and CRC32C.

The inactive Extent Map is written and synced before the inactive Bootstrap after-image. WAL Intent and Commit protect root publication. Recovery selects the highest valid committed generation, ignores uncommitted after-images, and fails closed if a committed map is corrupt.

The extent map validates full-device coverage with no gaps or overlaps. Its current fixed map slots impose a maximum encoded Extent Map size; larger highly fragmented layouts return ErrExtentMapTooLarge rather than silently truncating.

## Physical extent ownership

poolspace.Allocator tracks Metadata, Data and Free ranges on the same physical device. Rescan appends newly visible bytes as Free and rejects shrink. Metadata expansion selects its size internally from usage and growth observations and allocates only whole 1GiB extents. Existing Data extents are never moved.

OpenExtentView provides a bounded view for one physical extent. OpenDataView concatenates fragmented Data extents into a logical Data device, retaining the same parent LUN.

## Format compatibility and operational boundary

DetectFormat distinguishes BMPOOL02 and BMPOOL03. A damaged BMPOOL02 Superblock is still recognized by magic and cannot fall through to the BMPOOL03 formatter. The BMPOOL03 formatter also refuses non-zero bytes in the reserved Bootstrap region. No in-place BMPOOL02 migration is performed.

bmctl provides explicit format, rescan, metadata-expand and metadata-expand-status commands. Mutating commands require a configured helper that holds a real fenced Owner lease, plus a distributed SAN Bootstrap lock. Metadata expansion uses a fresh metadata usage/growth snapshot; the CLI does not invent these counters.

## Remaining integration before production

- The metadata usage snapshot must be sourced from authoritative runtime counters.
- BMPOOL03 is not yet wired into the existing per-AG Bitmap/Mapping WAL transaction. Data extent allocation, zeroing, mapping publication and Extent Map publication still need one crash-recoverable transaction/reconciliation design.
- The configured Owner lease helper must implement the real OwnerCoordinator path: old-owner PR fencing on all paths, reservation verification, durable OwnerRecord CAS and old-owner I/O drain.
- The two-page Bootstrap WAL protects Pool Extent Map root publication only; it does not replace the existing BMPOOL02 AG Journal.
- Sparse-file tests and CI are not substitutes for two-node SAN power-loss and multipath fault qualification.
