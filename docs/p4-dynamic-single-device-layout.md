# Dynamic single-device Pool geometry and extent-space foundation

This package is the first implementation slice for the agreed single-device
model. One backing block device is divided into a bootstrap reservation and
typed physical extents. Metadata and data extents are never allowed to overlap.

## Capacity planning

`poolspace.PlanDevice` computes an initial metadata budget from actual device
capacity and Volume slot count. It budgets bitmap bytes, two COW mapping
generations, WAL, per-Volume control metadata, and 30% headroom, then rounds
to 1GiB extents. It does not take a user-supplied metadata size.

The current estimator is deliberately a conservative sizing model, not a
claim that all existing BMPOOL02 structures are already dynamically laid out.
The legacy format still has fixed offsets and must not be formatted using this
plan until the versioned Bootstrap/ExtentMap migration is complete.

## Physical extent ownership

`poolspace.Allocator` tracks contiguous physical ranges as Metadata, Data, or
Free. Rescan only appends newly visible bytes as Free; shrink is rejected.
Metadata expansion allocates only whole 1GiB extents from Free space and is
all-or-nothing. Existing Data extents are never moved.

## Extent map codec

`ExtentMap` has generation, device capacity, contiguous full-device coverage,
typed extents, and CRC32C. The codec validates ordering, gaps, overlap,
bounds, kind values, reserved bytes and checksum.

## Important integration boundary

This foundation is not yet an online expansion implementation. The extent
map codec is not yet stored in dual Bootstrap slots, and allocator mutations
are not yet WAL-committed under the distributed Owner lease. Existing
BMPOOL02 superblocks and PoolVolume descriptors still use the legacy fixed
metadata geometry. The next integration change must introduce a versioned
bootstrap layout and WAL publication before exposing rescan/metadata-expand
as production commands. No command in this commit mutates a live Pool.
