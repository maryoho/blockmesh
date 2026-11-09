package metaservice

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"blockmesh/internal/allocator"
	"blockmesh/internal/dynamicruntime"
	"blockmesh/internal/metastore"
	"blockmesh/internal/uapi"
)

// executeAllocateZeroPublish connects the BMPOOL03 DM request to the durable
// PoolAllocator transaction. The allocator holds the distributed AG lock,
// zeros and flushes the new DataView extent, then publishes Bitmap + Mapping +
// idempotency record + B+Tree root in one cluster-safe WAL transaction.
func (p *DynamicPoolRuntimeProcessor) executeAllocateZeroPublish(
	ctx context.Context, req uapi.Request, snapshot dynamicruntime.RuntimeRequestSnapshot,
) (Result, error) {
	rt := snapshot.Runtime
	if snapshot.Spec.Mode != metastore.VolumeRuntimeReadWrite ||
		snapshot.OwnerEpoch == 0 || snapshot.OwnerEpoch != req.OwnerEpoch ||
		rt == nil || rt.Owner == nil || rt.Allocator == nil || rt.Tree == nil {
		return Result{}, ErrFenced
	}
	if !rt.Owner.Valid() || rt.Owner.Epoch() != req.OwnerEpoch {
		return Result{}, ErrFenced
	}

	fingerprint := allocationFingerprint(req)
	allocation, err := rt.Allocator.AllocateZeroPublishTxn(ctx, rt.Owner, allocator.AllocateRequest{
		IdempotencyKey: req.IdempotencyKey,
		Fingerprint: fingerprint,
		LogicalStart: req.LogicalStart,
		BlockCount: req.BlockCount,
		Operation: uint16(req.Operation),
	})
	if err != nil {
		if errors.Is(err, allocator.ErrAllocationOutcomeUnknown) {
			// A committed-vs-uncommitted WAL result cannot be guessed. Fence and
			// drain this runtime before allowing recovery/reopen to decide it.
			drainErr := p.manager.Deactivate(context.Background(), snapshot.Spec.VolumeID)
			return Result{}, errors.Join(ErrRetry, err, drainErr)
		}
		if errors.Is(err, allocator.ErrLogicalRangeMapped) {
			return Result{}, fmt.Errorf("%w: allocate request targets an already mapped logical block: %v", ErrInvalid, err)
		}
		if errors.Is(err, allocator.ErrIdempotencyConflict) {
			return Result{}, fmt.Errorf("%w: %v", ErrInvalid, err)
		}
		if errors.Is(err, allocator.ErrPoolAllocatorUnavailable) {
			return Result{}, errors.Join(ErrFenced, err)
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return Result{}, errors.Join(ErrRetry, err)
		}
		return Result{}, fmt.Errorf("BMPOOL03 allocate-zero-publish: %w", err)
	}
	if !rt.Owner.Valid() || rt.Owner.Epoch() != req.OwnerEpoch {
		// Publication may have completed, but the Owner proof is no longer
		// valid. Do not report success to a caller under a lost epoch.
		drainErr := p.manager.Deactivate(context.Background(), snapshot.Spec.VolumeID)
		return Result{}, errors.Join(ErrFenced, drainErr)
	}
	return Result{Mapping: &uapi.Mapping{LogicalBlock: allocation.LogicalStart, PhysicalBlock: allocation.PhysicalStart, BlockCount: allocation.BlockCount}, RootGeneration: rt.Tree.RootGeneration()}, nil
}

// allocationFingerprint binds an idempotency key to the complete immutable
// request identity and allocation range. OwnerEpoch, NodeID, BootID, RequestID and QueueGeneration are intentionally
// excluded: the same durable operation must replay after an Owner takeover.
func allocationFingerprint(req uapi.Request) [32]byte {
	b := make([]byte, 0, 16+8+4+2)
	b = append(b, req.VolumeID[:]...)
	var word [8]byte
	binary.LittleEndian.PutUint64(word[:], req.LogicalStart)
	b = append(b, word[:]...)
	binary.LittleEndian.PutUint32(word[:4], req.BlockCount)
	b = append(b, word[:4]...)
	binary.LittleEndian.PutUint16(word[:2], uint16(req.Operation))
	b = append(b, word[:2]...)
	return sha256.Sum256(b)
}
