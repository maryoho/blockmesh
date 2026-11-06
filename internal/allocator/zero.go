package allocator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	"blockmesh/internal/format"
)

// DataDevice is the SAN data LUN contract used before metadata publication.
// Sync must provide the platform's durable write barrier (e.g. block-device
// flush through the selected multipath device).
type DataDevice interface {
	io.WriterAt
	Sync() error
	Size() int64
}

// ZeroBlocks overwrites and synchronizes every newly allocated 4MiB block.
// It deliberately does not alter metadata. Callers must hold the distributed
// allocation-group lock until the subsequent bitmap/mapping/idempotency WAL
// commit has reached a known durable outcome.
func ZeroBlocks(ctx context.Context, dev DataDevice, physicalStart uint64, count uint32) error {
	if ctx == nil || dev == nil || count == 0 {
		return errors.New("context, data device and positive block count are required")
	}
	if physicalStart > math.MaxUint64/format.BlockSize ||
		uint64(count) > math.MaxUint64/format.BlockSize-physicalStart {
		return errors.New("physical block range overflows byte offset")
	}
	start := physicalStart * format.BlockSize
	length := uint64(count) * format.BlockSize
	if dev.Size() < 0 || start > uint64(dev.Size()) || length > uint64(dev.Size())-start {
		return fmt.Errorf("zero range [%d,%d) exceeds data device size %d", start, start+length, dev.Size())
	}
	const chunkSize = 1 << 20
	zero := make([]byte, chunkSize)
	for done := uint64(0); done < length; {
		if err := ctx.Err(); err != nil { return err }
		n := uint64(len(zero))
		if length-done < n { n = length-done }
		written := 0
		for written < int(n) {
			m, err := dev.WriteAt(zero[written:int(n)], int64(start+done)+int64(written))
			written += m
			if err != nil { return fmt.Errorf("zero physical block %d at byte %d: %w", physicalStart, start+done+uint64(written-m), err) }
			if m == 0 { return io.ErrShortWrite }
		}
		done += n
	}
	if err := ctx.Err(); err != nil { return err }
	if err := dev.Sync(); err != nil { return fmt.Errorf("durably sync zeroed data blocks: %w", err) }
	return nil
}
