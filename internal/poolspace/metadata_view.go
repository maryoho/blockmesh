package poolspace

import (
	"errors"
	"io"
	"math"

	"blockmesh/internal/device"
)

// MetadataView exposes the dynamic Metadata extents as one logical byte device.
// The first BootstrapSize bytes are excluded: Bootstrap records, Extent Maps and
// Bootstrap WAL remain reachable only through the parent Pool device.
type MetadataView struct {
	parent device.Device
	extents []Extent
	starts []uint64
	size uint64
}

func NewMetadataView(parent device.Device, extents []Extent) (*MetadataView, error) {
	if parent == nil || parent.Size() < 0 || len(extents) == 0 {
		return nil, ErrInvalidGeometry
	}
	v := &MetadataView{parent: parent}
	var logical, previousEnd uint64
	var bootstrapCovered uint64
	for _, e := range extents {
		if e.Kind != Metadata || e.Length == 0 || e.Start > math.MaxUint64-e.Length ||
			e.Start+e.Length > uint64(parent.Size()) || e.Start < previousEnd {
			return nil, ErrInvalidGeometry
		}
		end := e.Start + e.Length
		previousEnd = end
		if e.Start < BootstrapSize {
			if e.Start > bootstrapCovered {
				return nil, ErrInvalidGeometry
			}
			if end > BootstrapSize {
				bootstrapCovered = BootstrapSize
				e.Length = end - BootstrapSize
				e.Start = BootstrapSize
			} else {
				bootstrapCovered = end
				continue
			}
		}
		if e.Start < BootstrapSize {
			return nil, ErrInvalidGeometry
		}
		if e.Length == 0 {
			continue
		}
		v.extents = append(v.extents, e)
		v.starts = append(v.starts, logical)
		if logical > math.MaxUint64-e.Length {
			return nil, ErrInvalidGeometry
		}
		logical += e.Length
	}
	if bootstrapCovered != BootstrapSize || logical == 0 || logical > uint64(math.MaxInt64) {
		return nil, ErrInvalidGeometry
	}
	v.size = logical
	return v, nil
}

// Close releases only the logical view; the shared parent LUN remains owned by BootstrapStore.
func (v *MetadataView) Close() error { return nil }

func (v *MetadataView) Size() int64 {
	if v == nil { return 0 }
	return int64(v.size)
}
func (v *MetadataView) Sync() error {
	if v == nil || v.parent == nil { return ErrInvalidGeometry }
	return v.parent.Sync()
}
func (v *MetadataView) ReadAt(p []byte, off int64) (int, error) {
	if v == nil || v.parent == nil { return 0, ErrInvalidGeometry }
	if off < 0 { return 0, errors.New("negative MetadataView read offset") }
	if uint64(off) >= v.size {
		if len(p) == 0 && uint64(off) == v.size { return 0, nil }
		return 0, io.EOF
	}
	remaining := v.size - uint64(off)
	want := uint64(len(p))
	short := want > remaining
	if short { want = remaining }
	var done uint64
	for done < want {
		logical := uint64(off) + done
		i := v.findExtent(logical)
		if i < 0 { return int(done), ErrInvalidGeometry }
		within := logical - v.starts[i]
		n := v.extents[i].Length - within
		if n > want-done { n = want-done }
		got, err := v.parent.ReadAt(p[int(done):int(done+n)], int64(v.extents[i].Start+within))
		done += uint64(got)
		if err != nil { return int(done), err }
		if uint64(got) != n { return int(done), io.ErrUnexpectedEOF }
	}
	if short { return int(done), io.EOF }
	return int(done), nil
}
func (v *MetadataView) WriteAt(p []byte, off int64) (int, error) {
	if v == nil || v.parent == nil { return 0, ErrInvalidGeometry }
	if off < 0 { return 0, errors.New("negative MetadataView write offset") }
	if uint64(off) > v.size || uint64(len(p)) > v.size-uint64(off) {
		return 0, io.ErrShortWrite
	}
	var done uint64
	for done < uint64(len(p)) {
		logical := uint64(off) + done
		i := v.findExtent(logical)
		if i < 0 { return int(done), ErrInvalidGeometry }
		within := logical - v.starts[i]
		n := v.extents[i].Length - within
		if n > uint64(len(p))-done { n = uint64(len(p))-done }
		wrote, err := v.parent.WriteAt(p[int(done):int(done+n)], int64(v.extents[i].Start+within))
		done += uint64(wrote)
		if err != nil { return int(done), err }
		if uint64(wrote) != n { return int(done), io.ErrShortWrite }
	}
	return int(done), nil
}
func (v *MetadataView) findExtent(logical uint64) int {
	lo, hi := 0, len(v.starts)
	for lo < hi {
		mid := (lo + hi) / 2
		if v.starts[mid] <= logical { lo = mid + 1 } else { hi = mid }
	}
	i := lo - 1
	if i < 0 || logical-v.starts[i] >= v.extents[i].Length { return -1 }
	return i
}
