package metadata

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"blockmesh/internal/device"
	"blockmesh/internal/format"
)

const bitmapDescriptorMagic = uint64(0x424D424D41503031) // BMBMAP01

type Store struct {
	dev device.Device
	SB  format.Superblock
}

func OpenStore(path string) (*Store, error) {
	return openWithMode(path, false)
}

func OpenStoreWritable(path string) (*Store, error) {
	return openWithMode(path, true)
}

func openWithMode(path string, writable bool) (*Store, error) {
	d, err := device.Open(path, writable)
	if err != nil { return nil, err }
	s, err := OpenStoreDevice(d)
	if err != nil { _ = d.Close(); return nil, err }
	return s, nil
}

// OpenStoreDevice reads and validates the BMPOOL02 redundant superblock pair.
// The caller owns the supplied device.
func OpenStoreDevice(d device.Device) (*Store, error) {
	if d == nil { return nil, fmt.Errorf("nil metadata device") }
	var a, b []byte
	a = make([]byte, format.PageSize)
	b = make([]byte, format.PageSize)
	errA := readAtFull(d, a, int64(format.SuperAOffset))
	errB := readAtFull(d, b, int64(format.SuperBOffset))
	var sa, sb format.Superblock
	if errA == nil {
		sa, errA = format.DecodeSuperblock(a)
	}
	if errB == nil {
		sb, errB = format.DecodeSuperblock(b)
	}
	if errA != nil && errB != nil {
		return nil, fmt.Errorf("no valid superblock: A=%v; B=%v", errA, errB)
	}
	chosen := sa
	if errA != nil || (errB == nil && sb.Generation > sa.Generation) {
		chosen = sb
	}
	return &Store{dev: d, SB: chosen}, nil
}

func Init(path string, dataStartSector uint64, force bool) error {
	d, err := device.Open(path, true)
	if err != nil { return err }
	defer d.Close()
	return InitDevice(d, dataStartSector, force)
}

// InitDevice initializes the Pool-level BMPOOL02 superblock pair and AG
// bitmaps on an already-open metadata device. The caller owns device lifecycle.
func InitDevice(d device.Device, dataStartSector uint64, force bool) error {
	if d == nil { return fmt.Errorf("nil metadata device") }
	if err := format.ValidateLayout(); err != nil { return err }
	size := d.Size()
	if size < 0 || uint64(size) < format.MetadataSize {
		return fmt.Errorf("metadata device too small: have %d bytes, need at least %d", d.Size(), format.MetadataSize)
	}
	if !force {
		buf := make([]byte, format.PageSize)
		if readAtFull(d, buf, 0) == nil {
			for _, v := range buf {
				if v != 0 {
					return fmt.Errorf("device is not empty; pass --force to overwrite")
				}
			}
		}
	}
	// Invalidate redundant Pool Superblocks before writing the new generation,
	// so an interrupted reformat can never select a stale higher-generation copy.
	if err := writeAtFull(d, make([]byte, 2*format.PageSize), 0); err != nil { return err }
	if err := d.Sync(); err != nil { return err }
	// Volume identities and checkpoint descriptors are Pool-scoped. Clear
	// them before publishing the new Pool Superblocks.
	if err := zeroRegion(d, format.NodeTableOffset, format.NodeTableSize); err != nil { return err }
	if err := zeroRegion(d, format.WALOffset, format.WALSize); err != nil { return err }
	if err := zeroRegion(d, format.CheckpointOffset, format.CheckpointSize); err != nil { return err }
	sb, err := format.NewSuperblock(dataStartSector)
	if err != nil {
		return err
	}
	// Initialize every AG descriptor and its eight 4KiB bitmap data pages.
	for ag := uint32(0); ag < format.AGCount; ag++ {
		base := format.BitmapOffset + uint64(ag)*format.AGBitmapStride
		bitmap := make([]byte, format.BitmapBytes)
		if err = writeBitmap(d, ag, base, bitmap, 1); err != nil { return err }
	}
	// Publish the Pool Superblock pair only after every Pool-owned region is initialized.
	if err = writeAtFull(d, sb.Encode(), int64(format.SuperAOffset)); err != nil { return err }
	if err = d.Sync(); err != nil { return err }
	sb.Generation = 2
	if err = writeAtFull(d, sb.Encode(), int64(format.SuperBOffset)); err != nil { return err }
	return d.Sync()
}

func (s *Store) Close() error { return s.dev.Close() }

func (s *Store) Inspect(w io.Writer) error {
	fmt.Fprintf(w, "Pool UUID:          %x\n", s.SB.PoolUUID)
	fmt.Fprintf(w, "Generation:         %d\n", s.SB.Generation)
	fmt.Fprintf(w, "Pool size:          %d bytes (%.2f TiB)\n", s.SB.PoolSize, float64(s.SB.PoolSize)/(1024*1024*1024*1024))
	fmt.Fprintf(w, "Data start sector:  %d\n", s.SB.DataStartSector)
	fmt.Fprintf(w, "Allocation block:   %d bytes\n", format.BlockSize)
	fmt.Fprintf(w, "Total blocks:       %d\n", s.SB.TotalBlocks)
	fmt.Fprintf(w, "AG count:           %d\n", s.SB.AGCount)
	fmt.Fprintf(w, "AG blocks:          %d\n", s.SB.AGSizeBlocks)
	fmt.Fprintf(w, "Bitmap offset:      %d\n", s.SB.BitmapOffset)
	fmt.Fprintf(w, "Bitmap size:        %d bytes\n", s.SB.BitmapSize)
	fmt.Fprintf(w, "WAL offset/size:    %d / %d\n", s.SB.WALOffset, s.SB.WALSize)
	fmt.Fprintf(w, "Mapping offset/size:%d / %d\n", s.SB.MappingOffset, s.SB.MappingSize)
	fmt.Fprintf(w, "Reserved offset:    %d\n", s.SB.ReservedOffset)
	return nil
}

func (s *Store) BitmapStats(ag uint32) (allocated, free uint64, err error) {
	if ag >= s.SB.AGCount {
		err = fmt.Errorf("AG %d out of range", ag)
		return
	}
	bm, _, err := readBitmap(s.dev, ag, s.SB.BitmapOffset+uint64(ag)*format.AGBitmapStride)
	if err != nil {
		return
	}
	for _, b := range bm {
		for x := b; x != 0; x &= x - 1 {
			allocated++
		}
	}
	free = format.AGBlocks - allocated
	return
}

func (s *Store) SetBitmapBit(ag uint32, localBlock uint64, allocated bool) error {
	if ag >= s.SB.AGCount || localBlock >= format.AGBlocks {
		return fmt.Errorf("AG or local block out of range")
	}
	base := s.SB.BitmapOffset + uint64(ag)*format.AGBitmapStride
	bm, seq, err := readBitmap(s.dev, ag, base)
	if err != nil {
		return err
	}
	idx, mask := localBlock/8, byte(1<<(localBlock%8))
	if allocated {
		bm[idx] |= mask
	} else {
		bm[idx] &^= mask
	}
	return writeBitmap(s.dev, ag, base, bm, seq+1)
}

func readBitmap(d device.Device, ag uint32, base uint64) ([]byte, uint64, error) {
	desc := make([]byte, format.PageSize)
	if err := readAtFull(d, desc, int64(base)); err != nil {
		return nil, 0, err
	}
	if binary.LittleEndian.Uint64(desc[0:8]) != bitmapDescriptorMagic {
		return nil, 0, fmt.Errorf("AG %d bad bitmap descriptor magic", ag)
	}
	if binary.LittleEndian.Uint32(desc[8:12]) != ag {
		return nil, 0, fmt.Errorf("AG descriptor ID mismatch")
	}
	seq := binary.LittleEndian.Uint64(desc[16:24])
	want := binary.LittleEndian.Uint32(desc[24:28])
	bm := make([]byte, format.BitmapBytes)
	if err := readAtFull(d, bm, int64(base+format.PageSize)); err != nil {
		return nil, 0, err
	}
	if crc32.Checksum(bm, crc32.MakeTable(crc32.Castagnoli)) != want {
		return nil, 0, fmt.Errorf("AG %d bitmap CRC32C mismatch", ag)
	}
	return bm, seq, nil
}

func writeBitmap(d device.Device, ag uint32, base uint64, bm []byte, seq uint64) error {
	if len(bm) != int(format.BitmapBytes) {
		return fmt.Errorf("invalid bitmap size")
	}
	// Write bitmap data first, then descriptor. This is not a transaction; P2 WAL will provide crash atomicity.
	if err := writeAtFull(d, bm, int64(base+format.PageSize)); err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		return err
	}
	desc := make([]byte, format.PageSize)
	binary.LittleEndian.PutUint64(desc[0:8], bitmapDescriptorMagic)
	binary.LittleEndian.PutUint32(desc[8:12], ag)
	binary.LittleEndian.PutUint32(desc[12:16], 1)
	binary.LittleEndian.PutUint64(desc[16:24], seq)
	binary.LittleEndian.PutUint32(desc[24:28], crc32.Checksum(bm, crc32.MakeTable(crc32.Castagnoli)))
	return writeAtFull(d, desc, int64(base))
}

// Superblock returns the current metadata geometry used by the allocator.
func (s *Store) Superblock() format.Superblock { return s.SB }

// Device exposes the metadata device to the WAL and allocator packages.
// Callers must coordinate all mutations with the configured lock provider.
func (s *Store) Device() device.Device { return s.dev }

// ReadPage reads one aligned 4KiB metadata page.
func (s *Store) ReadPage(offset uint64) ([]byte, error) {
	if offset%format.PageSize != 0 || offset+format.PageSize > s.SB.MetadataSize {
		return nil, fmt.Errorf("invalid metadata page offset %d", offset)
	}
	b := make([]byte, format.PageSize)
	if err := readAtFull(s.dev, b, int64(offset)); err != nil {
		return nil, err
	}
	return b, nil
}

func readAtFull(d device.Device, b []byte, off int64) error {
	n, err := d.ReadAt(b, off)
	if err != nil && !(err == io.EOF && n == len(b)) {
		return err
	}
	if n != len(b) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func writeAtFull(d device.Device, b []byte, off int64) error {
	n, err := d.WriteAt(b, off)
	if err != nil {
		return err
	}
	if n != len(b) {
		return io.ErrShortWrite
	}
	return nil
}

func zeroRegion(d device.Device, offset, length uint64) error {
	const chunk = uint64(1 << 20)
	zero := make([]byte, chunk)
	for length > 0 {
		n := length
		if n > chunk { n = chunk }
		if err := writeAtFull(d, zero[:n], int64(offset)); err != nil { return err }
		offset += n
		length -= n
	}
	return d.Sync()
}
