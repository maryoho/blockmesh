package metastore

import (
	"errors"
	"fmt"
	"math"

	"blockmesh/internal/format"
)

// OpenPoolVolume opens one Volume whose metadata pages reside in the private
// Arena and Lifecycle extents of a BMPOOL02 Pool. The supplied device is the
// whole Pool device; the caller owns its lifecycle. Unlike Open, this function
// never reads or writes a legacy BMMETA01 superblock at LBA 0/1.
func OpenPoolVolume(dev Device, pool format.Superblock, index uint32, expectedVolumeID string) (*Store, error) {
	if dev == nil {
		return nil, errors.New("nil Pool metadata device")
	}
	if err := pool.Validate(); err != nil {
		return nil, fmt.Errorf("invalid Pool Superblock: %w", err)
	}
	if devSize, ok := dev.(interface{ Size() int64 }); ok {
		if devSize.Size() < 0 || uint64(devSize.Size()) < pool.MetadataSize {
			return nil, errors.New("metadata device is smaller than Pool geometry")
		}
	}
	pair, err := format.VolumeDescriptorPairOffset(pool, index)
	if err != nil {
		return nil, err
	}
	d, slot, err := readPoolVolumeDescriptor(dev, pool, pair, expectedVolumeID)
	if err != nil {
		return nil, err
	}
	sb := Superblock{
		VolumeID: d.VolumeID, Generation: d.Generation,
		TotalPages: pool.MetadataSize / PageSize,
		ArenaStart: d.ArenaStart, ArenaPages: d.ArenaPages,
		RootPage: d.RootPage, MappingSequence: d.MappingSequence,
		LifecycleStart: d.LifecycleStart, LifecyclePages: d.LifecyclePages,
	}
	if err := validatePoolStoreGeometry(sb, pool, index); err != nil {
		return nil, err
	}
	return &Store{
		dev: dev, sb: sb, sbSlot: slot,
		poolBacked: true, poolSB: pool, descriptor: d,
		descriptorPair: pair, volumeIndex: index,
	}, nil
}

func validatePoolStoreGeometry(sb Superblock, pool format.Superblock, index uint32) error {
	if sb.TotalPages != pool.MetadataSize/PageSize ||
		sb.ArenaStart < pool.ReservedOffset/PageSize ||
		sb.ArenaStart > sb.TotalPages ||
		sb.ArenaPages > sb.TotalPages-sb.ArenaStart {
		return ErrGeometry
	}
	reservedBytes := pool.MetadataSize - pool.ReservedOffset
	slotBytes := (reservedBytes / uint64(format.MaxVolumeDescriptors)) / PageSize * PageSize
	if slotBytes < (format.OwnerRecordPages+format.ReaderTablePages+3)*PageSize { return ErrGeometry }
	slotStart := pool.ReservedOffset/PageSize + uint64(index)*(slotBytes/PageSize)
	slotPages := slotBytes/PageSize
	ownerEnd:=slotStart+format.OwnerRecordPages
	readerStart:=slotStart+slotPages-format.ReaderTablePages
	if slotStart > sb.TotalPages || slotPages > sb.TotalPages-slotStart ||
		ownerEnd > readerStart || sb.ArenaStart < ownerEnd || sb.ArenaStart+sb.ArenaPages > readerStart {
		return ErrGeometry
	}
	if sb.LifecyclePages != 0 {
		if sb.LifecycleStart < sb.ArenaStart+sb.ArenaPages ||
			sb.LifecycleStart > readerStart ||
			sb.LifecyclePages > readerStart-sb.LifecycleStart {
			return ErrGeometry
		}
	}
	if sb.RootPage != NoPage &&
		(sb.RootPage < sb.ArenaStart || sb.RootPage-sb.ArenaStart >= sb.ArenaPages) {
		return ErrGeometry
	}
	return nil
}

// OwnerRecordRange returns the two reserved SAN OwnerRecord pages as byte offsets.
	// Pass Store.Device() and these offsets to volume.NewSANOwnerRecordStore.
	// The returned pages are private to this Volume and excluded from Arena,
	// Lifecycle and Reader Table geometry.
func (s *Store) OwnerRecordRange() (uint64, uint64, error) {
		if s==nil{return 0,0,ErrGeometry}
		if s.dynamicPool{
			s.mu.Lock();defer s.mu.Unlock()
			if s.dynamicDescriptor.OwnerRecordStart==0{return 0,0,ErrGeometry}
			start:=s.dynamicDescriptor.OwnerRecordStart*PageSize
			return start,start+PageSize,nil
		}
		if !s.poolBacked { return 0, 0, ErrGeometry }
		reservedBytes := s.poolSB.MetadataSize - s.poolSB.ReservedOffset
		slotBytes := (reservedBytes / uint64(format.MaxVolumeDescriptors)) / PageSize * PageSize
		slotPages := slotBytes / PageSize
		slotStart := s.poolSB.ReservedOffset/PageSize + uint64(s.volumeIndex)*slotPages
		if slotPages < format.OwnerRecordPages+format.ReaderTablePages+1 { return 0, 0, ErrGeometry }
		start := slotStart * PageSize
		return start, start + PageSize, nil
	}

	// ReaderTableRange returns the fixed per-Volume Reader Table extent. The
// range is the final ReaderTablePages of this Volume's bootstrap slot; old
// descriptors that consumed the entire slot do not have this facility.
func (s *Store) ReaderTableRange() (uint64, uint32, error) {
	if s == nil || !s.poolBacked { return 0, 0, ErrGeometry }
	reservedBytes := s.poolSB.MetadataSize - s.poolSB.ReservedOffset
	slotBytes := (reservedBytes / uint64(format.MaxVolumeDescriptors)) / PageSize * PageSize
	slotPages := slotBytes / PageSize
	slotStart := s.poolSB.ReservedOffset/PageSize + uint64(s.volumeIndex)*slotPages
	if slotPages < format.ReaderTablePages { return 0, 0, ErrGeometry }
	start := slotStart + slotPages - format.ReaderTablePages
	if start < s.sb.ArenaStart+s.sb.ArenaPages { return 0, 0, ErrGeometry }
	if s.sb.LifecyclePages != 0 && start < s.sb.LifecycleStart+s.sb.LifecyclePages {
		return 0, 0, ErrGeometry
	}
	return start, uint32(format.ReaderTablePages), nil
}

func readPoolVolumeDescriptor(dev Device, pool format.Superblock, pair uint64, expected string) (format.VolumeDescriptor, uint64, error) {
	var best format.VolumeDescriptor
	var bestSlot uint64
	found := false
	for slot := uint64(0); slot < 2; slot++ {
		b := make([]byte, PageSize)
		if err := readFullAt(dev, b, int64(pair+slot*PageSize)); err != nil {
			return format.VolumeDescriptor{}, 0, err
		}
		d, err := format.DecodeVolumeDescriptor(b)
		if err != nil {
			continue
		}
		if err = d.ValidatePool(pool); err != nil {
			continue
		}
		if found && d.VolumeID != best.VolumeID {
			return format.VolumeDescriptor{}, 0, errors.New("Volume Descriptor copies contain different Volume IDs")
		}
		if expected != "" && d.VolumeID != expected {
			return format.VolumeDescriptor{}, 0, fmt.Errorf("Volume Descriptor belongs to %q, not %q", d.VolumeID, expected)
		}
		if found && d.Generation == best.Generation && d != best {
			return format.VolumeDescriptor{}, 0, fmt.Errorf("conflicting Volume Descriptor copies at generation %d", d.Generation)
		}
		if !found || d.Generation > best.Generation {
			best, bestSlot, found = d, slot, true
		}
	}
	if !found {
		return format.VolumeDescriptor{}, 0, ErrUnformatted
	}
	return best, bestSlot, nil
}

func (s *Store) refreshPoolLocked() error {
	d, slot, err := readPoolVolumeDescriptor(s.dev, s.poolSB, s.descriptorPair, s.sb.VolumeID)
	if err != nil {
		return err
	}
	if d.ArenaStart != s.descriptor.ArenaStart || d.ArenaPages != s.descriptor.ArenaPages ||
		d.LifecycleStart != s.descriptor.LifecycleStart || d.LifecyclePages != s.descriptor.LifecyclePages {
		return ErrGeometry
	}
	candidate := Superblock{
		VolumeID: d.VolumeID, Generation: d.Generation, TotalPages: s.poolSB.MetadataSize/PageSize,
		ArenaStart: d.ArenaStart, ArenaPages: d.ArenaPages, RootPage: d.RootPage,
		MappingSequence: d.MappingSequence, LifecycleStart: d.LifecycleStart, LifecyclePages: d.LifecyclePages,
	}
	if err := validatePoolStoreGeometry(candidate, s.poolSB, s.volumeIndex); err != nil { return err }
	if d.Generation > s.descriptor.Generation {
		s.descriptor = d
		s.sb.Generation = d.Generation
		s.sb.RootPage = d.RootPage
		s.sb.MappingSequence = d.MappingSequence
		s.sbSlot = slot
	}
	return nil
}

func (s *Store) poolDescriptorAfterImage(rootPage, mappingSequence uint64) (format.VolumeDescriptor, uint64, []byte, error) {
	if s.descriptor.Generation == math.MaxUint64 || s.descriptor.VolumeSequence == math.MaxUint64 {
		return format.VolumeDescriptor{}, 0, nil, errors.New("Volume Descriptor sequence exhausted")
	}
	next := s.descriptor
	next.Generation++
	next.VolumeSequence++
	next.RootPage = rootPage
	next.MappingSequence = mappingSequence
	if err := next.ValidatePool(s.poolSB); err != nil {
		return format.VolumeDescriptor{}, 0, nil, err
	}
	candidate := Superblock{
		VolumeID: next.VolumeID, Generation: next.Generation, TotalPages: s.poolSB.MetadataSize/PageSize,
		ArenaStart: next.ArenaStart, ArenaPages: next.ArenaPages, RootPage: next.RootPage,
		MappingSequence: next.MappingSequence, LifecycleStart: next.LifecycleStart, LifecyclePages: next.LifecyclePages,
	}
	if err := validatePoolStoreGeometry(candidate, s.poolSB, s.volumeIndex); err != nil {
		return format.VolumeDescriptor{}, 0, nil, err
	}
	image, err := next.Encode()
	if err != nil {
		return format.VolumeDescriptor{}, 0, nil, err
	}
	return next, uint64(1) - s.sbSlot, image, nil
}
