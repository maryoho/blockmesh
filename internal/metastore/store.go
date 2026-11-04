// Package metastore implements the fixed-LBA, 4 KiB-page metadata format,
// process-local reservations, and optional WAL-persisted Arena lifecycle.
// Callers may publish through the legacy page-write/Sync path or commit page,
// lifecycle, and root images together through a WAL Committer.
package metastore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"strings"
	"sync"
)

const (
	PageSize       = 4096
	Superblock0    = uint64(0)
	Superblock1    = uint64(1)
	FormatVersion  = uint16(1)
	LifecycleFormatVersion = uint16(2)
	PageHeaderSize = 32
	PagePayloadMax = PageSize - PageHeaderSize
	NoPage         = uint64(math.MaxUint64)
)

var (
	ErrUnformatted = errors.New("metastore is not formatted")
	ErrCorrupt     = errors.New("metastore page is corrupt")
	ErrOutOfRange  = errors.New("metastore page id out of range")
	ErrGeometry    = errors.New("metastore geometry mismatch")
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type Device interface {
	ReadAt(p []byte, off int64) (int, error)
	WriteAt(p []byte, off int64) (int, error)
	Sync() error
}

type Layout struct {
	VolumeID   string
	TotalPages uint64
	ArenaStart uint64
	ArenaPages uint64
	LifecycleStart uint64
	LifecyclePages uint64
}

type Superblock struct {
	VolumeID        string
	Generation      uint64
	TotalPages      uint64
	ArenaStart      uint64
	ArenaPages      uint64
	RootPage        uint64
	MappingSequence uint64
	LifecycleStart uint64
	LifecyclePages uint64
}

type Page struct {
	ID         uint64
	Type       uint16
	Generation uint64
	Payload    []byte
}

// PageImage is a complete, already encoded 4 KiB metadata after-image.
type PageImage struct {
	Offset uint64
	Data   []byte
}

// Committer durably commits page after-images as one redo-WAL transaction.
// Implementations must not expose any after-image before the transaction's
// commit record is durable.
type Committer interface {
	CommitImages(images []PageImage) error
}

type Store struct {
	mu     sync.RWMutex
	dev    Device
	sb     Superblock
	sbSlot uint64
}

func validateLayout(l Layout) error {
	if strings.TrimSpace(l.VolumeID) == "" || len(l.VolumeID) > 64 {
		return errors.New("volume id must contain 1..64 bytes")
	}
	if l.TotalPages < 3 || l.TotalPages > uint64(math.MaxInt64/PageSize) || l.ArenaPages == 0 || l.ArenaStart < 2 {
		return ErrGeometry
	}
	if l.ArenaStart > l.TotalPages || l.ArenaPages > l.TotalPages-l.ArenaStart {
		return ErrGeometry
	}
	if l.LifecyclePages != 0 {
		if l.LifecyclePages < 2 || l.LifecycleStart < 2 || l.LifecycleStart > l.TotalPages ||
			l.LifecyclePages > l.TotalPages-l.LifecycleStart {
			return ErrGeometry
		}
		arenaEnd := l.ArenaStart + l.ArenaPages
		lifecycleEnd := l.LifecycleStart + l.LifecyclePages
		if l.LifecycleStart < arenaEnd && lifecycleEnd > l.ArenaStart {
			return ErrGeometry
		}
	}
	return nil
}

// Format initializes both redundant superblocks. The generation-1 copy is
// written first; generation-0 is a valid fallback if the first copy is later
// damaged. The caller must ensure the device is exclusively owned.
func Format(dev Device, l Layout) (*Store, error) {
	if dev == nil {
		return nil, errors.New("nil metadata device")
	}
	if err := validateLayout(l); err != nil {
		return nil, err
	}
	// Invalidate both old copies first. Otherwise a stale superblock left by a
	// previous format could have a larger generation and win during Open if
	// formatting is interrupted before both new copies are written.
	if err := writeFullAt(dev, make([]byte, 2*PageSize), 0); err != nil {
		return nil, err
	}
	if err := dev.Sync(); err != nil {
		return nil, err
	}
	if l.LifecyclePages >= 2 {
		zero := make([]byte, PageSize)
		for i := uint64(0); i < 2; i++ {
			if err := writeFullAt(dev, zero, int64((l.LifecycleStart+i)*PageSize)); err != nil { return nil, err }
		}
		if err := dev.Sync(); err != nil { return nil, err }
	}
	sb := Superblock{VolumeID: l.VolumeID, Generation: 1, TotalPages: l.TotalPages,
		ArenaStart: l.ArenaStart, ArenaPages: l.ArenaPages, RootPage: NoPage, LifecycleStart:l.LifecycleStart, LifecyclePages:l.LifecyclePages}

	b, err := encodeSuperblock(sb)
	if err != nil {
		return nil, err
	}
	if err = writeFullAt(dev, b, int64(Superblock1*PageSize)); err != nil {
		return nil, err
	}
	if err = dev.Sync(); err != nil {
		return nil, err
	}
	fallback := sb
	fallback.Generation = 0
	b, err = encodeSuperblock(fallback)
	if err != nil {
		return nil, err
	}
	if err = writeFullAt(dev, b, int64(Superblock0*PageSize)); err != nil {
		return nil, err
	}
	if err = dev.Sync(); err != nil {
		return nil, err
	}
	return &Store{dev: dev, sb: sb, sbSlot: Superblock1}, nil
}

func Open(dev Device, expectedVolumeID string) (*Store, error) {
	if dev == nil {
		return nil, errors.New("nil metadata device")
	}
	var candidates []struct {
		sb   Superblock
		slot uint64
	}
	for _, slot := range []uint64{Superblock0, Superblock1} {
		b := make([]byte, PageSize)
		if err := readFullAt(dev, b, int64(slot*PageSize)); err != nil {
			continue
		}
		sb, err := decodeSuperblock(b)
		if err == nil && (expectedVolumeID == "" || sb.VolumeID == expectedVolumeID) {
			candidates = append(candidates, struct {
				sb   Superblock
				slot uint64
			}{sb, slot})
		}
	}
	if len(candidates) == 0 {
		return nil, ErrUnformatted
	}
	best := candidates[0]
	for _, c := range candidates[1:] {
		if c.sb.Generation > best.sb.Generation {
			best = c
		}
	}
	return &Store{dev: dev, sb: best.sb, sbSlot: best.slot}, nil
}

func (s *Store) Superblock() Superblock {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sb
}

// Publish atomically advances the root descriptor by writing the inactive
// superblock and flushing it. It never overwrites the currently selected copy.
func (s *Store) Publish(rootPage, mappingSequence uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rootPage != NoPage && !s.inArena(rootPage) {
		return ErrOutOfRange
	}
	if mappingSequence <= s.sb.MappingSequence {
		return errors.New("mapping sequence must increase")
	}
	if s.sb.Generation == math.MaxUint64 {
		return errors.New("superblock generation exhausted")
	}
	for _, image := range extra {
		if image.Offset%PageSize != 0 || len(image.Data) != PageSize || image.Offset > s.sb.TotalPages*PageSize || PageSize > s.sb.TotalPages*PageSize-image.Offset {
			return fmt.Errorf("invalid extra metadata after-image at offset %d", image.Offset)
		}
		if s.sb.LifecyclePages == 0 || image.Offset < s.sb.LifecycleStart*PageSize ||
			image.Offset >= (s.sb.LifecycleStart+s.sb.LifecyclePages)*PageSize {
			return fmt.Errorf("extra metadata after-image %d is outside lifecycle region", image.Offset)
		}
		if _, exists := seen[image.Offset/PageSize]; exists { return fmt.Errorf("duplicate metadata after-image at offset %d", image.Offset) }
		seen[image.Offset/PageSize] = struct{}{}
		images = append(images, PageImage{Offset:image.Offset, Data:append([]byte(nil), image.Data...)})
	}
	next := s.sb
	next.Generation++
	next.RootPage = rootPage
	next.MappingSequence = mappingSequence
	slot := uint64(1) - s.sbSlot
	b, err := encodeSuperblock(next)
	if err != nil {
		return err
	}
	if err = writeFullAt(s.dev, b, int64(slot*PageSize)); err != nil {
		return err
	}
	if err = s.dev.Sync(); err != nil {
		return err
	}
	s.sb, s.sbSlot = next, slot
	return nil
}

// CommitPagesAndPublish commits immutable COW pages and the inactive
// superblock root descriptor in one WAL transaction. The root descriptor is
// appended last, so recovery can never publish a root before its pages.
func (s *Store) CommitPagesAndPublish(pages []Page, rootPage, mappingSequence uint64, committer Committer) error {
	return s.CommitPagesAndPublishWithImages(pages, nil, rootPage, mappingSequence, committer)
}

// CommitPagesAndPublishWithImages commits COW pages, caller-supplied durable
// lifecycle images, and the inactive root descriptor in one WAL transaction.
// Lifecycle images are applied after data pages and before the root descriptor.
func (s *Store) CommitPagesAndPublishWithImages(pages []Page, extra []PageImage, rootPage, mappingSequence uint64, committer Committer) error {
	if committer == nil {
		return errors.New("nil metadata WAL committer")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if rootPage != NoPage && !s.inArena(rootPage) {
		return ErrOutOfRange
	}
	if mappingSequence <= s.sb.MappingSequence {
		return errors.New("mapping sequence must increase")
	}
	if s.sb.Generation == math.MaxUint64 {
		return errors.New("superblock generation exhausted")
	}

	images := make([]PageImage, 0, len(pages)+len(extra)+1)
	seen := make(map[uint64]struct{}, len(pages))
	rootIncluded := rootPage == NoPage
	for _, page := range pages {
		if !s.inArena(page.ID) {
			return fmt.Errorf("%w: COW page %d", ErrOutOfRange, page.ID)
		}
		if _, exists := seen[page.ID]; exists {
			return fmt.Errorf("duplicate COW page %d", page.ID)
		}
		seen[page.ID] = struct{}{}
		if page.ID == rootPage {
			rootIncluded = true
		}
		encoded, err := encodePage(page)
		if err != nil {
			return fmt.Errorf("encode COW page %d: %w", page.ID, err)
		}
		images = append(images, PageImage{Offset: page.ID * PageSize, Data: encoded})
	}
	if !rootIncluded && rootPage != NoPage {
		// Root collapse may publish an existing child without rewriting it.
		// Validate that the referenced page is already durable and readable.
		b := make([]byte, PageSize)
		if err := readFullAt(s.dev, b, int64(rootPage*PageSize)); err != nil {
			return fmt.Errorf("read existing COW root page %d: %w", rootPage, err)
		}
		if _, err := decodePage(rootPage, b); err != nil {
			return fmt.Errorf("validate existing COW root page %d: %w", rootPage, err)
		}
	}

	next := s.sb
	next.Generation++
	next.RootPage = rootPage
	next.MappingSequence = mappingSequence
	slot := uint64(1) - s.sbSlot
	superblockImage, err := encodeSuperblock(next)
	if err != nil {
		return err
	}
	// The root descriptor is deliberately the final after-image in the WAL
	// transaction. Recovery replays page data before making it reachable.
	images = append(images, PageImage{Offset: slot * PageSize, Data: superblockImage})
	if err = committer.CommitImages(images); err != nil {
		return err
	}
	s.sb, s.sbSlot = next, slot
	return nil
}

func (s *Store) inArena(id uint64) bool {
	return id >= s.sb.ArenaStart && id-s.sb.ArenaStart < s.sb.ArenaPages
}

// WritePage writes one immutable arena page. Call Sync after writing all pages
// of a COW update and before Publish makes their root reachable.
func (s *Store) WritePage(p Page) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.inArena(p.ID) {
		return ErrOutOfRange
	}
	b, err := encodePage(p)
	if err != nil {
		return err
	}
	return writeFullAt(s.dev, b, int64(p.ID*PageSize))
}

func (s *Store) ReadPage(id uint64) (Page, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.inArena(id) {
		return Page{}, ErrOutOfRange
	}
	b := make([]byte, PageSize)
	if err := readFullAt(s.dev, b, int64(id*PageSize)); err != nil {
		return Page{}, err
	}
	return decodePage(id, b)
}

func (s *Store) Sync() error { return s.dev.Sync() }

func encodeSuperblock(s Superblock) ([]byte, error) {
	if len(s.VolumeID) == 0 || len(s.VolumeID) > 64 || s.TotalPages < 3 || s.TotalPages > uint64(math.MaxInt64/PageSize) || s.ArenaPages == 0 || s.ArenaStart < 2 || s.ArenaStart > s.TotalPages || s.ArenaPages > s.TotalPages-s.ArenaStart {
		return nil, ErrGeometry
	}
	if version==FormatVersion && (s.LifecycleStart!=0||s.LifecyclePages!=0) {return Superblock{},ErrCorrupt}
	if version==LifecycleFormatVersion && s.LifecyclePages<2 {return Superblock{},ErrGeometry}
	if s.LifecyclePages != 0 {
		if s.LifecyclePages < 2 || s.LifecycleStart < 2 || s.LifecycleStart > s.TotalPages || s.LifecyclePages > s.TotalPages-s.LifecycleStart { return nil, ErrGeometry }
		arenaEnd,lifecycleEnd:=s.ArenaStart+s.ArenaPages,s.LifecycleStart+s.LifecyclePages
		if s.LifecycleStart<arenaEnd&&lifecycleEnd>s.ArenaStart{return nil,ErrGeometry}
	}
	b := make([]byte, PageSize)
	copy(b[0:8], []byte{'B', 'M', 'M', 'E', 'T', 'A', '0', '1'})
	version:=FormatVersion
	if s.LifecyclePages!=0 {version=LifecycleFormatVersion}
	binary.LittleEndian.PutUint16(b[8:10], version)
	binary.LittleEndian.PutUint16(b[10:12], PageSize)
	binary.LittleEndian.PutUint64(b[16:24], s.Generation)
	binary.LittleEndian.PutUint64(b[24:32], s.TotalPages)
	binary.LittleEndian.PutUint64(b[32:40], s.ArenaStart)
	binary.LittleEndian.PutUint64(b[40:48], s.ArenaPages)
	binary.LittleEndian.PutUint64(b[48:56], s.RootPage)
	binary.LittleEndian.PutUint64(b[56:64], s.MappingSequence)
	binary.LittleEndian.PutUint64(b[128:136], s.LifecycleStart)
	binary.LittleEndian.PutUint64(b[136:144], s.LifecyclePages)
	copy(b[64:128], s.VolumeID)
	binary.LittleEndian.PutUint32(b[PageSize-4:], crc32.Checksum(b[:PageSize-4], crcTable))
	return b, nil
}

func decodeSuperblock(b []byte) (Superblock, error) {
	if len(b) != PageSize { return Superblock{}, ErrCorrupt }
	version:=binary.LittleEndian.Uint16(b[8:10])
	if string(b[:8]) != "BMMETA01" || (version != FormatVersion && version != LifecycleFormatVersion) || binary.LittleEndian.Uint16(b[10:12]) != PageSize {
		return Superblock{}, ErrCorrupt
	}
	if crc32.Checksum(b[:PageSize-4], crcTable) != binary.LittleEndian.Uint32(b[PageSize-4:]) {
		return Superblock{}, ErrCorrupt
	}
	id := strings.TrimRight(string(b[64:128]), "\x00")
	s := Superblock{VolumeID: id, Generation: binary.LittleEndian.Uint64(b[16:24]), TotalPages: binary.LittleEndian.Uint64(b[24:32]), ArenaStart: binary.LittleEndian.Uint64(b[32:40]), ArenaPages: binary.LittleEndian.Uint64(b[40:48]), RootPage: binary.LittleEndian.Uint64(b[48:56]), MappingSequence: binary.LittleEndian.Uint64(b[56:64]), LifecycleStart: binary.LittleEndian.Uint64(b[128:136]), LifecyclePages: binary.LittleEndian.Uint64(b[136:144])}
	if len(id) == 0 || s.TotalPages < 3 || s.TotalPages > uint64(math.MaxInt64/PageSize) || s.ArenaPages == 0 || s.ArenaStart < 2 || s.ArenaStart > s.TotalPages || s.ArenaPages > s.TotalPages-s.ArenaStart || (s.RootPage != NoPage && !(s.RootPage >= s.ArenaStart && s.RootPage-s.ArenaStart < s.ArenaPages)) {
		return Superblock{}, ErrGeometry
	}
	if s.LifecyclePages != 0 {
		if s.LifecyclePages < 2 || s.LifecycleStart < 2 || s.LifecycleStart > s.TotalPages || s.LifecyclePages > s.TotalPages-s.LifecycleStart {
			return Superblock{}, ErrGeometry
		}
		arenaEnd, lifecycleEnd := s.ArenaStart+s.ArenaPages, s.LifecycleStart+s.LifecyclePages
		if s.LifecycleStart < arenaEnd && lifecycleEnd > s.ArenaStart { return Superblock{}, ErrGeometry }
	}
	return s, nil
}

func encodePage(p Page) ([]byte, error) {
	if len(p.Payload) > PagePayloadMax {
		return nil, fmt.Errorf("page payload %d exceeds %d", len(p.Payload), PagePayloadMax)
	}
	b := make([]byte, PageSize)
	copy(b[:4], "BMPG")
	binary.LittleEndian.PutUint16(b[4:6], FormatVersion)
	binary.LittleEndian.PutUint16(b[6:8], p.Type)
	binary.LittleEndian.PutUint64(b[8:16], p.ID)
	binary.LittleEndian.PutUint64(b[16:24], p.Generation)
	binary.LittleEndian.PutUint32(b[24:28], uint32(len(p.Payload)))
	copy(b[PageHeaderSize:], p.Payload)
	binary.LittleEndian.PutUint32(b[28:32], 0)
	binary.LittleEndian.PutUint32(b[28:32], crc32.Checksum(b, crcTable))
	return b, nil
}

func decodePage(id uint64, b []byte) (Page, error) {
	if len(b) != PageSize || string(b[:4]) != "BMPG" || binary.LittleEndian.Uint16(b[4:6]) != FormatVersion {
		return Page{}, ErrCorrupt
	}
	want := binary.LittleEndian.Uint32(b[28:32])
	binary.LittleEndian.PutUint32(b[28:32], 0)
	if crc32.Checksum(b, crcTable) != want {
		return Page{}, ErrCorrupt
	}
	if binary.LittleEndian.Uint64(b[8:16]) != id {
		return Page{}, ErrCorrupt
	}
	n := binary.LittleEndian.Uint32(b[24:28])
	if n > PagePayloadMax {
		return Page{}, ErrCorrupt
	}
	return Page{ID: id, Type: binary.LittleEndian.Uint16(b[6:8]), Generation: binary.LittleEndian.Uint64(b[16:24]), Payload: append([]byte(nil), b[PageHeaderSize:PageHeaderSize+int(n)]...)}, nil
}

func writeFullAt(d Device, b []byte, off int64) error {
	n, err := d.WriteAt(b, off)
	if err != nil {
		return err
	}
	if n != len(b) {
		return io.ErrShortWrite
	}
	return nil
}
func readFullAt(d Device, b []byte, off int64) error {
	n, err := d.ReadAt(b, off)
	if err != nil && !(err == io.EOF && n == len(b)) {
		return err
	}
	if n != len(b) {
		return io.ErrUnexpectedEOF
	}
	return nil
}
