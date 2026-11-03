package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"blockmesh-p3/internal/allocator"
	"blockmesh-p3/internal/fence"
	"blockmesh-p3/internal/volume"
	"blockmesh-p3/internal/wal"
)

type Engine struct {
	mu         sync.RWMutex
	state      diskState
	statePath  string
	log        *wal.Log
	lease      volume.Lease
	fencer     fence.Fencer
	nodeID     string
	ownerEpoch uint64
	nextTx     uint64
}

type Config struct {
	VolumeID, NodeID, StatePath, WALPath string
	TotalBlocks                          uint64
	Lease                                volume.Lease
	Fencer                               fence.Fencer
}

func Open(cfg Config) (*Engine, error) {
	if cfg.VolumeID == "" || cfg.NodeID == "" || cfg.StatePath == "" || cfg.WALPath == "" || cfg.TotalBlocks == 0 {
		return nil, errors.New("incomplete engine config")
	}
	if cfg.Lease == nil || cfg.Fencer == nil {
		return nil, errors.New("Lease and Fencer implementations are required")
	}
	l, e := wal.Open(cfg.WALPath)
	if e != nil {
		return nil, e
	}
	en := &Engine{statePath: cfg.StatePath, log: l, lease: cfg.Lease, fencer: cfg.Fencer, nodeID: cfg.NodeID, nextTx: 1}
	b, e := os.ReadFile(cfg.StatePath)
	if e == nil {
		if e = json.Unmarshal(b, &en.state); e != nil {
			l.Close()
			return nil, e
		}
	} else if !os.IsNotExist(e) {
		l.Close()
		return nil, e
	} else {
		bm := allocator.New(cfg.TotalBlocks)
		en.state = diskState{Descriptor: Descriptor{VolumeID: cfg.VolumeID, MappingSequence: 0, RootGeneration: 0}, TotalBlocks: cfg.TotalBlocks, Bitmap: bm.Bytes()}
		if e = en.persistLocked(); e != nil {
			l.Close()
			return nil, e
		}
	}
	if en.state.Descriptor.VolumeID != cfg.VolumeID || en.state.TotalBlocks != cfg.TotalBlocks {
		l.Close()
		return nil, errors.New("state file belongs to another volume or pool geometry")
	}
	records, e := l.Replay()
	if e != nil {
		l.Close()
		return nil, e
	}
	for _, r := range records {
		if r.Intent == nil {
			continue
		}
		if r.Intent.VolumeID != cfg.VolumeID {
			continue
		}
		var candidate diskState
		if e = json.Unmarshal(r.Intent.Payload, &candidate); e != nil {
			l.Close()
			return nil, fmt.Errorf("decode committed transaction %d: %w", r.Intent.TxID, e)
		}
		if candidate.Descriptor.MappingSequence > en.state.Descriptor.MappingSequence {
			en.state = candidate
			if e = en.persistLocked(); e != nil {
				l.Close()
				return nil, e
			}
		}
		if r.Intent.TxID >= en.nextTx {
			en.nextTx = r.Intent.TxID + 1
		}
	}
	return en, nil
}
func (e *Engine) persistLocked() error {
	b, err := json.Marshal(e.state)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(e.statePath), 0700); err != nil {
		return err
	}
	tmp := e.statePath + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, e.statePath); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(e.statePath))
	if err == nil {
		err = d.Sync()
		d.Close()
	}
	return err
}
func (e *Engine) ActivateRW(ctx context.Context) error {
	epoch, err := e.lease.AcquireOwner(ctx, e.state.Descriptor.VolumeID, e.nodeID)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ownerEpoch = epoch
	e.state.Descriptor.OwnerNode = e.nodeID
	e.state.Descriptor.OwnerEpoch = epoch
	return e.persistLocked()
}
func (e *Engine) ActivateRO(_ context.Context, bootID string) (Reader, error) {
	if bootID == "" {
		return Reader{}, errors.New("boot id required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, r := range e.state.Readers {
		if r.NodeID == e.nodeID && r.BootID == bootID && r.State == "ACTIVE" {
			return r, nil
		}
	}
	var epoch uint64
	for _, r := range e.state.Readers {
		if r.NodeID == e.nodeID && r.Epoch >= epoch {
			epoch = r.Epoch + 1
		}
	}
	r := Reader{NodeID: e.nodeID, BootID: bootID, Epoch: epoch + 1, Generation: e.state.Descriptor.RootGeneration, State: "ACTIVE"}
	e.state.Readers = append(e.state.Readers, r)
	return r, e.persistLocked()
}
func (e *Engine) checkOwner(ctx context.Context) error {
	if e.ownerEpoch == 0 {
		return ErrNotOwner
	}
	if err := e.lease.RenewOwner(ctx, e.state.Descriptor.VolumeID, e.nodeID, e.ownerEpoch); err != nil {
		return err
	}
	if e.state.Descriptor.OwnerNode != e.nodeID || e.state.Descriptor.OwnerEpoch != e.ownerEpoch {
		return ErrNotOwner
	}
	return nil
}
func (e *Engine) commitLocked(ctx context.Context, mutate func(*diskState) error) error {
	if err := e.checkOwner(ctx); err != nil {
		return err
	}
	candidate := e.cloneState()
	if err := mutate(&candidate); err != nil {
		return err
	}
	candidate.Descriptor.MappingSequence++
	candidate.Descriptor.RootGeneration++
	candidate.LastTxID = e.nextTx
	payload, err := json.Marshal(candidate)
	if err != nil {
		return err
	}
	seq := candidate.Descriptor.MappingSequence
	crc, err := e.log.AppendIntent(wal.Intent{TxID: e.nextTx, VolumeID: candidate.Descriptor.VolumeID, Sequence: seq, OwnerEpoch: e.ownerEpoch, Payload: payload})
	if err != nil {
		return err
	}
	if err = e.log.AppendCommit(wal.Commit{TxID: e.nextTx, IntentCRC: crc}); err != nil {
		return err
	}
	// Commit is durable at this point. Apply failure must be recovered from WAL.
	e.state = candidate
	e.nextTx++
	return e.persistLocked()
}
func (e *Engine) cloneState() diskState {
	b, _ := json.Marshal(e.state)
	var s diskState
	_ = json.Unmarshal(b, &s)
	return s
}
func (e *Engine) PutMapping(ctx context.Context, logical, physical, length uint64) error {
	if length == 0 {
		return errors.New("zero-length extent")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.commitLocked(ctx, func(s *diskState) error {
		bm, err := allocator.FromBytes(s.TotalBlocks, s.Bitmap)
		if err != nil {
			return err
		}
		if physical+length > s.TotalBlocks {
			return errors.New("physical extent out of range")
		}
		for i := uint64(0); i < length; i++ {
			if bm.Allocated(physical + i) {
				return errors.New("physical block already allocated")
			}
			if err = bm.Mark(physical+i, true); err != nil {
				return err
			}
		}
		for _, x := range s.Mappings {
			if logical < x.LogicalStart+x.Length && x.LogicalStart < logical+length {
				return errors.New("logical extent overlaps existing mapping")
			}
		}
		s.Mappings = append(s.Mappings, Extent{LogicalStart: logical, PhysicalStart: physical, Length: length})
		s.Bitmap = bm.Bytes()
		s.Mappings = coalesce(s.Mappings)
		return nil
	})
}
func (e *Engine) AllocateMapping(ctx context.Context, logical, length uint64) (uint64, error) {
	if length == 0 {
		return 0, errors.New("zero length")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	var start uint64
	err := e.commitLocked(ctx, func(s *diskState) error {
		bm, er := allocator.FromBytes(s.TotalBlocks, s.Bitmap)
		if er != nil {
			return er
		}
		for _, x := range s.Mappings {
			if logical < x.LogicalStart+x.Length && x.LogicalStart < logical+length {
				return errors.New("logical extent overlaps existing mapping")
			}
		}
		ids := make([]uint64, length)
		for i := range ids {
			id, er := bm.Allocate()
			if er != nil {
				return er
			}
			ids[i] = id
		}
		for i := 1; i < len(ids); i++ {
			if ids[i] != ids[0]+uint64(i) {
				return errors.New("allocator returned non-contiguous blocks; use single-block allocation or extent allocator")
			}
		}
		start = ids[0]
		s.Bitmap = bm.Bytes()
		s.Mappings = append(s.Mappings, Extent{LogicalStart: logical, PhysicalStart: start, Length: length})
		s.Mappings = coalesce(s.Mappings)
		return nil
	})
	return start, err
}
func (e *Engine) Lookup(logical uint64) (uint64, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	i := sort.Search(len(e.state.Mappings), func(i int) bool { return e.state.Mappings[i].LogicalStart+e.state.Mappings[i].Length > logical })
	if i == len(e.state.Mappings) || logical < e.state.Mappings[i].LogicalStart {
		return 0, ErrMappingNotFound
	}
	x := e.state.Mappings[i]
	return x.PhysicalStart + (logical - x.LogicalStart), nil
}
func (e *Engine) Discard(ctx context.Context, logical, length uint64) error {
	if length == 0 {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.commitLocked(ctx, func(s *diskState) error {
		end := logical + length
		var next []Extent
		for _, x := range s.Mappings {
			xs, xe := x.LogicalStart, x.LogicalStart+x.Length
			if xe <= logical || xs >= end {
				next = append(next, x)
				continue
			}
			cutS := max(xs, logical)
			cutE := min(xe, end)
			for p := cutS; p < cutE; p++ {
				s.Retired = append(s.Retired, Retired{PhysicalBlock: x.PhysicalStart + (p - xs), Generation: s.Descriptor.RootGeneration + 1, TxID: s.LastTxID + 1})
			}
			if xs < cutS {
				next = append(next, Extent{LogicalStart: xs, PhysicalStart: x.PhysicalStart, Length: cutS - xs})
			}
			if cutE < xe {
				next = append(next, Extent{LogicalStart: cutE, PhysicalStart: x.PhysicalStart + (cutE - xs), Length: xe - cutE})
			}
		}
		s.Mappings = coalesce(next)
		return nil
	})
}
func coalesce(in []Extent) []Extent {
	sort.Slice(in, func(i, j int) bool { return in[i].LogicalStart < in[j].LogicalStart })
	out := make([]Extent, 0, len(in))
	for _, x := range in {
		n := len(out)
		if n > 0 && out[n-1].LogicalStart+out[n-1].Length == x.LogicalStart && out[n-1].PhysicalStart+out[n-1].Length == x.PhysicalStart {
			out[n-1].Length += x.Length
		} else {
			out = append(out, x)
		}
	}
	return out
}
func (e *Engine) Descriptor() Descriptor {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.state.Descriptor
}
func (e *Engine) RefreshReader(node, boot string, epoch uint64) (uint64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := range e.state.Readers {
		r := &e.state.Readers[i]
		if r.NodeID == node && r.BootID == boot && r.Epoch == epoch && r.State == "ACTIVE" {
			r.Generation = e.state.Descriptor.RootGeneration
			if err := e.persistLocked(); err != nil {
				return 0, err
			}
			return r.Generation, nil
		}
	}
	return 0, ErrReaderNotFound
}
func (e *Engine) MarkReaderLeaving(node, boot string, epoch uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := range e.state.Readers {
		r := &e.state.Readers[i]
		if r.NodeID == node && r.BootID == boot && r.Epoch == epoch {
			r.State = "LEAVING"
			return e.persistLocked()
		}
	}
	return ErrReaderNotFound
}

// FenceAndReclaim is deliberately strict: lease death is checked, then SAN fencing
// must complete, before a reader is marked FENCED or retired blocks are freed.
func (e *Engine) FenceAndReclaim(ctx context.Context, node, boot string) error {
	alive, err := e.lease.ReaderAlive(ctx, e.state.Descriptor.VolumeID, node, boot)
	if err != nil {
		return err
	}
	if alive {
		return errors.New("reader lease is still alive")
	}
	if err = e.fencer.FenceNode(ctx, e.state.Descriptor.VolumeID, node); err != nil {
		return fmt.Errorf("SAN fencing failed; retain retired resources: %w", err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	found := false
	for i := range e.state.Readers {
		r := &e.state.Readers[i]
		if r.NodeID == node && r.BootID == boot && r.State != "FENCED" {
			r.State = "FENCED"
			found = true
		}
	}
	if !found {
		return ErrReaderNotFound
	}
	return e.persistLocked()
}
func (e *Engine) ReclaimRetired(ctx context.Context) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	minGen := e.state.Descriptor.RootGeneration
	for _, r := range e.state.Readers {
		if r.State == "ACTIVE" || r.State == "LEAVING" {
			if r.Generation < minGen {
				minGen = r.Generation
			}
		}
	}
	bm, err := allocator.FromBytes(e.state.TotalBlocks, e.state.Bitmap)
	if err != nil {
		return 0, err
	}
	kept := make([]Retired, 0, len(e.state.Retired))
	n := 0
	for _, r := range e.state.Retired {
		if r.Generation <= minGen {
			_ = bm.Mark(r.PhysicalBlock, false)
			n++
		} else {
			kept = append(kept, r)
		}
	}
	if n == 0 {
		return 0, nil
	}
	if err = e.checkOwner(ctx); err != nil {
		return 0, err
	}
	candidate := e.cloneState()
	candidate.Bitmap = bm.Bytes()
	candidate.Retired = kept
	candidate.Descriptor.MappingSequence++
	candidate.Descriptor.RootGeneration++
	candidate.LastTxID = e.nextTx
	payload, err := json.Marshal(candidate)
	if err != nil {
		return 0, err
	}
	crc, err := e.log.AppendIntent(wal.Intent{TxID: e.nextTx, VolumeID: candidate.Descriptor.VolumeID, Sequence: candidate.Descriptor.MappingSequence, OwnerEpoch: e.ownerEpoch, Payload: payload})
	if err != nil {
		return 0, err
	}
	if err = e.log.AppendCommit(wal.Commit{TxID: e.nextTx, IntentCRC: crc}); err != nil {
		return 0, err
	}
	e.state = candidate
	e.nextTx++
	if err = e.persistLocked(); err != nil {
		return 0, err
	}
	return n, nil
}
func (e *Engine) Close() error { return e.log.Close() }
