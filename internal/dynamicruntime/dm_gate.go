package dynamicruntime

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"blockmesh/internal/dmowner"
	"blockmesh/internal/metastore"
)

type DMDeviceResolver func(volumeID string) string

type dmGateSession struct {
	device string
	mode metastore.VolumeRuntimeMode
	epoch uint64
	rootGeneration uint64
	instance uint64
	stopped bool
}

// DMRequestGate connects DynamicVolumeRuntimeManager to the kernel DM target.
// Fence/Drain are synchronous: the kernel closes admission and waits for every
// tracked in-flight BIO before the command returns.
type DMRequestGate struct {
	owner dmowner.Gate
	reader dmowner.ReaderGate
	deviceFor DMDeviceResolver

	mu sync.Mutex
	locks map[string]*sync.Mutex
	sessions map[string]dmGateSession
}

func NewDMRequestGate(owner dmowner.Gate, reader dmowner.ReaderGate, deviceFor DMDeviceResolver) (*DMRequestGate, error) {
	if owner == nil || reader == nil || deviceFor == nil {
		return nil, errors.New("DM Owner gate, Reader gate and Volume-to-device resolver are required")
	}
	return &DMRequestGate{owner:owner,reader:reader,deviceFor:deviceFor,
		locks:make(map[string]*sync.Mutex),sessions:make(map[string]dmGateSession)},nil
}

func (g *DMRequestGate) volumeLock(volumeID string) *sync.Mutex {
	g.mu.Lock()
	defer g.mu.Unlock()
	l:=g.locks[volumeID]
	if l==nil { l=&sync.Mutex{};g.locks[volumeID]=l }
	return l
}

func (g *DMRequestGate) EnableRequestsGuarded(ctx context.Context, spec metastore.VolumeRuntimeSpec, rt *Runtime, check func() error) error {
	if g==nil || ctx==nil || rt==nil || check==nil { return errors.New("DM gate activation arguments are required") }
	if rt.Spec!=spec { return errors.New("DM gate Runtime identity mismatch") }
	device:=g.deviceFor(spec.VolumeID)
	if device=="" { return fmt.Errorf("no DM device is configured for Volume %s",spec.VolumeID) }
	l:=g.volumeLock(spec.VolumeID);l.Lock();defer l.Unlock()

	var session dmGateSession
	session.device=device
	session.mode=spec.Mode
	switch spec.Mode {
	case metastore.VolumeRuntimeReadWrite:
		if rt.Owner==nil || !rt.Owner.Valid() || rt.Owner.Epoch()==0 {
			return ErrOwnerProof
		}
		session.epoch=rt.Owner.Epoch()
		instance,err:=g.owner.Prepare(ctx,device,session.epoch)
		if err!=nil { return fmt.Errorf("prepare DM RW OwnerEpoch gate: %w",err) }
		session.instance=instance
	case metastore.VolumeRuntimeReadOnly:
		reader,ok:=interface{}(rt).(interface{ RootGeneration() uint64 })
		_ = reader
		if rt.RootGeneration==0 || spec.ReaderEpoch==0 { return ErrReaderHealthRequired }
		session.epoch=spec.ReaderEpoch
		session.rootGeneration=rt.RootGeneration
		instance,err:=g.reader.PrepareReader(ctx,device,session.epoch,session.rootGeneration)
		if err!=nil { return fmt.Errorf("prepare DM RO ReaderEpoch gate: %w",err) }
		session.instance=instance
	default:
		return errors.New("unsupported DM Runtime mode")
	}
	// Publish the prepared, closed session before health check/enable. If a
	// command response is ambiguous, StopRequests can still fence this instance.
	g.mu.Lock();g.sessions[spec.VolumeID]=session;g.mu.Unlock()
	if err:=check();err!=nil {
		fenceErr:=g.fenceSession(ctx,spec.VolumeID,session)
		return errors.Join(fmt.Errorf("final Runtime health check before DM admission: %w",err),fenceErr)
	}
	switch session.mode {
	case metastore.VolumeRuntimeReadWrite:
		if err:=g.owner.Enable(ctx,device,session.epoch,session.instance);err!=nil {
			fenceErr:=g.fenceSession(ctx,spec.VolumeID,session)
			return errors.Join(fmt.Errorf("enable DM RW OwnerEpoch gate: %w",err),fenceErr)
		}
	case metastore.VolumeRuntimeReadOnly:
		if err:=g.reader.EnableReader(ctx,device,session.epoch,session.rootGeneration,session.instance);err!=nil {
			fenceErr:=g.fenceSession(ctx,spec.VolumeID,session)
			return errors.Join(fmt.Errorf("enable DM RO ReaderEpoch gate: %w",err),fenceErr)
		}
	}
	return nil
}

func (g *DMRequestGate) fenceSession(ctx context.Context, volumeID string, s dmGateSession) error {
	switch s.mode {
	case metastore.VolumeRuntimeReadWrite:
		return g.owner.Fence(ctx,s.device,s.epoch,s.instance)
	case metastore.VolumeRuntimeReadOnly:
		return g.reader.FenceReader(ctx,s.device,s.epoch,s.instance)
	default:
		return errors.New("unknown DM gate session mode")
	}
}

func (g *DMRequestGate) StopRequests(ctx context.Context, volumeID string) error {
	if g==nil || ctx==nil { return errors.New("DM gate and context are required") }
	l:=g.volumeLock(volumeID);l.Lock();defer l.Unlock()
	g.mu.Lock();s,ok:=g.sessions[volumeID];g.mu.Unlock()
	if !ok {
		device:=g.deviceFor(volumeID)
		if device=="" { return fmt.Errorf("no DM device is configured for Volume %s",volumeID) }
		// FenceCurrent inspects the kernel target mode and fences either the
		// Reader gate or Owner gate; it is also safe after process restart.
		return g.owner.FenceCurrent(ctx,device)
	}
	if err:=g.fenceSession(ctx,volumeID,s);err!=nil{return err}
	s.stopped=true
	g.mu.Lock();g.sessions[volumeID]=s;g.mu.Unlock()
	return nil
}

func (g *DMRequestGate) DrainRequests(ctx context.Context, volumeID string) error {
	if g==nil || ctx==nil { return errors.New("DM gate and context are required") }
	l:=g.volumeLock(volumeID);l.Lock();defer l.Unlock()
	g.mu.Lock();s,ok:=g.sessions[volumeID];g.mu.Unlock()
	if !ok {
		device:=g.deviceFor(volumeID)
		if device=="" { return fmt.Errorf("no DM device is configured for Volume %s",volumeID) }
		// DM target Fence messages synchronously wait for inflight_bios == 0.
		return g.owner.FenceCurrent(ctx,device)
	}
	if !s.stopped {
		if err:=g.fenceSession(ctx,volumeID,s);err!=nil{return err}
	}
	// The kernel Fence command is the drain barrier; a successful return proves
	// there are no tracked in-flight BIOs for this target instance.
	g.mu.Lock();delete(g.sessions,volumeID);g.mu.Unlock()
	return nil
}

func (g *DMRequestGate) FailStop(ctx context.Context, spec metastore.VolumeRuntimeSpec, cause error) error {
	if g==nil || ctx==nil { return errors.New("DM gate and context are required") }
	if spec.VolumeID=="" { return errors.New("Volume ID is required for DM fail-stop") }
	if err:=g.StopRequests(ctx,spec.VolumeID);err!=nil{return fmt.Errorf("fence DM target after Runtime lease loss (%v): %w",cause,err)}
	if err:=g.DrainRequests(ctx,spec.VolumeID);err!=nil{return fmt.Errorf("drain DM target after Runtime lease loss (%v): %w",cause,err)}
	return nil
}

var _ RequestGate = (*DMRequestGate)(nil)
