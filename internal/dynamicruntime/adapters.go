package dynamicruntime

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"blockmesh/internal/metastore"
	"blockmesh/internal/lock"
	"blockmesh/internal/volume"
)

// OwnerCoordinatorAdapter connects the P3 distributed Owner state machine to
// the BMPOOL03 Runtime Manager. The coordinator performs sanlock acquisition,
// all-path SCSI PR fencing/reservation verification, old-owner I/O drain and
// durable OwnerRecord publication before this adapter returns.
type OwnerCoordinatorAdapter struct {
	Coordinator *volume.OwnerCoordinator
}

func (a OwnerCoordinatorAdapter) Acquire(ctx context.Context, spec metastore.VolumeRuntimeSpec) (OwnerLease, error) {
	if a.Coordinator == nil {
		return nil, errors.New("P3 OwnerCoordinator is required")
	}
	lease, err := a.Coordinator.AcquireOwner(ctx, spec.VolumeID, spec.NodeID, spec.BootID)
	if err != nil {
		return nil, err
	}
	if lease == nil {
		return nil, errors.New("P3 OwnerCoordinator returned a nil lease")
	}
	return lease, nil
}

// DMAdmissionEpoch is installed into the DM target while it is suspended.
// Every request emitted after Resume must carry this exact epoch tuple. The
// userspace request consumer must additionally call RuntimeRequestResolver
// before applying a mapping or issuing data I/O.
type DMAdmissionEpoch struct {
	Mode           metastore.VolumeRuntimeMode
	OwnerEpoch     uint64
	ReaderSlot     uint32
	ReaderEpoch    uint64
	RootGeneration uint64
}

// DMTargetController is the narrow transport to the actual DM target control
// plane (netlink/ioctl or the kernel queue manager). Suspend must prevent new
// BIO admission; Drain must wait for all already admitted requests; SetEpoch
// must atomically replace the request identity while suspended.
type DMTargetController interface {
	Suspend(context.Context, string) error
	Drain(context.Context, string) error
	SetAdmissionEpoch(context.Context, string, DMAdmissionEpoch) error
	Resume(context.Context, string) error
}

type gateEntry struct {
	mu sync.Mutex
	epoch DMAdmissionEpoch
	active bool
	failed error
}

// DMRequestGate implements the manager's fail-closed admission contract over
// a DMTargetController. Calls for the same Volume are serialized. The target
// remains suspended whenever any transition fails.
type DMRequestGate struct {
	controller DMTargetController
	mu sync.Mutex
	entries map[string]*gateEntry
}

func NewDMRequestGate(controller DMTargetController) (*DMRequestGate, error) {
	if controller == nil {
		return nil, errors.New("DM target controller is required")
	}
	return &DMRequestGate{controller: controller, entries: make(map[string]*gateEntry)}, nil
}

func (g *DMRequestGate) entry(volumeID string) *gateEntry {
	g.mu.Lock()
	defer g.mu.Unlock()
	e := g.entries[volumeID]
	if e == nil {
		e = &gateEntry{}
		g.entries[volumeID] = e
	}
	return e
}

func admissionEpoch(spec metastore.VolumeRuntimeSpec, rt *Runtime) (DMAdmissionEpoch, error) {
	if rt == nil || rt.Spec != spec {
		return DMAdmissionEpoch{}, errors.New("DM admission Runtime does not match VolumeRuntimeSpec")
	}
	switch spec.Mode {
	case metastore.VolumeRuntimeReadWrite:
		if rt.Owner == nil || !rt.Owner.Valid() || rt.Owner.Epoch() == 0 ||
			rt.Owner.VolumeID() != spec.VolumeID || rt.Descriptor.OwnerEpoch != rt.Owner.Epoch() {
			return DMAdmissionEpoch{}, ErrOwnerProof
		}
		return DMAdmissionEpoch{Mode: spec.Mode, OwnerEpoch: rt.Owner.Epoch()}, nil
	case metastore.VolumeRuntimeReadOnly:
		if rt.Owner != nil || spec.ReaderEpoch == 0 || rt.RootGeneration == 0 {
			return DMAdmissionEpoch{}, ErrReaderHealthRequired
		}
		return DMAdmissionEpoch{
			Mode: spec.Mode, ReaderSlot: spec.ReaderSlot,
			ReaderEpoch: spec.ReaderEpoch, RootGeneration: rt.RootGeneration,
		}, nil
	default:
		return DMAdmissionEpoch{}, errors.New("invalid DM admission mode")
	}
}

func (g *DMRequestGate) EnableRequestsGuarded(ctx context.Context, spec metastore.VolumeRuntimeSpec, rt *Runtime, healthCheck func() error) error {
	if g == nil || ctx == nil || healthCheck == nil {
		return errors.New("DM RequestGate, context and health check are required")
	}
	e := g.entry(spec.VolumeID)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.active {
		return errors.New("DM requests are already enabled for this Volume")
	}
	if err := g.controller.Suspend(ctx, spec.VolumeID); err != nil {
		return fmt.Errorf("suspend DM target before epoch installation: %w", err)
	}
	if err := g.controller.Drain(ctx, spec.VolumeID); err != nil {
		e.failed = err
		return fmt.Errorf("drain DM target before epoch installation: %w", err)
	}
	epoch, err := admissionEpoch(spec, rt)
	if err != nil {
		e.failed = err
		return err
	}
	if err = g.controller.SetAdmissionEpoch(ctx, spec.VolumeID, epoch); err != nil {
		e.failed = err
		return fmt.Errorf("install DM admission epoch: %w", err)
	}
	if err = healthCheck(); err != nil {
		e.failed = err
		return fmt.Errorf("final lease/Reader health check before DM resume: %w", err)
	}
	if err = ctx.Err(); err != nil {
		e.failed = err
		return err
	}
	if err = g.controller.Resume(ctx, spec.VolumeID); err != nil {
		e.failed = err
		return fmt.Errorf("resume DM target: %w", err)
	}
	e.epoch, e.active, e.failed = epoch, true, nil
	return nil
}

func (g *DMRequestGate) StopRequests(ctx context.Context, volumeID string) error {
	if g == nil || ctx == nil || volumeID == "" {
		return errors.New("DM RequestGate, context and Volume ID are required")
	}
	e := g.entry(volumeID)
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := g.controller.Suspend(ctx, volumeID); err != nil {
		e.active = false
		e.failed = err
		return fmt.Errorf("suspend DM target: %w", err)
	}
	e.active = false
	return nil
}

func (g *DMRequestGate) DrainRequests(ctx context.Context, volumeID string) error {
	if g == nil || ctx == nil || volumeID == "" {
		return errors.New("DM RequestGate, context and Volume ID are required")
	}
	e := g.entry(volumeID)
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := g.controller.Drain(ctx, volumeID); err != nil {
		e.failed = err
		return fmt.Errorf("drain DM target: %w", err)
	}
	return nil
}

func (g *DMRequestGate) FailStop(ctx context.Context, spec metastore.VolumeRuntimeSpec, cause error) error {
	if g == nil || ctx == nil || spec.VolumeID == "" {
		return errors.New("DM RequestGate, context and Volume ID are required")
	}
	e := g.entry(spec.VolumeID)
	e.mu.Lock()
	defer e.mu.Unlock()
	// Always attempt Drain even if Suspend reports an error. A failed Suspend
	// is returned and keeps the runtime pinned by DynamicVolumeRuntimeManager.
	suspendErr := g.controller.Suspend(ctx, spec.VolumeID)
	e.active = false
	e.failed = errors.Join(cause, suspendErr)
	drainErr := g.controller.Drain(ctx, spec.VolumeID)
	return errors.Join(suspendErr, drainErr)
}

// RuntimeRequestResolver is the userspace validation point for requests
// consumed from the kernel request ring. A successful result pins the Runtime
// snapshot for the caller's in-flight request; completion must be reported
// before the caller releases its request reference and allows DM Drain to end.
type RuntimeRequestResolver struct {
	Manager *DynamicVolumeRuntimeManager
}

type DMRequestIdentity struct {
	VolumeID       string
	NodeID         string
	BootID         string
	OwnerEpoch     uint64
	ReaderEpoch    uint64
	RootGeneration uint64
}

func (r RuntimeRequestResolver) Resolve(ctx context.Context, id DMRequestIdentity) (RuntimeRequestSnapshot, error) {
	if r.Manager == nil {
		return RuntimeRequestSnapshot{}, errors.New("DynamicVolumeRuntimeManager is required")
	}
	return r.Manager.ResolveRequest(ctx, id.VolumeID, id.NodeID, id.BootID,
		id.OwnerEpoch, id.ReaderEpoch, id.RootGeneration)
}

// NewIntegratedManager wires the real BMPOOL03 backend, P3 OwnerCoordinator,
// Reader opener and DM admission gate. OwnerCoordinator and Backend must share
// the exact same distributed SanlockLocker instance; otherwise activation is
// rejected rather than silently using separate lock domains.
func NewIntegratedManager(backend *Backend, owners *volume.OwnerCoordinator, controller DMTargetController) (*DynamicVolumeRuntimeManager, *DMRequestGate, error) {
	if backend == nil || owners == nil {
		return nil, nil, errors.New("BMPOOL03 Backend and P3 OwnerCoordinator are required")
	}
	sanlock, ok := backend.cfg.Locker.(*lock.SanlockLocker)
	if !ok || sanlock == nil || !owners.UsesSanlockLocker(sanlock) {
		return nil, nil, errors.New("BMPOOL03 Backend and P3 OwnerCoordinator must share the same SanlockLocker instance")
	}
	gate, err := NewDMRequestGate(controller)
	if err != nil {
		return nil, nil, err
	}
	manager, err := NewDynamicVolumeRuntimeManager(ManagerConfig{
		AcquireOwner: OwnerCoordinatorAdapter{Coordinator: owners}.Acquire,
		OpenReadWrite: backend,
		OpenReadOnly: backend.ReadOnlyOpener(gate),
		Gate: gate,
	})
	if err != nil {
		return nil, nil, err
	}
	return manager, gate, nil
}
