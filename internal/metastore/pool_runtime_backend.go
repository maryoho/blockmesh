package metastore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"blockmesh/internal/device"
	"blockmesh/internal/dmowner"
	"blockmesh/internal/fence"
	"blockmesh/internal/format"
	"blockmesh/internal/lock"
	"blockmesh/internal/volume"
	"blockmesh/internal/runtimeevents"
)

// PoolOwnerLeaseAcquirer must acquire the exclusive distributed Owner
// resource and complete old-owner fencing (SCSI PR PREEMPT AND ABORT on every
// configured path, with verification) before returning. The BMDVOL02 v2
// descriptor does not contain the previous NodeID/BootID, so this operation
// cannot safely be inferred from the descriptor by this package.
type PoolOwnerLeaseAcquirer interface {
	AcquireFencedOwner(context.Context, VolumeRuntimeSpec) (RuntimeOwnerLease, error)
}

// PoolOwnerCoordinator adapts the shared-SAN OwnerCoordinator to the Pool
// runtime. The coordinator owns sanlock acquisition, old-owner PR fencing,
// all-path verification, old-owner I/O barrier and authoritative identity CAS.
type PoolOwnerCoordinator struct { Coordinator *volume.OwnerCoordinator }

func (p PoolOwnerCoordinator) AcquireFencedOwner(ctx context.Context, spec VolumeRuntimeSpec) (RuntimeOwnerLease, error) {
	if p.Coordinator == nil { return nil, errors.New("OwnerCoordinator is required") }
	return p.Coordinator.AcquireOwner(ctx, spec.VolumeID, spec.NodeID, spec.BootID)
}

// PoolRuntimeConfig wires the Pool-backed metadata objects to the platform
// services. All callbacks are mandatory: silently substituting local locks,
// no-op fencing, or no-op I/O admission would make shared-SAN operation unsafe.
type PoolRuntimeConfig struct {
	DMGate dmowner.Gate
	DMDeviceFor func(volumeID string) string
	OpenDevice func(context.Context, VolumeRuntimeSpec) (RuntimeMetadataDevice, error)
	Owner PoolOwnerLeaseAcquirer
	Locker *lock.SanlockLocker
	OpenPoolSuperblock func(RuntimeMetadataDevice) (format.Superblock, error)
	CommitterForAG func(device.Device, format.Superblock, uint32) (Committer, error)
	RecoverAG func(context.Context, Committer) error
	ReaderSetResource func(volumeID string) string
	ReaderSlotResource func(volumeID string, slot uint32) string
	PRRegistrar fence.ReaderRegistrar
	ReaderRecoveryProbe ReaderLeaseProbe
	ReaderFencer fence.Fencer
	RecoveryConfig ReaderRecoveryConfig
	ReaderHeartbeatInterval time.Duration
	OnReaderLeaseLost func(context.Context, VolumeRuntimeSpec, error)
	// OnOwnerLeaseLost must synchronously fence/suspend the local data path.
	// Returning is allowed only after new and in-flight data I/O are blocked.
	OnOwnerLeaseLost func(context.Context, VolumeRuntimeSpec, error) error

	// LivePages must return every page reachable from the current B+Tree root
	// except the root itself. It is mandatory because omitting a live page
	// would allow PersistentArena to reuse metadata still reachable by readers.
	LivePages func(*Store) ([]uint64, error)
	BuildRecovery func(VolumeRuntimeSpec, *VolumeRuntimeMetadata) (ManagedReaderRecovery, error)

	EnableRequests func(context.Context, VolumeRuntimeSpec, *VolumeRuntimeMetadata) error
	// EnableRequestsGuarded must call guard while holding the DM admission
	// serialization lock immediately before making the queue live. If guard
	// fails, it must leave admission closed.
	EnableRequestsGuarded func(context.Context, VolumeRuntimeSpec, *VolumeRuntimeMetadata, func() error) error
	StopRequests func(context.Context, string) error
	DrainMetadataIO func(context.Context, string) error
	CloseMetadata func(context.Context, VolumeRuntimeSpec, *VolumeRuntimeMetadata) error
	Events runtimeevents.Sink
}

type kernelOwnerSession struct { epoch uint64; instance uint64 }

type PoolVolumeRuntimeBackend struct {
	cfg PoolRuntimeConfig
	coordinator *SanlockReaderCoordinator
	ownerSessions sync.Map
}

func NewPoolVolumeRuntimeBackend(cfg PoolRuntimeConfig) (*PoolVolumeRuntimeBackend, error) {
	if cfg.OpenDevice == nil || cfg.Owner == nil || cfg.Locker == nil || cfg.DMGate == nil || cfg.DMDeviceFor == nil ||
		!cfg.Locker.Distributed() || cfg.OpenPoolSuperblock == nil || cfg.CommitterForAG == nil || cfg.RecoverAG == nil ||
		cfg.ReaderSetResource == nil || cfg.ReaderSlotResource == nil ||
		cfg.PRRegistrar == nil || cfg.ReaderRecoveryProbe == nil ||
		cfg.ReaderFencer == nil || cfg.LivePages == nil || cfg.OnReaderLeaseLost == nil || cfg.OnOwnerLeaseLost == nil ||
		cfg.EnableRequests == nil || cfg.EnableRequestsGuarded == nil || cfg.StopRequests == nil ||
		cfg.DrainMetadataIO == nil || cfg.CloseMetadata == nil {
		return nil, errors.New("Pool runtime requires device, fenced owner, distributed sanlock/WAL, kernel DM OwnerEpoch gate, reader lease/fencing, live-page, recovery and I/O hooks")
	}
	switch cfg.ReaderFencer.(type) {
	case fence.NoopFencer, *fence.NoopFencer:
		return nil, errors.New("NoopFencer is forbidden for Pool runtime")
	}
	probe, ok := cfg.ReaderRecoveryProbe.(*SanlockReaderLeaseProbe)
	if !ok || probe.Locker == nil || probe.Locker != cfg.Locker || probe.ResourceFor == nil {
		return nil, errors.New("Pool runtime requires a SanlockReaderLeaseProbe using the same distributed locker")
	}
	switch owner := cfg.Owner.(type) {
	case PoolOwnerCoordinator:
		if !owner.Coordinator.UsesSanlockLocker(cfg.Locker) {
			return nil, errors.New("OwnerCoordinator must use the exact Pool runtime sanlock locker")
		}
	case *PoolOwnerCoordinator:
		if owner == nil || !owner.Coordinator.UsesSanlockLocker(cfg.Locker) {
			return nil, errors.New("OwnerCoordinator must use the exact Pool runtime sanlock locker")
		}
	}
	if cfg.BuildRecovery == nil && cfg.RecoveryConfig.HeartbeatTimeout <= 0 {
		return nil, errors.New("automatic Reader recovery requires a positive heartbeat timeout")
	}
	coordinator := &SanlockReaderCoordinator{
		Locker: cfg.Locker, ResourceForVolume: cfg.ReaderSetResource,
	}
	return &PoolVolumeRuntimeBackend{cfg: cfg, coordinator: coordinator}, nil
}


func (b *PoolVolumeRuntimeBackend) HandleOwnerLeaseLost(ctx context.Context, spec VolumeRuntimeSpec, cause error) error {
	if cause == nil { cause = errors.New("Owner Lease lost") }
	kernelFenceErr := b.fenceKernel(ctx, spec.VolumeID)
	// This hook is the synchronous hardware/data-path fail-stop. The ordinary
	// StopRequests and DrainMetadataIO hooks then establish software admission
	// closure and an explicit completion barrier before cleanup can release
	// metadata resources.
	failStopErr := b.cfg.OnOwnerLeaseLost(ctx, spec, cause)
	stopErr := b.cfg.StopRequests(ctx, spec.VolumeID)
	drainErr := b.cfg.DrainMetadataIO(ctx, spec.VolumeID)
	return errors.Join(kernelFenceErr, failStopErr, stopErr, drainErr)
}

func (b *PoolVolumeRuntimeBackend) OpenDevice(ctx context.Context, spec VolumeRuntimeSpec) (RuntimeMetadataDevice, error) {
	return b.cfg.OpenDevice(ctx, spec)
}

func (b *PoolVolumeRuntimeBackend) AcquireOwnerLease(ctx context.Context, spec VolumeRuntimeSpec) (RuntimeOwnerLease, error) {
	if spec.Mode != VolumeRuntimeReadWrite { return nil, errors.New("owner lease requested for non-RW runtime") }
	lease, err := b.cfg.Owner.AcquireFencedOwner(ctx, spec)
	if err != nil { return nil, err }
	if lease == nil || lease.Epoch() == 0 {
		if lease != nil { _ = lease.Close() }
		return nil, errors.New("fenced owner provider returned an invalid lease epoch")
	}
	proof, ok := lease.(interface {
		OwnerRecord() volume.OwnerRecord
		ReservationVerified() bool
		PreviousOwnerFenced() bool
		OldOwnerIODrained() bool
	})
	if !ok {
		_ = lease.Close()
		return nil, errors.New("Pool Owner provider must return a lease carrying durable OwnerRecord, old-owner fencing/drain and all-path PR reservation proof")
	}
	record := proof.OwnerRecord()
	if !proof.ReservationVerified() || !proof.PreviousOwnerFenced() || !proof.OldOwnerIODrained() || record.VolumeID != spec.VolumeID ||
		record.NodeID != spec.NodeID || record.BootID != spec.BootID || record.Epoch != lease.Epoch() {
		_ = lease.Close()
		return nil, errors.New("Pool Owner lease proof does not match requested Volume identity, epoch, old-owner fencing/drain, or verified SCSI PR reservation")
	}
	if health, ok := lease.(interface{ Valid() bool }); !ok || !health.Valid() {
		_ = lease.Close()
		return nil, errors.New("Pool Owner provider must expose a synchronously valid sanlock lease")
	}
	if _, ok := lease.(RuntimeOwnerLeaseMonitor); !ok {
		_ = lease.Close()
		return nil, errors.New("Pool Owner provider must return a monitored sanlock Owner Lease")
	}
	device, deviceErr := b.kernelDevice(spec.VolumeID)
	if deviceErr != nil { _ = lease.Close(); return nil, deviceErr }
	if err := b.cfg.DMGate.FenceCurrent(ctx, device); err != nil {
		_ = lease.Close()
		_ = b.recordEvent(spec,lease.Epoch(),"dm.owner_gate.fenced","fence_stale_local_gate","failure","dm-gate-error",err.Error())
		return nil, fmt.Errorf("fence current local DM target after SAN takeover: %w", err)
	}
	if err:=b.recordEvent(spec,lease.Epoch(),"dm.owner_gate.fenced","fence_stale_local_gate","success","dm-gate-returned","local stale DM Owner gate fenced after SAN takeover");err!=nil{_ = lease.Close();return nil,err}
	b.ownerSessions.Delete(spec.VolumeID)
	return lease, nil
}

func (b *PoolVolumeRuntimeBackend) kernelDevice(volumeID string) (string, error) {
	device := b.cfg.DMDeviceFor(volumeID)
	if strings.TrimSpace(device) == "" { return "", fmt.Errorf("DM device resolver returned an empty device for Volume %s", volumeID) }
	return device, nil
}
func (b *PoolVolumeRuntimeBackend) fenceKernel(ctx context.Context, volumeID string) error {
	value, ok := b.ownerSessions.Load(volumeID)
	device, err := b.kernelDevice(volumeID); if err != nil { return err }
	if !ok { return b.cfg.DMGate.FenceCurrent(ctx, device) }
	session := value.(kernelOwnerSession)
	return b.cfg.DMGate.Fence(ctx, device, session.epoch, session.instance)
}

func (b *PoolVolumeRuntimeBackend) openPoolVolume(dev RuntimeMetadataDevice, spec VolumeRuntimeSpec) (*Store, format.Superblock, format.VolumeDescriptor, error) {
	pool, err := b.cfg.OpenPoolSuperblock(dev)
	if err != nil { return nil, format.Superblock{}, format.VolumeDescriptor{}, fmt.Errorf("open BMPOOL02: %w", err) }
	store, err := OpenPoolVolume(dev, pool, spec.VolumeIndex, spec.VolumeID)
	if err != nil { return nil, format.Superblock{}, format.VolumeDescriptor{}, err }
	desc, err := store.PoolDescriptor()
	if err != nil { return nil, format.Superblock{}, format.VolumeDescriptor{}, err }
	return store, pool, desc, nil
}

// RecoverWAL locates the Volume's AG from its redundant descriptor pair and
// replays that AG journal while holding the distributed WAL resource.
func (b *PoolVolumeRuntimeBackend) RecoverWAL(ctx context.Context, spec VolumeRuntimeSpec, dev RuntimeMetadataDevice) error {
	_, sb, desc, err := b.openPoolVolume(dev, spec)
	if err != nil { return err }
	committer, err := b.cfg.CommitterForAG(dev, sb, desc.AG)
	if err != nil { return err }
	if committer == nil { return errors.New("AG committer factory returned nil") }
	if safe, ok := committer.(ClusterSafeCommitter); !ok || !safe.ClusterSafe() {
		return ErrClusterSafeCommitterRequired
	}
	if err=b.cfg.RecoverAG(ctx, committer);err!=nil{
		_ = b.recordEvent(spec,0,"wal.recovery.completed","recover_wal","failure","ag-recovery-error",err.Error())
		return err
	}
	return b.recordEvent(spec,0,"wal.recovery.completed","recover_wal","success","recover-ag-callback-returned","configured AG recovery callback returned success")
}

func (b *PoolVolumeRuntimeBackend) OpenMetadata(_ context.Context, spec VolumeRuntimeSpec, dev RuntimeMetadataDevice) (*VolumeRuntimeMetadata, error) {
	store, sb, desc, err := b.openPoolVolume(dev, spec)
	if err != nil { return nil, err }
	committer, err := b.cfg.CommitterForAG(dev, sb, desc.AG)
	if err != nil { return nil, err }
	if committer == nil { return nil, errors.New("AG committer factory returned nil") }
	if safe, ok := committer.(ClusterSafeCommitter); !ok || !safe.ClusterSafe() {
		return nil, ErrClusterSafeCommitterRequired
	}
	readerStart, readerPages, err := store.ReaderTableRange()
	if err != nil { return nil, err }
	readers, err := NewReaderTable(store, readerStart, readerPages, committer, b.coordinator)
	if err != nil { return nil, err }
	live, err := b.cfg.LivePages(store)
	if err != nil { return nil, fmt.Errorf("enumerate live B+Tree pages: %w", err) }
	arena, err := NewPersistentArena(store, live, committer, b.coordinator)
	if err != nil { return nil, fmt.Errorf("open persistent Arena: %w", err) }
	return &VolumeRuntimeMetadata{
		Store: store, ReaderTable: readers, Arena: arena,
		PublishOwnerEpoch: func(_ context.Context, epoch uint64) error {
			if err:=store.AdvanceOwnerEpoch(epoch, committer);err!=nil{
				_ = b.recordEvent(spec,epoch,"descriptor.owner_epoch.durable","publish_owner_epoch","failure","descriptor-commit-error",err.Error())
				return err
			}
			return b.recordEvent(spec,epoch,"descriptor.owner_epoch.durable","publish_owner_epoch","success","advance-owner-epoch-returned","Store.AdvanceOwnerEpoch returned success through cluster-safe committer")
		},
	}, nil
}

func (b *PoolVolumeRuntimeBackend) JoinReader(ctx context.Context, spec VolumeRuntimeSpec, meta *VolumeRuntimeMetadata) (RuntimeReaderSession, error) {
	if spec.ReaderEpoch == 0 { return nil, errors.New("RO runtime requires a nonzero ReaderEpoch") }
	probe, ok := b.cfg.ReaderRecoveryProbe.(*SanlockReaderLeaseProbe)
	if !ok || probe.Locker != b.cfg.Locker {
		return nil, errors.New("reader lease probe must use the Pool runtime sanlock locker")
	}
	leaseConfig := volume.ReaderLeaseConfig{
		Locker: b.cfg.Locker,
		ResourceFor: b.cfg.ReaderSlotResource,
	}
	session, err := JoinReaderSession(ctx, meta.ReaderTable, leaseConfig, b.cfg.PRRegistrar,
		spec.ReaderSlot, spec.NodeID, spec.BootID, spec.ReaderEpoch,
		func(cause error) {
			// This callback must suspend/fail-stop the local data path; a
			// best-effort request stop is not sufficient after lease loss.
			b.cfg.OnReaderLeaseLost(context.Background(), spec, cause)
		})
	if err != nil { return nil, err }
	return startPoolReaderHeartbeat(context.Background(), session, spec, b.cfg.ReaderHeartbeatInterval, b.cfg.OnReaderLeaseLost), nil
}

type poolRuntimeDrain struct { drain func(context.Context, string) error }
func (d poolRuntimeDrain) WaitForMetadataIOsDrained(ctx context.Context, volumeID string) error {
	return d.drain(ctx, volumeID)
}

func (b *PoolVolumeRuntimeBackend) BuildReaderRecovery(spec VolumeRuntimeSpec, meta *VolumeRuntimeMetadata) (ManagedReaderRecovery, error) {
	if b.cfg.BuildRecovery != nil { return b.cfg.BuildRecovery(spec, meta) }
	controller := &ReaderRecoveryController{
		Table: meta.ReaderTable, Probe: b.cfg.ReaderRecoveryProbe,
		Fencer: b.cfg.ReaderFencer, Arena: meta.Arena,
		Drain: poolRuntimeDrain{drain: b.cfg.DrainMetadataIO},
		Config: b.cfg.RecoveryConfig,
	}
	if err := controller.ValidateProduction(); err != nil { return nil, err }
	return controller, nil
}

func (b *PoolVolumeRuntimeBackend) EnableRequests(ctx context.Context, spec VolumeRuntimeSpec, meta *VolumeRuntimeMetadata) error {
	if spec.Mode == VolumeRuntimeReadWrite { return errors.New("RW requests require kernel OwnerEpoch guarded admission") }
	return b.cfg.EnableRequests(ctx, spec, meta)
}
func (b *PoolVolumeRuntimeBackend) EnableRequestsGuarded(ctx context.Context, spec VolumeRuntimeSpec, meta *VolumeRuntimeMetadata, guard func() error) error {
	if guard == nil { return errors.New("Owner admission guard is required") }
	if meta == nil || meta.OwnerEpoch == 0 { return errors.New("durably published nonzero OwnerEpoch is required for kernel DM admission") }
	device, deviceErr := b.kernelDevice(spec.VolumeID); if deviceErr != nil { return deviceErr }
	instance, err := b.cfg.DMGate.Prepare(ctx, device, meta.OwnerEpoch)
	if err != nil { return fmt.Errorf("prepare kernel DM OwnerEpoch %d: %w", meta.OwnerEpoch, err) }
	if instance == 0 { return errors.New("kernel DM target returned a zero table instance ID") }
	if err=b.recordEvent(spec,meta.OwnerEpoch,"dm.owner_gate.prepared","prepare_dm_owner_gate","success","dm-gate-prepare-returned",fmt.Sprintf("DM table instance %x prepared",instance));err!=nil{return err}
	session := kernelOwnerSession{epoch: meta.OwnerEpoch, instance: instance}
	b.ownerSessions.Store(spec.VolumeID, session)
	var called atomic.Bool
	err = b.cfg.EnableRequestsGuarded(ctx, spec, meta, func() error {
		called.Store(true)
		if err := guard(); err != nil { return err }
		if err := b.cfg.DMGate.Enable(ctx, device, meta.OwnerEpoch, instance); err != nil {
			_ = b.recordEvent(spec,meta.OwnerEpoch,"dm.owner_gate.enabled","enable_dm_owner_gate","failure","dm-gate-enable-error",err.Error())
			return fmt.Errorf("enable kernel DM OwnerEpoch %d: %w", meta.OwnerEpoch, err)
		}
		return nil
	})
	if !called.Load() {
		// A platform hook that returns without consulting the lease guard has
		// violated the admission contract. Close and drain defensively in case
		// it partially enabled the DM queue.
		stopErr := b.StopRequests(ctx,spec.VolumeID)
		drainErr := b.DrainMetadataIO(ctx,spec.VolumeID)
		return errors.Join(errors.New("DM admission hook returned without invoking Owner health guard"),err,stopErr,drainErr)
	}
	if err != nil { return errors.Join(err, b.StopRequests(ctx, spec.VolumeID), b.DrainMetadataIO(ctx, spec.VolumeID)) }
	if err=b.recordEvent(spec,meta.OwnerEpoch,"dm.owner_gate.enabled","enable_dm_owner_gate","success","guarded-admission-hook-returned",fmt.Sprintf("DM Owner gate enabled for table instance %x",instance));err!=nil{
		return errors.Join(err,b.StopRequests(ctx,spec.VolumeID),b.DrainMetadataIO(ctx,spec.VolumeID))
	}
	return nil
}
func (b *PoolVolumeRuntimeBackend) StopRequests(ctx context.Context, volumeID string) error {
	return errors.Join(b.fenceKernel(ctx, volumeID), b.cfg.StopRequests(ctx, volumeID))
}
func (b *PoolVolumeRuntimeBackend) DrainMetadataIO(ctx context.Context, volumeID string) error {
	var kernelErr error
	if value, ok := b.ownerSessions.Load(volumeID); ok {
		device, err := b.kernelDevice(volumeID)
		if err != nil { kernelErr = err } else { session := value.(kernelOwnerSession); kernelErr = b.cfg.DMGate.Drain(ctx, device, session.epoch, session.instance) }
	} else {
		device, err := b.kernelDevice(volumeID)
		if err != nil { kernelErr = err } else { kernelErr = b.cfg.DMGate.FenceCurrent(ctx, device) }
	}
	return errors.Join(kernelErr, b.cfg.DrainMetadataIO(ctx, volumeID))
}
func (b *PoolVolumeRuntimeBackend) CloseMetadata(ctx context.Context, spec VolumeRuntimeSpec, meta *VolumeRuntimeMetadata) error {
	return b.cfg.CloseMetadata(ctx, spec, meta)
}

func (b *PoolVolumeRuntimeBackend) recordEvent(spec VolumeRuntimeSpec,epoch uint64,eventType,phase,result,proof,detail string) error {
	if b.cfg.Events==nil{return nil}
	return b.cfg.Events.Record(runtimeevents.Event{VolumeID:spec.VolumeID,NodeID:spec.NodeID,BootID:spec.BootID,OwnerEpoch:epoch,EventType:eventType,Phase:phase,Result:result,Proof:proof,Detail:detail})
}

var _ VolumeRuntimeBackend = (*PoolVolumeRuntimeBackend)(nil)


// poolRuntimeReaderSession keeps the durable heartbeat alive for the entire
// admitted RO runtime and joins it before unregistering PR keys or closing the
// Reader Table slot.
type poolRuntimeReaderSession struct {
	session *ReaderSession
	cancel context.CancelFunc
	done chan struct{}
}

func startPoolReaderHeartbeat(parent context.Context, session *ReaderSession, spec VolumeRuntimeSpec,
	interval time.Duration, onLost func(context.Context, VolumeRuntimeSpec, error)) RuntimeReaderSession {
	if interval <= 0 { interval = time.Second }
	ctx, cancel := context.WithCancel(parent)
	w := &poolRuntimeReaderSession{session:session, cancel:cancel, done:make(chan struct{})}
	go func() {
		defer close(w.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done(): return
			case <-ticker.C:
				if err := session.Heartbeat(); err != nil {
					onLost(context.Background(), spec, err)
					return
				}
			}
		}
	}()
	return w
}

func (s *poolRuntimeReaderSession) CloseDrained(ctx context.Context, drained bool) error {
	if s == nil { return nil }
	s.cancel()
	<-s.done
	return s.session.CloseDrained(ctx, drained)
}
