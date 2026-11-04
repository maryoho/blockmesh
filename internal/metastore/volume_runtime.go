package metastore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

var (
	ErrVolumeRuntimeStopping = errors.New("volume runtime is stopping or awaiting cleanup")
	ErrVolumeRuntimeConflict = errors.New("volume runtime is already active with a different identity or mode")
)

// VolumeRuntimeMode determines which distributed ownership primitive is
// acquired before the fixed-LBA metadata is opened.
type VolumeRuntimeMode uint8

const (
	VolumeRuntimeReadOnly VolumeRuntimeMode = iota + 1
	VolumeRuntimeReadWrite
)

type VolumeRuntimeSpec struct {
	VolumeID string
	Mode     VolumeRuntimeMode
	NodeID   string
	BootID   string
}

// VolumeRuntimeMetadata is the set of fixed-LBA metadata objects that must
// share one Store and one volume identity. The constructors for these objects
// remain backend-owned because WAL/AG geometry and sanlock resource names are
// deployment configuration.
type VolumeRuntimeMetadata struct {
	Store       *Store
	ReaderTable *ReaderTable
	Arena       *PersistentArena
	// PublishOwnerEpoch must commit the newly acquired owner lease epoch to
	// the BMDVOL02 descriptor through the cluster-safe WAL before admission.
	// It is required for RW runtimes and must be backed by Store.AdvanceOwnerEpoch.
	PublishOwnerEpoch func(context.Context, uint64) error
}

// RuntimeMetadataDevice is the complete random-access device contract needed
// by Store and the fixed-LBA WAL, plus an owned close operation.
type RuntimeMetadataDevice interface {
	Device
	io.Closer
	Size() int64
}

// RuntimeReaderSession is normally backed by ReaderSession. Closing it is
// permitted only after the local metadata I/O drain has completed.
type RuntimeReaderSession interface {
	CloseDrained(context.Context, bool) error
}

// RuntimeOwnerLease represents a successfully acquired distributed owner
// lease. Implementations must not return until any previous owner has been
// fenced from every SAN path; the epoch is monotonically increasing per Volume.
type RuntimeOwnerLease interface {
	io.Closer
	Epoch() uint64
}

// VolumeRuntimeBackend contains the platform-specific operations. Production
// implementations must use a block-device backend, distributed sanlock
// leases, a cluster-safe WAL committer, SAN PR fencing and a real DM I/O drain
// barrier. FileDevice/MemoryLease/NoopFencer are not production adapters.
type VolumeRuntimeBackend interface {
	OpenDevice(context.Context, VolumeRuntimeSpec) (RuntimeMetadataDevice, error)
	AcquireOwnerLease(context.Context, VolumeRuntimeSpec) (RuntimeOwnerLease, error)
	RecoverWAL(context.Context, VolumeRuntimeSpec, RuntimeMetadataDevice) error
	OpenMetadata(context.Context, VolumeRuntimeSpec, RuntimeMetadataDevice) (*VolumeRuntimeMetadata, error)
	JoinReader(context.Context, VolumeRuntimeSpec, *VolumeRuntimeMetadata) (RuntimeReaderSession, error)
	BuildReaderRecovery(VolumeRuntimeSpec, *VolumeRuntimeMetadata) (ManagedReaderRecovery, error)
	EnableRequests(context.Context, VolumeRuntimeSpec, *VolumeRuntimeMetadata) error
	StopRequests(context.Context, string) error
	DrainMetadataIO(context.Context, string) error
	CloseMetadata(context.Context, VolumeRuntimeSpec, *VolumeRuntimeMetadata) error
}

type volumeRuntime struct {
	spec     VolumeRuntimeSpec
	device   RuntimeMetadataDevice
	owner    RuntimeOwnerLease
	metadata *VolumeRuntimeMetadata
	reader   RuntimeReaderSession
	enabled  bool
	stopping bool
	recoveryStarted bool
}

// VolumeRuntimeManager serializes per-volume activation/deactivation and owns
// the recovery supervisor. The runtime map also retains partially initialized
// resources after failed cleanup so a later Deactivate can safely retry; only
// runtimes with enabled request admission are reported as active.
type VolumeRuntimeManager struct {
	mu       sync.Mutex
	backend  VolumeRuntimeBackend
	runtimes map[string]*volumeRuntime
	supervisor *ReaderRecoverySupervisor
	ctx      context.Context
	cancel   context.CancelFunc
	closed   bool
}

func NewVolumeRuntimeManager(backend VolumeRuntimeBackend) (*VolumeRuntimeManager, error) {
	if backend == nil {
		return nil, errors.New("volume runtime backend is required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &VolumeRuntimeManager{
		backend: backend, runtimes: make(map[string]*volumeRuntime),
		ctx: ctx, cancel: cancel,
	}
	supervisor, err := NewReaderRecoverySupervisor(func(volumeID string) (ManagedReaderRecovery, error) {
		// Activate holds m.mu while invoking the supervisor factory, so this
		// lookup intentionally does not acquire the mutex again.
		rt := m.runtimes[volumeID]
		if rt == nil || rt.metadata == nil {
			return nil, fmt.Errorf("volume %s metadata runtime is not initialized", volumeID)
		}
		return m.backend.BuildReaderRecovery(rt.spec, rt.metadata)
	})
	if err != nil {
		cancel()
		return nil, err
	}
	m.supervisor = supervisor
	return m, nil
}

func validateRuntimeSpec(spec VolumeRuntimeSpec) error {
	if strings.TrimSpace(spec.VolumeID) == "" || strings.TrimSpace(spec.NodeID) == "" || strings.TrimSpace(spec.BootID) == "" {
		return errors.New("volume, node and boot identities are required")
	}
	if spec.Mode != VolumeRuntimeReadOnly && spec.Mode != VolumeRuntimeReadWrite {
		return errors.New("invalid volume runtime mode")
	}
	return nil
}

// Activate follows the fencing/recovery order:
// device -> (RW owner lease) -> WAL recovery -> fixed-LBA Store/ReaderTable/
// Arena -> (RO Reader Session) -> Recovery Controller -> request admission.
// The supplied context bounds activation only; background recovery is bound
// to the manager lifetime and is stopped by Deactivate/Shutdown.
func (m *VolumeRuntimeManager) Activate(ctx context.Context, spec VolumeRuntimeSpec) error {
	if m == nil {
		return errors.New("nil volume runtime manager")
	}
	if ctx == nil {
		return errors.New("activation context is required")
	}
	if err := validateRuntimeSpec(spec); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrRecoverySupervisorClosed
	}
	if current, exists := m.runtimes[spec.VolumeID]; exists {
		if current.stopping {
			return ErrVolumeRuntimeStopping
		}
		if current.spec != spec {
			return ErrVolumeRuntimeConflict
		}
		return nil
	}

	rt := &volumeRuntime{spec: spec}
	fail := func(cause error) error {
		cleanupErr := m.rollbackActivation(ctx, rt)
		if cleanupErr != nil {
			// Preserve partially acquired resources so Deactivate can retry
			// cleanup. Never orphan an open device/lease after a failed close.
			rt.stopping = true
			m.runtimes[spec.VolumeID] = rt
		}
		return errors.Join(cause, cleanupErr)
	}

	dev, err := m.backend.OpenDevice(ctx, spec)
	if err != nil {
		if dev != nil {
			rt.device = dev
			return fail(fmt.Errorf("open metadata device for %s: %w", spec.VolumeID, err))
		}
		return fmt.Errorf("open metadata device for %s: %w", spec.VolumeID, err)
	}
	if dev == nil {
		return errors.New("runtime backend returned nil metadata device")
	}
	rt.device = dev

	if spec.Mode == VolumeRuntimeReadWrite {
		rt.owner, err = m.backend.AcquireOwnerLease(ctx, spec)
		if err != nil {
			return fail(fmt.Errorf("acquire volume owner lease: %w", err))
		}
		if rt.owner == nil {
			return fail(errors.New("runtime backend returned nil owner lease"))
		}
		if err = m.backend.RecoverWAL(ctx, spec, rt.device); err != nil {
			return fail(fmt.Errorf("recover fixed-LBA WAL: %w", err))
		}
	}

	rt.metadata, err = m.backend.OpenMetadata(ctx, spec, rt.device)
	if err != nil {
		return fail(fmt.Errorf("open fixed-LBA metadata: %w", err))
	}
	if err = validateRuntimeMetadata(spec, rt.metadata); err != nil {
		return fail(err)
	}

	if spec.Mode == VolumeRuntimeReadWrite {
		if rt.owner == nil || rt.owner.Epoch() == 0 {
			return fail(errors.New("RW runtime requires a valid distributed owner lease epoch"))
		}
		if rt.metadata.PublishOwnerEpoch == nil {
			return fail(errors.New("RW runtime requires durable Pool Volume owner-epoch publication"))
		}
		if err = rt.metadata.PublishOwnerEpoch(ctx, rt.owner.Epoch()); err != nil {
			return fail(fmt.Errorf("publish durable Volume owner epoch: %w", err))
		}
	}

	if spec.Mode == VolumeRuntimeReadOnly {
		rt.reader, err = m.backend.JoinReader(ctx, spec, rt.metadata)
		if err != nil {
			return fail(fmt.Errorf("join online Reader Generation: %w", err))
		}
		if rt.reader == nil {
			return fail(errors.New("runtime backend returned nil reader session"))
		}
	}

	// The factory resolves this runtime from the map during Activate.
	m.runtimes[spec.VolumeID] = rt
	if err = m.supervisor.Activate(m.ctx, spec.VolumeID); err != nil {
		delete(m.runtimes, spec.VolumeID)
		return fail(fmt.Errorf("start reader recovery supervisor: %w", err))
	}
	rt.recoveryStarted = true
	if err = m.backend.EnableRequests(ctx, spec, rt.metadata); err != nil {
		// EnableRequests may have partially admitted I/O before returning an
		// error. Reuse the full teardown path (including drain) rather than
		// assuming the volume remained quiescent.
		cleanupErr := m.deactivateLocked(ctx, spec.VolumeID, rt)
		return errors.Join(fmt.Errorf("enable volume requests: %w", err), cleanupErr)
	}
	rt.enabled = true
	return nil
}

func validateRuntimeMetadata(spec VolumeRuntimeSpec, metadata *VolumeRuntimeMetadata) error {
	if metadata == nil || metadata.Store == nil || metadata.ReaderTable == nil || metadata.Arena == nil {
		return errors.New("fixed-LBA runtime requires Store, ReaderTable and PersistentArena")
	}
	sb := metadata.Store.Superblock()
	if sb.VolumeID != spec.VolumeID {
		return fmt.Errorf("metadata volume mismatch: requested %s, got %s", spec.VolumeID, sb.VolumeID)
	}
	if metadata.ReaderTable.store != metadata.Store || metadata.Arena.store != metadata.Store {
		return errors.New("Store, ReaderTable and PersistentArena must share one fixed-LBA Store")
	}
	return nil
}

// rollbackActivation is used before the runtime is made available to callers.
// No requests have been admitted, so a joined reader can be closed as drained.
func (m *VolumeRuntimeManager) rollbackActivation(ctx context.Context, rt *volumeRuntime) error {
	var failures []error
	if rt.recoveryStarted {
		if err := m.supervisor.Deactivate(ctx, rt.spec.VolumeID); err != nil {
			return fmt.Errorf("stop recovery during activation rollback: %w", err)
		}
		rt.recoveryStarted = false
	}
	if rt.reader != nil {
		if err := rt.reader.CloseDrained(ctx, true); err != nil {
			failures = append(failures, fmt.Errorf("close reader session during activation rollback: %w", err))
			return errors.Join(failures...)
		}
		rt.reader = nil
	}
	if rt.metadata != nil {
		if err := m.backend.CloseMetadata(ctx, rt.spec, rt.metadata); err != nil {
			failures = append(failures, fmt.Errorf("close metadata during activation rollback: %w", err))
			return errors.Join(failures...)
		}
		rt.metadata = nil
	}
	if rt.owner != nil {
		if err := rt.owner.Close(); err != nil {
			failures = append(failures, fmt.Errorf("release owner lease during activation rollback: %w", err))
			return errors.Join(failures...)
		}
		rt.owner = nil
	}
	if rt.device != nil {
		if err := rt.device.Close(); err != nil {
			return fmt.Errorf("close metadata device during activation rollback: %w", err)
		}
		rt.device = nil
	}
	return errors.Join(failures...)
}

// Deactivate stops admission, joins the recovery worker, drains metadata I/O,
// closes the reader session, and only then releases metadata/lease/device.
func (m *VolumeRuntimeManager) Deactivate(ctx context.Context, volumeID string) error {
	if m == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("deactivation context is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	rt := m.runtimes[volumeID]
	if rt == nil {
		return nil
	}
	return m.deactivateLocked(ctx, volumeID, rt)
}

// deactivateLocked requires m.mu. It deliberately stops at the first unsafe
// boundary so a retry can continue without releasing resources prematurely.
func (m *VolumeRuntimeManager) deactivateLocked(ctx context.Context, volumeID string, rt *volumeRuntime) error {
	if !rt.stopping {
		if err := m.backend.StopRequests(ctx, volumeID); err != nil {
			return fmt.Errorf("stop new volume requests: %w", err)
		}
		rt.enabled = false
		rt.stopping = true
	}
	if err := m.supervisor.Deactivate(ctx, volumeID); err != nil {
		return fmt.Errorf("stop reader recovery worker: %w", err)
	}
	rt.recoveryStarted = false
	if err := m.backend.DrainMetadataIO(ctx, volumeID); err != nil {
		return fmt.Errorf("drain metadata I/O: %w", err)
	}
	if rt.reader != nil {
		if err := rt.reader.CloseDrained(ctx, true); err != nil {
			return fmt.Errorf("close drained Reader Session: %w", err)
		}
		rt.reader = nil
	}
	if rt.metadata != nil {
		if err := m.backend.CloseMetadata(ctx, rt.spec, rt.metadata); err != nil {
			return fmt.Errorf("close fixed-LBA metadata: %w", err)
		}
		rt.metadata = nil
	}
	if rt.owner != nil {
		if err := rt.owner.Close(); err != nil {
			return fmt.Errorf("release Volume Owner Lease: %w", err)
		}
		rt.owner = nil
	}
	if rt.device != nil {
		if err := rt.device.Close(); err != nil {
			return fmt.Errorf("close metadata device: %w", err)
		}
		rt.device = nil
	}
	delete(m.runtimes, volumeID)
	return nil
}

// Shutdown rejects new activations, deactivates every volume, and joins all
// recovery workers. Failed drains deliberately leave their resources open.
func (m *VolumeRuntimeManager) Shutdown(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("shutdown context is required")
	}
	m.mu.Lock()
	m.closed = true
	m.cancel()
	ids := make([]string, 0, len(m.runtimes))
	for id := range m.runtimes {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	sort.Strings(ids)
	var failures []error
	for _, id := range ids {
		if err := m.Deactivate(ctx, id); err != nil {
			failures = append(failures, fmt.Errorf("deactivate volume %s: %w", id, err))
		}
	}
	if err := m.supervisor.Shutdown(ctx); err != nil {
		failures = append(failures, err)
	}
	return errors.Join(failures...)
}

func (m *VolumeRuntimeManager) ActiveVolumes() []string {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0, len(m.runtimes))
	for id, rt := range m.runtimes {
		if rt.enabled {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}
