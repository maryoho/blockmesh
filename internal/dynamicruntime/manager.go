package dynamicruntime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"blockmesh/internal/metastore"
)

var (
	ErrManagerClosed = errors.New("BMPOOL03 DynamicVolumeRuntimeManager is closed")
	ErrRuntimeConflict = errors.New("BMPOOL03 Volume is active with a different runtime identity or mode")
	ErrRuntimeStopping = errors.New("BMPOOL03 Volume runtime is stopping or requires cleanup")
	ErrReadOnlyUnavailable = errors.New("BMPOOL03 read-only runtime opener is not configured")
	ErrOwnerMonitorRequired = errors.New("BMPOOL03 RW runtime requires a monitored Owner lease")
	ErrReaderHealthRequired = errors.New("BMPOOL03 RO runtime requires a heartbeat-capable Reader session")
)

type OwnerAcquirer func(context.Context, metastore.VolumeRuntimeSpec) (OwnerLease, error)

// ReadOnlySession is the live Reader Table / sanlock session. Heartbeat must
// fail when either the distributed Reader lease or durable Reader slot is no
// longer healthy. CloseDrained may be called only after the DM read path drains.
type ReadOnlySession interface {
	Heartbeat() error
	CloseDrained(context.Context, bool) error
	Slot() uint32
	Epoch() uint64
	RootGeneration() uint64
	RequestSnapshot(context.Context) (rootPage, rootGeneration uint64, err error)
}

// ReadOnlyRuntimeOpener must open BMPOOL03 views from one Bootstrap generation,
// join the requested Reader slot and return the session that pins its generation.
// It must not acquire or publish a RW Owner lease.
type ReadOnlyRuntimeOpener func(context.Context, metastore.VolumeRuntimeSpec) (*Runtime, ReadOnlySession, error)

// RequestGate serializes the final health check with DM queue admission.
// FailStop must synchronously close/suspend the local data path before returning.
type RequestGate interface {
	EnableRequestsGuarded(context.Context, metastore.VolumeRuntimeSpec, *Runtime, func() error) error
	StopRequests(context.Context, string) error
	DrainRequests(context.Context, string) error
	FailStop(context.Context, metastore.VolumeRuntimeSpec, error) error
}

type ManagerConfig struct {
	AcquireOwner OwnerAcquirer
	OpenReadWrite interface {
		OpenReadWrite(context.Context, metastore.VolumeRuntimeSpec, OwnerLease) (*Runtime, error)
	}
	OpenReadOnly ReadOnlyRuntimeOpener
	Gate RequestGate
	ReaderHeartbeatInterval time.Duration
}

type RuntimeState uint8

const (
	RuntimeCreated RuntimeState = iota
	RuntimeOpening
	RuntimeAdmitting
	RuntimeActive
	RuntimeLeaseLost
	RuntimeStopping
	RuntimeStopped
	RuntimeFailed
)

type managedRuntime struct {
	spec metastore.VolumeRuntimeSpec
	runtime *Runtime
	reader ReadOnlySession
	state RuntimeState
	enabled bool
	stopping bool
	watchCancel context.CancelFunc
	watchDone chan struct{}
	failure error
	lossCause error
	failStopOnce sync.Once
	requestMu sync.Mutex
	poisoned error
}

type DynamicVolumeRuntimeManager struct {
	cfg ManagerConfig
	mu sync.Mutex
	runtimes map[string]*managedRuntime
	volumeLocks map[string]*sync.Mutex
	closed bool
	inflight sync.WaitGroup
}

func NewDynamicVolumeRuntimeManager(cfg ManagerConfig) (*DynamicVolumeRuntimeManager, error) {
	if cfg.OpenReadWrite == nil || cfg.Gate == nil {
		return nil, errors.New("BMPOOL03 Runtime opener and DM RequestGate are required")
	}
	if cfg.ReaderHeartbeatInterval <= 0 {
		cfg.ReaderHeartbeatInterval = 250 * time.Millisecond
	}
	return &DynamicVolumeRuntimeManager{
		cfg: cfg,
		runtimes: make(map[string]*managedRuntime),
		volumeLocks: make(map[string]*sync.Mutex),
	}, nil
}

func validateManagerSpec(spec metastore.VolumeRuntimeSpec) error {
	if strings.TrimSpace(spec.VolumeID) == "" || strings.TrimSpace(spec.NodeID) == "" || strings.TrimSpace(spec.BootID) == "" {
		return errors.New("VolumeID, NodeID and BootID are required")
	}
	switch spec.Mode {
	case metastore.VolumeRuntimeReadWrite:
		if spec.ReaderEpoch != 0 {
			return errors.New("RW activation must not carry a ReaderEpoch")
		}
	case metastore.VolumeRuntimeReadOnly:
		if spec.ReaderEpoch == 0 {
			return errors.New("RO activation requires a non-zero ReaderEpoch")
		}
	default:
		return errors.New("invalid BMPOOL03 runtime mode")
	}
	return nil
}

func (m *DynamicVolumeRuntimeManager) volumeLock(volumeID string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := m.volumeLocks[volumeID]
	if l == nil {
		l = &sync.Mutex{}
		m.volumeLocks[volumeID] = l
	}
	return l
}

// Activate opens the selected BMPOOL03 runtime, starts lease-health supervision,
// and admits DM requests only after a final synchronous health check under the
// RequestGate's admission serialization lock. Repeated activation with the same
// identity is idempotent; a different identity/mode is rejected.
func (m *DynamicVolumeRuntimeManager) Activate(ctx context.Context, spec metastore.VolumeRuntimeSpec) error {
	if m == nil || ctx == nil {
		return errors.New("manager and activation context are required")
	}
	if err := validateManagerSpec(spec); err != nil {
		return err
	}
	vl := m.volumeLock(spec.VolumeID)
	vl.Lock()
	defer vl.Unlock()

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrManagerClosed
	}
	m.inflight.Add(1)
	m.mu.Unlock()
	defer m.inflight.Done()
	m.mu.Lock()
	if current := m.runtimes[spec.VolumeID]; current != nil {
		if current.stopping || current.state == RuntimeLeaseLost || current.state == RuntimeFailed {
			m.mu.Unlock()
			return ErrRuntimeStopping
		}
		if current.spec != spec {
			m.mu.Unlock()
			return ErrRuntimeConflict
		}
		if current.state == RuntimeActive {
			m.mu.Unlock()
			return nil
		}
		m.mu.Unlock()
		return ErrRuntimeStopping
	}
	m.mu.Unlock()

	rt := &managedRuntime{spec: spec, state: RuntimeOpening}
	var openErr error
	switch spec.Mode {
	case metastore.VolumeRuntimeReadWrite:
		if m.cfg.AcquireOwner == nil {
			return errors.New("RW activation requires a P3 OwnerCoordinator adapter")
		}
		owner, err := m.cfg.AcquireOwner(ctx, spec)
		if err != nil {
			return fmt.Errorf("acquire fenced P3 Owner lease: %w", err)
		}
		if owner == nil || !owner.Valid() || owner.Epoch() == 0 || owner.VolumeID() != spec.VolumeID {
			if owner==nil{return ErrOwnerProof}
			closeErr:=owner.Close()
			if closeErr!=nil {
				rt.runtime=&Runtime{Spec:spec,Owner:owner}
				return m.retainPartialActivation(ctx,rt,errors.Join(ErrOwnerProof,closeErr))
			}
			return ErrOwnerProof
		}
		monitor, ok := owner.(interface{ Lost() <-chan error })
		if !ok || monitor.Lost() == nil {
			closeErr:=owner.Close()
			if closeErr!=nil {
				rt.runtime=&Runtime{Spec:spec,Owner:owner}
				return m.retainPartialActivation(ctx,rt,errors.Join(ErrOwnerMonitorRequired,closeErr))
			}
			return ErrOwnerMonitorRequired
		}
		rt.runtime, openErr = m.cfg.OpenReadWrite.OpenReadWrite(ctx, spec, owner)
		// Backend.OpenReadWrite takes ownership of owner and closes it on failure.
		if openErr != nil {
			if rt.runtime != nil {
				return m.retainPartialActivation(ctx, rt, fmt.Errorf("open BMPOOL03 RW runtime: %w", openErr))
			}
			return fmt.Errorf("open BMPOOL03 RW runtime: %w", openErr)
		}
		if rt.runtime == nil || rt.runtime.Owner == nil ||
			rt.runtime.Spec != spec || rt.runtime.Owner.Epoch() != owner.Epoch() ||
			rt.runtime.Owner.VolumeID() != spec.VolumeID {
			cause := errors.New("BMPOOL03 backend returned a runtime with mismatched spec or Owner lease")
			if rt.runtime != nil { return m.retainPartialActivation(ctx, rt, cause) }
			closeErr:=owner.Close()
			if closeErr!=nil {
				rt.runtime=&Runtime{Spec:spec,Owner:owner}
				return m.retainPartialActivation(ctx,rt,errors.Join(cause,closeErr))
			}
			return cause
		}
		if monitor, ok := rt.runtime.Owner.(interface{ Lost() <-chan error }); !ok || monitor.Lost() == nil {
			return m.retainPartialActivation(ctx, rt, ErrOwnerMonitorRequired)
		}
	case metastore.VolumeRuntimeReadOnly:
		if m.cfg.OpenReadOnly == nil {
			return ErrReadOnlyUnavailable
		}
		rt.runtime, rt.reader, openErr = m.cfg.OpenReadOnly(ctx, spec)
		if openErr != nil {
			return m.retainPartialActivation(ctx, rt, fmt.Errorf("open BMPOOL03 RO runtime: %w", openErr))
		}
		if rt.runtime == nil || rt.reader == nil || rt.runtime.Owner != nil || rt.runtime.Spec != spec ||
			rt.reader.Slot() != spec.ReaderSlot || rt.reader.Epoch() != spec.ReaderEpoch ||
			rt.runtime.RootGeneration == 0 || rt.runtime.RootGeneration != rt.reader.RootGeneration() {
			return m.retainPartialActivation(ctx, rt, errors.New("BMPOOL03 RO opener returned nil or mismatched Runtime/Reader session"))
		}
	}

	check := func() error { return m.checkHealth(rt) }
	watchCtx, watchCancel := context.WithCancel(context.Background())
	rt.watchCancel = watchCancel
	rt.watchDone = make(chan struct{})
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		watchCancel()
		rt.watchCancel = nil
		rt.watchDone = nil // watcher has not been started
		return m.retainPartialActivation(ctx, rt, ErrManagerClosed)
	}
	m.runtimes[spec.VolumeID] = rt
	m.mu.Unlock()
	go m.watchRuntime(watchCtx, rt)

	m.mu.Lock()
	rt.state = RuntimeAdmitting
	m.mu.Unlock()
	if err := m.cfg.Gate.EnableRequestsGuarded(ctx, spec, rt.runtime, check); err != nil {
		cleanupErr := m.deactivateLocked(ctx, spec.VolumeID, rt)
		return errors.Join(fmt.Errorf("admit BMPOOL03 DM requests: %w", err), cleanupErr)
	}
	if err := check(); err != nil {
		m.failStop(rt, err)
		cleanupErr := m.deactivateLocked(ctx, spec.VolumeID, rt)
		return errors.Join(err, cleanupErr)
	}
	m.mu.Lock()
	if rt.state == RuntimeLeaseLost || rt.failure != nil {
		m.mu.Unlock()
		cause := rt.failure
		if cause == nil { cause = errors.New("runtime lease was lost during admission") }
		cleanupErr := m.deactivateLocked(ctx, spec.VolumeID, rt)
		return errors.Join(cause, cleanupErr)
	}
	rt.enabled = true
	rt.state = RuntimeActive
	m.mu.Unlock()
	return nil
}

// retainPartialActivation keeps partially opened resources registered when
// cleanup cannot complete. It never drops a Reader lease or parent LUN silently.
func (m *DynamicVolumeRuntimeManager) retainPartialActivation(ctx context.Context, rt *managedRuntime, cause error) error {
	if rt == nil || (rt.runtime == nil && rt.reader == nil) {
		return cause
	}
	rt.stopping = true
	rt.state = RuntimeFailed
	m.mu.Lock()
	m.runtimes[rt.spec.VolumeID] = rt
	m.mu.Unlock()
	cleanupErr := m.deactivateLocked(ctx, rt.spec.VolumeID, rt)
	return errors.Join(cause, cleanupErr)
}

func (m *DynamicVolumeRuntimeManager) checkHealth(rt *managedRuntime) error {
	if rt == nil || rt.runtime == nil {
		return errors.New("BMPOOL03 runtime is unavailable")
	}
	switch rt.spec.Mode {
	case metastore.VolumeRuntimeReadWrite:
		if rt.runtime.Owner == nil || !rt.runtime.Owner.Valid() {
			return ErrOwnerProof
		}
	case metastore.VolumeRuntimeReadOnly:
		if rt.reader == nil || rt.reader.Epoch() != rt.spec.ReaderEpoch ||
			rt.runtime.RootGeneration == 0 || rt.runtime.RootGeneration != rt.reader.RootGeneration() {
			return ErrReaderHealthRequired
		}
		if err := rt.reader.Heartbeat(); err != nil {
			return fmt.Errorf("BMPOOL03 Reader lease/table heartbeat: %w", err)
		}
	default:
		return errors.New("invalid BMPOOL03 runtime mode")
	}
	return nil
}

func (m *DynamicVolumeRuntimeManager) watchRuntime(ctx context.Context, rt *managedRuntime) {
	defer close(rt.watchDone)
	if rt.spec.Mode == metastore.VolumeRuntimeReadWrite {
		owner := rt.runtime.Owner.(interface{ Lost() <-chan error })
		select {
		case <-ctx.Done():
			return
		case cause, open := <-owner.Lost():
			if !open || cause == nil { cause = errors.New("P3 Owner lease lost") }
			m.failStop(rt, cause)
			return
		}
	}
	ticker := time.NewTicker(m.cfg.ReaderHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.checkHealth(rt); err != nil {
				m.failStop(rt, err)
				return
			}
		}
	}
}

func (m *DynamicVolumeRuntimeManager) failStop(rt *managedRuntime, cause error) {
	if rt == nil { return }
	rt.failStopOnce.Do(func() {
		err := m.cfg.Gate.FailStop(context.Background(), rt.spec, cause)
		m.mu.Lock()
		rt.state = RuntimeLeaseLost
		rt.enabled = false
		rt.lossCause = cause
		rt.failure = err
		m.mu.Unlock()
	})
}

// Deactivate closes admission and drains all in-flight requests before releasing
// Reader/Owner leases or either logical view. On any unsafe teardown boundary,
// the Runtime remains registered so cleanup can be retried without losing pins.
func (m *DynamicVolumeRuntimeManager) Deactivate(ctx context.Context, volumeID string) error {
	if m == nil { return nil }
	if ctx == nil { return errors.New("deactivation context is required") }
	vl := m.volumeLock(volumeID)
	vl.Lock()
	defer vl.Unlock()
	m.mu.Lock()
	rt := m.runtimes[volumeID]
	m.mu.Unlock()
	if rt == nil { return nil }
	return m.deactivateLocked(ctx, volumeID, rt)
}

func (m *DynamicVolumeRuntimeManager) deactivateLocked(ctx context.Context, volumeID string, rt *managedRuntime) error {
	m.mu.Lock()
	if !rt.stopping {
		rt.stopping = true
		if rt.state != RuntimeLeaseLost { rt.state = RuntimeStopping }
	}
	m.mu.Unlock()
	if err := m.cfg.Gate.StopRequests(ctx, volumeID); err != nil {
		return fmt.Errorf("stop BMPOOL03 DM request admission: %w", err)
	}
	if err := m.cfg.Gate.DrainRequests(ctx, volumeID); err != nil {
		return fmt.Errorf("drain BMPOOL03 DM requests: %w", err)
	}
	if rt.watchCancel != nil { rt.watchCancel() }
	if rt.watchDone != nil { <-rt.watchDone }
	m.mu.Lock()
	failStopErr := rt.failure
	m.mu.Unlock()
	if failStopErr != nil {
		return fmt.Errorf("runtime lease loss could not be safely fail-stopped; retaining all resources: %w", failStopErr)
	}
	if rt.reader != nil {
		if err := rt.reader.CloseDrained(ctx, true); err != nil {
			return fmt.Errorf("close drained BMPOOL03 Reader session: %w", err)
		}
		rt.reader = nil
	}
	if rt.runtime != nil {
		if err := rt.runtime.Close(); err != nil {
			return fmt.Errorf("close BMPOOL03 Runtime resources: %w", err)
		}
		rt.runtime = nil
	}
	m.mu.Lock()
	delete(m.runtimes, volumeID)
	rt.state = RuntimeStopped
	m.mu.Unlock()
	return nil
}

func (m *DynamicVolumeRuntimeManager) Shutdown(ctx context.Context) error {
	if m == nil { return nil }
	if ctx == nil { return errors.New("shutdown context is required") }
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	wait := make(chan struct{})
	go func() { m.inflight.Wait(); close(wait) }()
	select {
	case <-wait:
	case <-ctx.Done(): return ctx.Err()
	}
	m.mu.Lock()
	ids := make([]string, 0, len(m.runtimes))
	for id := range m.runtimes { ids = append(ids, id) }
	m.mu.Unlock()
	sort.Strings(ids)
	var failures []error
	for _, id := range ids {
		if err := m.Deactivate(ctx, id); err != nil {
			failures = append(failures, fmt.Errorf("deactivate Volume %s: %w", id, err))
		}
	}
	return errors.Join(failures...)
}

// RuntimeRequestSnapshot is a short-lived request admission proof. The
// kernel BIO remains in flight until the corresponding completion is posted;
// Deactivate fences and drains the DM target before closing Runtime resources.
type RuntimeRequestSnapshot struct {
	Spec metastore.VolumeRuntimeSpec
	Runtime *Runtime
	RootPage uint64
	RootGeneration uint64
	OwnerEpoch uint64
	ReaderEpoch uint64
}

// ResolveRequest performs the userspace half of BMPOOL03 request admission.
// RW requests are checked against the durable BMDVOL03 OwnerEpoch. RO requests
// must match the durable Reader slot and its exact pinned RootGeneration.
func (m *DynamicVolumeRuntimeManager) ResolveRequest(ctx context.Context,volumeID,nodeID,bootID string,
	ownerEpoch,readerEpoch,rootGeneration uint64) (RuntimeRequestSnapshot,error) {
	var out RuntimeRequestSnapshot
	if m==nil || ctx==nil || ctx.Err()!=nil || strings.TrimSpace(volumeID)=="" || strings.TrimSpace(nodeID)=="" || strings.TrimSpace(bootID)=="" {
		return out,metastore.ErrRuntimeRequestInvalid
	}
	m.mu.Lock()
	rt:=m.runtimes[volumeID]
	if rt==nil || !rt.enabled || rt.stopping || rt.state!=RuntimeActive || rt.failure!=nil || rt.poisoned!=nil || rt.runtime==nil {
		m.mu.Unlock()
		return out,metastore.ErrRuntimeRequestFenced
	}
	spec:=rt.spec
	m.mu.Unlock()
	if spec.VolumeID!=volumeID || spec.NodeID!=nodeID || spec.BootID!=bootID {
		return out,metastore.ErrRuntimeRequestFenced
	}
	rt.requestMu.Lock()
	defer rt.requestMu.Unlock()
	m.mu.Lock()
	active:=m.runtimes[volumeID]==rt && rt.enabled && !rt.stopping &&
		rt.state==RuntimeActive && rt.failure==nil && rt.poisoned==nil
	m.mu.Unlock()
	if !active { return out,metastore.ErrRuntimeRequestFenced }
	out.Spec,out.Runtime=spec,rt.runtime
	switch spec.Mode {
	case metastore.VolumeRuntimeReadWrite:
		if ownerEpoch==0 || readerEpoch!=0 { return RuntimeRequestSnapshot{},metastore.ErrRuntimeRequestInvalid }
		if rt.runtime.Owner==nil || !rt.runtime.Owner.Valid() || rt.runtime.Owner.Epoch()!=ownerEpoch {
			return RuntimeRequestSnapshot{},metastore.ErrRuntimeRequestStaleEpoch
		}
		if rt.runtime.Store==nil || rt.runtime.Tree==nil { return RuntimeRequestSnapshot{},metastore.ErrRuntimeRequestFenced }
		if err:=rt.runtime.Store.Refresh();err!=nil{return RuntimeRequestSnapshot{},err}
		desc,err:=rt.runtime.Store.DynamicPoolDescriptor();if err!=nil{return RuntimeRequestSnapshot{},err}
		if desc.OwnerEpoch!=ownerEpoch{return RuntimeRequestSnapshot{},metastore.ErrRuntimeRequestStaleEpoch}
		sb:=rt.runtime.Store.Superblock()
		if rt.runtime.Tree.RootGeneration()!=sb.MappingSequence {
			if err:=rt.runtime.Tree.Refresh();err!=nil{return RuntimeRequestSnapshot{},err}
			sb=rt.runtime.Store.Superblock()
		}
		if rt.runtime.Tree.RootGeneration()!=sb.MappingSequence{return RuntimeRequestSnapshot{},metastore.ErrRuntimeRequestStaleEpoch}
		if rootGeneration!=0 && rootGeneration!=sb.MappingSequence{return RuntimeRequestSnapshot{},metastore.ErrRuntimeRequestStaleEpoch}
		out.RootPage,out.RootGeneration,out.OwnerEpoch=sb.RootPage,sb.MappingSequence,ownerEpoch
	case metastore.VolumeRuntimeReadOnly:
		if ownerEpoch!=0 || readerEpoch==0 || rootGeneration==0 || readerEpoch!=spec.ReaderEpoch {
			return RuntimeRequestSnapshot{},metastore.ErrRuntimeRequestInvalid
		}
		if rt.reader==nil || rt.reader.Epoch()!=readerEpoch { return RuntimeRequestSnapshot{},metastore.ErrRuntimeRequestStaleEpoch }
		root,generation,err:=rt.reader.RequestSnapshot(ctx)
		if err!=nil {
			if errors.Is(err,metastore.ErrReaderLeaseLost)||errors.Is(err,metastore.ErrReaderIdentity) {
				return RuntimeRequestSnapshot{},errors.Join(metastore.ErrRuntimeRequestFenced,err)
			}
			if errors.Is(err,metastore.ErrReaderGeneration) {
				return RuntimeRequestSnapshot{},errors.Join(metastore.ErrRuntimeRequestStaleEpoch,err)
			}
			return RuntimeRequestSnapshot{},err
		}
		if generation!=rootGeneration || generation!=rt.runtime.RootGeneration ||
			rt.runtime.Tree==nil || rt.runtime.Tree.RootGeneration()!=generation {
			return RuntimeRequestSnapshot{},metastore.ErrRuntimeRequestStaleEpoch
		}
		out.RootPage,out.RootGeneration,out.ReaderEpoch=root,generation,readerEpoch
	default:
		return RuntimeRequestSnapshot{},metastore.ErrRuntimeRequestInvalid
	}
	return out,nil
}

func (m *DynamicVolumeRuntimeManager) RuntimeState(volumeID string) RuntimeState {
	if m == nil { return RuntimeCreated }
	m.mu.Lock()
	defer m.mu.Unlock()
	if rt := m.runtimes[volumeID]; rt != nil { return rt.state }
	return RuntimeCreated
}

func (m *DynamicVolumeRuntimeManager) ActiveVolumes() []string {
	if m == nil { return nil }
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0, len(m.runtimes))
	for id, rt := range m.runtimes {
		if rt.enabled && rt.state == RuntimeActive { ids = append(ids, id) }
	}
	sort.Strings(ids)
	return ids
}
