package metastore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var ErrRecoverySupervisorClosed = errors.New("reader recovery supervisor is closed")

// ManagedReaderRecovery is the production lifecycle contract. Validation is
// performed synchronously before a volume activation is reported successful.
type ManagedReaderRecovery interface {
	ValidateProduction() error
	Run(context.Context) error
}

type ReaderRecoveryFactory func(volumeID string) (ManagedReaderRecovery, error)

type recoveryRun struct {
	cancel context.CancelFunc
	done chan struct{}
	err error
}

// ReaderRecoverySupervisor owns one recovery loop per activated volume.
// Activate is idempotent for an already-running volume; Deactivate cancels
// and joins that loop before the caller may close its metadata resources.
type ReaderRecoverySupervisor struct {
	mu sync.Mutex
	factory ReaderRecoveryFactory
	runs map[string]*recoveryRun
	closed bool
}

func NewReaderRecoverySupervisor(factory ReaderRecoveryFactory) (*ReaderRecoverySupervisor, error) {
	if factory == nil { return nil, errors.New("reader recovery factory is required") }
	return &ReaderRecoverySupervisor{factory: factory, runs: make(map[string]*recoveryRun)}, nil
}

func (s *ReaderRecoverySupervisor) Activate(ctx context.Context, volumeID string) error {
	if s == nil { return errors.New("nil reader recovery supervisor") }
	if ctx == nil { return errors.New("activation context is required") }
	if strings.TrimSpace(volumeID) == "" { return errors.New("volume ID is required") }

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed { return ErrRecoverySupervisorClosed }
	if current := s.runs[volumeID]; current != nil {
		select {
		case <-current.done:
			delete(s.runs, volumeID)
		default:
			return nil
		}
	}

	controller, err := s.factory(volumeID)
	if err != nil { return fmt.Errorf("build reader recovery controller for %s: %w", volumeID, err) }
	if controller == nil { return errors.New("reader recovery factory returned nil controller") }
	if err = controller.ValidateProduction(); err != nil {
		return fmt.Errorf("unsafe reader recovery configuration for %s: %w", volumeID, err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	run := &recoveryRun{cancel: cancel, done: make(chan struct{})}
	s.runs[volumeID] = run
	go func() {
		err := controller.Run(runCtx)
		if errors.Is(err, context.Canceled) { err = nil }
		run.err = err
		close(run.done)
	}()
	return nil
}

// Deactivate stops and joins the recovery loop. The caller must not close the
// Store, Arena, WAL committer, sanlock locker or device before this returns.
func (s *ReaderRecoverySupervisor) Deactivate(ctx context.Context, volumeID string) error {
	if s == nil { return nil }
	if ctx == nil { return errors.New("deactivation context is required") }
	s.mu.Lock()
	run := s.runs[volumeID]
	if run == nil { s.mu.Unlock(); return nil }
	run.cancel()
	s.mu.Unlock()

	select {
	case <-run.done:
		s.mu.Lock()
		if s.runs[volumeID] == run { delete(s.runs, volumeID) }
		s.mu.Unlock()
		return run.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Shutdown prevents further activation, cancels every loop and waits for all
// controllers to stop. A timeout leaves the supervisor closed; callers may
// invoke Shutdown again with a fresh context to finish joining.
func (s *ReaderRecoverySupervisor) Shutdown(ctx context.Context) error {
	if s == nil { return nil }
	if ctx == nil { return errors.New("shutdown context is required") }
	s.mu.Lock()
	s.closed = true
	runs := make(map[string]*recoveryRun, len(s.runs))
	for volumeID, run := range s.runs {
		runs[volumeID] = run
		run.cancel()
	}
	s.mu.Unlock()

	var failures []error
	for volumeID, run := range runs {
		select {
		case <-run.done:
			if run.err != nil { failures = append(failures, fmt.Errorf("stop recovery for %s: %w", volumeID, run.err)) }
			s.mu.Lock()
			if s.runs[volumeID] == run { delete(s.runs, volumeID) }
			s.mu.Unlock()
		case <-ctx.Done():
			return errors.Join(append(failures, ctx.Err())...)
		}
	}
	return errors.Join(failures...)
}
