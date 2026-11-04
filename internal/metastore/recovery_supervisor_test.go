package metastore

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type supervisorTestController struct {
	validateErr error
	runErr error
	started chan struct{}
	runs atomic.Int32
}

func (c *supervisorTestController) ValidateProduction() error { return c.validateErr }
func (c *supervisorTestController) Run(ctx context.Context) error {
	c.runs.Add(1)
	if c.started != nil { close(c.started) }
	if c.runErr != nil { return c.runErr }
	<-ctx.Done()
	return ctx.Err()
}

func TestReaderRecoverySupervisorActivateDeactivate(t *testing.T) {
	controller := &supervisorTestController{started: make(chan struct{})}
	factoryCalls := atomic.Int32{}
	s, err := NewReaderRecoverySupervisor(func(string) (ManagedReaderRecovery, error) {
		factoryCalls.Add(1)
		return controller, nil
	})
	if err != nil { t.Fatal(err) }

	ctx := context.Background()
	if err = s.Activate(ctx, "vol-a"); err != nil { t.Fatal(err) }
	select { case <-controller.started: case <-time.After(time.Second): t.Fatal("controller did not start") }
	if err = s.Activate(ctx, "vol-a"); err != nil { t.Fatalf("duplicate activation must be idempotent: %v", err) }
	if got := factoryCalls.Load(); got != 1 { t.Fatalf("factory called %d times, want 1", got) }

	stopCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err = s.Deactivate(stopCtx, "vol-a"); err != nil { t.Fatal(err) }
	if err = s.Deactivate(stopCtx, "vol-a"); err != nil { t.Fatalf("duplicate deactivation must be idempotent: %v", err) }
}

func TestReaderRecoverySupervisorRejectsUnsafeController(t *testing.T) {
	wantErr := errors.New("NoopFencer is forbidden")
	controller := &supervisorTestController{validateErr: wantErr}
	s, err := NewReaderRecoverySupervisor(func(string) (ManagedReaderRecovery, error) { return controller, nil })
	if err != nil { t.Fatal(err) }
	if err = s.Activate(context.Background(), "vol-a"); !errors.Is(err, wantErr) {
		t.Fatalf("unsafe controller must be rejected synchronously, got %v", err)
	}
	if got := controller.runs.Load(); got != 0 { t.Fatalf("unsafe controller started %d times", got) }
}

func TestReaderRecoverySupervisorShutdownAllVolumes(t *testing.T) {
	// Use separate start channels to avoid depending on goroutine scheduling.
	c1 := &supervisorTestController{started: make(chan struct{})}
	c2 := &supervisorTestController{started: make(chan struct{})}
	s, err := NewReaderRecoverySupervisor(func(volumeID string) (ManagedReaderRecovery, error) {
		if volumeID == "a" { return c1, nil }
		return c2, nil
	})
	if err != nil { t.Fatal(err) }
	if err = s.Activate(context.Background(), "a"); err != nil { t.Fatal(err) }
	if err = s.Activate(context.Background(), "b"); err != nil { t.Fatal(err) }
	for _, ch := range []<-chan struct{}{c1.started, c2.started} {
		select { case <-ch: case <-time.After(time.Second): t.Fatal("controller did not start") }
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = s.Shutdown(ctx); err != nil { t.Fatal(err) }
	if err = s.Activate(context.Background(), "c"); !errors.Is(err, ErrRecoverySupervisorClosed) {
		t.Fatalf("activation after shutdown must fail, got %v", err)
	}
}

func TestReaderRecoverySupervisorParentCancellation(t *testing.T) {
	controller := &supervisorTestController{started: make(chan struct{})}
	s, err := NewReaderRecoverySupervisor(func(string) (ManagedReaderRecovery, error) { return controller, nil })
	if err != nil { t.Fatal(err) }
	parent, cancelParent := context.WithCancel(context.Background())
	if err = s.Activate(parent, "vol-a"); err != nil { t.Fatal(err) }
	select { case <-controller.started: case <-time.After(time.Second): t.Fatal("controller did not start") }
	cancelParent()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = s.Deactivate(ctx, "vol-a"); err != nil { t.Fatal(err) }
}
