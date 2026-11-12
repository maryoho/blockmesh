package metadata

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"blockmesh/internal/fence"
	"blockmesh/internal/volume"
)

type testFencer struct {
	err    error
	called bool
}

func (f *testFencer) FenceNode(context.Context, string, string) error { f.called = true; return f.err }

type testLease struct{ alive bool }

func (*testLease) AcquireOwner(context.Context, string, string) (uint64, error) { return 1, nil }
func (*testLease) RenewOwner(context.Context, string, string, uint64) error     { return nil }
func (*testLease) ReleaseOwner(context.Context, string, string, uint64) error   { return nil }
func (l *testLease) ReaderAlive(context.Context, string, string, string) (bool, error) {
	return l.alive, nil
}
func openTest(t *testing.T, lease volume.Lease, f fence.Fencer) (*Engine, string, string) {
	t.Helper()
	d := t.TempDir()
	s, w := filepath.Join(d, "state"), filepath.Join(d, "wal")
	e, err := OpenEngine(Config{VolumeID: "v1", NodeID: "n1", StatePath: s, WALPath: w, TotalBlocks: 64, Lease: lease, Fencer: f})
	if err != nil {
		t.Fatal(err)
	}
	if err = e.ActivateRW(context.Background()); err != nil {
		t.Fatal(err)
	}
	return e, s, w
}
func TestMappingAndRecovery(t *testing.T) {
	lease := volume.NewMemoryLease()
	e, s, w := openTest(t, lease, fence.NoopFencer{})
	if err := e.PutMapping(context.Background(), 10, 20, 4); err != nil {
		t.Fatal(err)
	}
	if p, err := e.Lookup(12); err != nil || p != 22 {
		t.Fatalf("lookup=%d err=%v", p, err)
	}
	_ = e.Close()
	e2, err := OpenEngine(Config{VolumeID: "v1", NodeID: "n1", StatePath: s, WALPath: w, TotalBlocks: 64, Lease: lease, Fencer: fence.NoopFencer{}})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if p, err := e2.Lookup(13); err != nil || p != 23 {
		t.Fatalf("recovered lookup=%d err=%v", p, err)
	}
}
func TestReaderPreventsReclaimUntilRefresh(t *testing.T) {
	e, _, _ := openTest(t, volume.NewMemoryLease(), fence.NoopFencer{})
	defer e.Close()
	if err := e.PutMapping(context.Background(), 10, 20, 1); err != nil {
		t.Fatal(err)
	}
	r, err := e.ActivateRO(context.Background(), "boot-1")
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Discard(context.Background(), 10, 1); err != nil {
		t.Fatal(err)
	}
	n, err := e.ReclaimRetired(context.Background())
	if err != nil || n != 0 {
		t.Fatalf("early reclaim=%d err=%v", n, err)
	}
	if _, err = e.RefreshReader(r.NodeID, r.BootID, r.Epoch); err != nil {
		t.Fatal(err)
	}
	n, err = e.ReclaimRetired(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("reclaim=%d err=%v", n, err)
	}
}
func TestFencingRequired(t *testing.T) {
	lease := &testLease{alive: true}
	f := &testFencer{}
	e, _, _ := openTest(t, lease, f)
	defer e.Close()
	_, _ = e.ActivateRO(context.Background(), "boot-x")
	if err := e.FenceAndReclaim(context.Background(), "n1", "boot-x"); err == nil {
		t.Fatal("expected live reader rejection")
	}
	lease.alive = false
	f.err = errors.New("PR failed")
	if err := e.FenceAndReclaim(context.Background(), "n1", "boot-x"); err == nil {
		t.Fatal("expected fencing failure")
	}
	if !f.called {
		t.Fatal("fencer not called")
	}
}
