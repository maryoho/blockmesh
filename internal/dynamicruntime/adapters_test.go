package dynamicruntime

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"blockmesh/internal/format"
	"blockmesh/internal/metastore"
)

type recordingDMController struct {
	calls []string
	epoch DMAdmissionEpoch
	failAt string
}

func (c *recordingDMController) call(name string) error {
	c.calls = append(c.calls, name)
	if c.failAt == name { return errors.New("injected " + name + " failure") }
	return nil
}
func (c *recordingDMController) Suspend(_ context.Context, _ string) error { return c.call("suspend") }
func (c *recordingDMController) Drain(_ context.Context, _ string) error { return c.call("drain") }
func (c *recordingDMController) SetAdmissionEpoch(_ context.Context, _ string, e DMAdmissionEpoch) error {
	c.epoch = e
	return c.call("set-epoch")
}
func (c *recordingDMController) Resume(_ context.Context, _ string) error { return c.call("resume") }

func TestDMRequestGateRWAdmissionInstallsOwnerEpochBeforeResume(t *testing.T) {
	controller := &recordingDMController{}
	gate, err := NewDMRequestGate(controller)
	if err != nil { t.Fatal(err) }
	spec := metastore.VolumeRuntimeSpec{
		VolumeID: "vol-rw", Mode: metastore.VolumeRuntimeReadWrite,
		NodeID: "node-a", BootID: "boot-a",
	}
	owner := newManagerTestOwner(spec.VolumeID)
	rt := &Runtime{Spec: spec, Owner: owner, Descriptor: format.DynamicVolumeDescriptor{OwnerEpoch: owner.Epoch()}}
	checks := 0
	err = gate.EnableRequestsGuarded(context.Background(), spec, rt, func() error { checks++; return owner.Valid() })
	if err != nil { t.Fatal(err) }
	if checks != 1 { t.Fatalf("health checks=%d, want 1", checks) }
	if !reflect.DeepEqual(controller.calls, []string{"suspend", "drain", "set-epoch", "resume"}) {
		t.Fatalf("controller calls=%v", controller.calls)
	}
	if controller.epoch.Mode != metastore.VolumeRuntimeReadWrite || controller.epoch.OwnerEpoch != owner.Epoch() {
		t.Fatalf("installed epoch=%+v", controller.epoch)
	}
}

func TestDMRequestGateROAdmissionPinsReaderGeneration(t *testing.T) {
	controller := &recordingDMController{}
	gate, err := NewDMRequestGate(controller)
	if err != nil { t.Fatal(err) }
	spec := metastore.VolumeRuntimeSpec{
		VolumeID: "vol-ro", Mode: metastore.VolumeRuntimeReadOnly,
		NodeID: "node-ro", BootID: "boot-ro", ReaderSlot: 3, ReaderEpoch: 11,
	}
	rt := &Runtime{Spec: spec, RootGeneration: 42}
	if err = gate.EnableRequestsGuarded(context.Background(), spec, rt, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	want := DMAdmissionEpoch{Mode: metastore.VolumeRuntimeReadOnly, ReaderSlot: 3, ReaderEpoch: 11, RootGeneration: 42}
	if controller.epoch != want { t.Fatalf("epoch=%+v want=%+v", controller.epoch, want) }
}

func TestDMRequestGateNeverResumesAfterEpochInstallFailure(t *testing.T) {
	controller := &recordingDMController{failAt: "set-epoch"}
	gate, err := NewDMRequestGate(controller)
	if err != nil { t.Fatal(err) }
	spec := metastore.VolumeRuntimeSpec{
		VolumeID: "vol-rw-fail", Mode: metastore.VolumeRuntimeReadWrite,
		NodeID: "node-a", BootID: "boot-a",
	}
	owner := newManagerTestOwner(spec.VolumeID)
	rt := &Runtime{Spec: spec, Owner: owner, Descriptor: format.DynamicVolumeDescriptor{OwnerEpoch: owner.Epoch()}}
	err = gate.EnableRequestsGuarded(context.Background(), spec, rt, func() error { return nil })
	if err == nil { t.Fatal("expected epoch installation failure") }
	if containsString(controller.calls, "resume") { t.Fatalf("target resumed after failure: %v", controller.calls) }
}

func TestDMRequestGateFailStopSuspendsAndDrains(t *testing.T) {
	controller := &recordingDMController{}
	gate, err := NewDMRequestGate(controller)
	if err != nil { t.Fatal(err) }
	spec := metastore.VolumeRuntimeSpec{VolumeID: "vol-fail", Mode: metastore.VolumeRuntimeReadWrite}
	if err = gate.FailStop(context.Background(), spec, errors.New("owner lost")); err != nil { t.Fatal(err) }
	if !reflect.DeepEqual(controller.calls, []string{"suspend", "drain"}) {
		t.Fatalf("controller calls=%v", controller.calls)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values { if value == want { return true } }
	return false
}
