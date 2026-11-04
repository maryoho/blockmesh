package metastore

import (
	"context"
	"testing"
	"time"

	"blockmesh/internal/device"
	"blockmesh/internal/fence"
	"blockmesh/internal/format"
	"blockmesh/internal/lock"
	"blockmesh/internal/volume"
)

type poolRuntimeTestOwner struct{}
func (poolRuntimeTestOwner) AcquireFencedOwner(context.Context, VolumeRuntimeSpec) (RuntimeOwnerLease, error) {
	return nil, nil
}

type poolRuntimeTestFencer struct{}
func (poolRuntimeTestFencer) FenceNode(context.Context, string, string) error { return nil }

type poolRuntimeTestRegistrar struct{}
func (poolRuntimeTestRegistrar) RegisterReader(context.Context, string, string) error { return nil }
func (poolRuntimeTestRegistrar) UnregisterReader(context.Context, string, string) error { return nil }

func validPoolRuntimeConfig() PoolRuntimeConfig {
	locker := &lock.SanlockLocker{}
	return PoolRuntimeConfig{
		OpenDevice: func(context.Context, VolumeRuntimeSpec) (RuntimeMetadataDevice, error) { return nil, nil },
		Owner: poolRuntimeTestOwner{}, Locker: locker,
		OpenPoolSuperblock: func(RuntimeMetadataDevice) (format.Superblock, error) { return format.Superblock{}, nil },
		CommitterForAG: func(device.Device, format.Superblock, uint32) (Committer, error) { return nil, nil },
		RecoverAG: func(context.Context, Committer) error { return nil },
		ReaderSetResource: func(string) string { return "reader-set" },
		ReaderSlotResource: func(string, uint32) string { return "reader-slot" },
		PRRegistrar: poolRuntimeTestRegistrar{},
		ReaderRecoveryProbe: &SanlockReaderLeaseProbe{Locker: locker, ResourceFor: func(string, uint32) string { return "reader-slot" }},
		ReaderFencer: poolRuntimeTestFencer{},
		RecoveryConfig: ReaderRecoveryConfig{HeartbeatTimeout: 5 * time.Second},
		LivePages: func(*Store) ([]uint64, error) { return nil, nil },
		OnReaderLeaseLost: func(context.Context, VolumeRuntimeSpec, error) {},
		OnOwnerLeaseLost: func(context.Context, VolumeRuntimeSpec, error) error { return nil },
		EnableRequests: func(context.Context, VolumeRuntimeSpec, *VolumeRuntimeMetadata) error { return nil },
		StopRequests: func(context.Context, string) error { return nil },
		DrainMetadataIO: func(context.Context, string) error { return nil },
		CloseMetadata: func(context.Context, VolumeRuntimeSpec, *VolumeRuntimeMetadata) error { return nil },
	}
}

func TestNewPoolVolumeRuntimeBackendRequiresDistributedProductionDependencies(t *testing.T) {
	backend, err := NewPoolVolumeRuntimeBackend(validPoolRuntimeConfig())
	if err != nil { t.Fatalf("valid config rejected: %v", err) }
	if backend == nil { t.Fatal("nil backend") }
}

func TestNewPoolVolumeRuntimeBackendRejectsNoopFencer(t *testing.T) {
	cfg := validPoolRuntimeConfig()
	cfg.ReaderFencer = fence.NoopFencer{}
	if _, err := NewPoolVolumeRuntimeBackend(cfg); err == nil {
		t.Fatal("NoopFencer accepted by Pool runtime")
	}
}

func TestNewPoolVolumeRuntimeBackendRequiresHeartbeatRecoveryPolicy(t *testing.T) {
	cfg := validPoolRuntimeConfig()
	cfg.RecoveryConfig.HeartbeatTimeout = 0
	if _, err := NewPoolVolumeRuntimeBackend(cfg); err == nil {
		t.Fatal("missing heartbeat timeout accepted")
	}
}

func TestNewPoolVolumeRuntimeBackendRequiresSharedReaderLocker(t *testing.T) {
	cfg := validPoolRuntimeConfig()
	cfg.ReaderRecoveryProbe = &SanlockReaderLeaseProbe{Locker: &lock.SanlockLocker{}, ResourceFor: func(string, uint32) string { return "reader-slot" }}
	if _, err := NewPoolVolumeRuntimeBackend(cfg); err == nil {
		t.Fatal("reader probe with a different sanlock locker accepted")
	}
}

type poolOwnerTestRecords struct{}
func (poolOwnerTestRecords) Load(context.Context,string)(volume.OwnerRecord,bool,error){return volume.OwnerRecord{},false,nil}
func (poolOwnerTestRecords) CompareAndSwap(context.Context,string,*volume.OwnerRecord,volume.OwnerRecord)error{return nil}

func TestNewPoolVolumeRuntimeBackendRequiresOwnerCoordinatorToShareSanlockLocker(t *testing.T) {
	cfg:=validPoolRuntimeConfig()
	other:=&lock.SanlockLocker{}
	coordinator,err:=volume.NewOwnerCoordinator(volume.OwnerCoordinatorConfig{
		Locker:other,ResourceFor:func(string)string{return "owner-resource"},
		Records:poolOwnerTestRecords{},Fencer:poolRuntimeTestFencer{},
		ReadEpoch:func(context.Context,string)(uint64,error){return 1,nil},
		DrainOldOwner:func(context.Context,string,volume.OwnerRecord)error{return nil},
	})
	if err!=nil{t.Fatal(err)}
	cfg.Owner=PoolOwnerCoordinator{Coordinator:coordinator}
	if _,err=NewPoolVolumeRuntimeBackend(cfg);err==nil{
		t.Fatal("OwnerCoordinator using a different sanlock locker was accepted")
	}
}
