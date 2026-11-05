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
		EnableRequestsGuarded: func(_ context.Context,_ VolumeRuntimeSpec,_ *VolumeRuntimeMetadata,guard func()error)error{return guard()},
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

type poolOwnerTestReservation struct{}
func (poolOwnerTestReservation) PrepareOwnerReservation(context.Context,string,string)error{return nil}
func (poolOwnerTestReservation) EstablishOwnerReservation(context.Context,string,string)error{return nil}

type poolOwnerTestRecords struct{}
func (poolOwnerTestRecords) Load(context.Context,string)(volume.OwnerRecord,bool,error){return volume.OwnerRecord{},false,nil}
func (poolOwnerTestRecords) CompareAndSwap(context.Context,string,*volume.OwnerRecord,volume.OwnerRecord)error{return nil}

func TestNewPoolVolumeRuntimeBackendRequiresOwnerCoordinatorToShareSanlockLocker(t *testing.T) {
	cfg:=validPoolRuntimeConfig()
	other:=&lock.SanlockLocker{}
	coordinator,err:=volume.NewOwnerCoordinator(volume.OwnerCoordinatorConfig{
		Locker:other,ResourceFor:func(string)string{return "owner-resource"},
		Records:poolOwnerTestRecords{},Fencer:poolRuntimeTestFencer{},Reservation:poolOwnerTestReservation{},
		ReadEpoch:func(context.Context,string)(uint64,error){return 1,nil},
		DrainOldOwner:func(context.Context,string,volume.OwnerRecord)error{return nil},
	})
	if err!=nil{t.Fatal(err)}
	cfg.Owner=PoolOwnerCoordinator{Coordinator:coordinator}
	if _,err=NewPoolVolumeRuntimeBackend(cfg);err==nil{
		t.Fatal("OwnerCoordinator using a different sanlock locker was accepted")
	}
}


type poolRuntimeProofLease struct {
	record volume.OwnerRecord
	epoch uint64
	verified bool
	lost chan error
	closed bool
}
func (l *poolRuntimeProofLease) Close()error{l.closed=true;return nil}
func (l *poolRuntimeProofLease) Epoch()uint64{if l.epoch!=0{return l.epoch};return l.record.Epoch}
func (l *poolRuntimeProofLease) Lost()<-chan error{return l.lost}
func (l *poolRuntimeProofLease) Valid()bool{return true}
func (l *poolRuntimeProofLease) OwnerRecord()volume.OwnerRecord{return l.record}
func (l *poolRuntimeProofLease) ReservationVerified()bool{return l.verified}

type poolRuntimeProofOwner struct{lease RuntimeOwnerLease}
func (o poolRuntimeProofOwner) AcquireFencedOwner(context.Context,VolumeRuntimeSpec)(RuntimeOwnerLease,error){return o.lease,nil}

func TestPoolRuntimeRequiresMatchingOwnerRecordAndPRProof(t *testing.T) {
	spec:=VolumeRuntimeSpec{VolumeID:"vol-a",Mode:VolumeRuntimeReadWrite,NodeID:"node-a",BootID:"boot-a"}
	for _,tc:=range []struct{name string;record volume.OwnerRecord;verified bool}{
		{name:"matching",record:volume.OwnerRecord{VolumeID:"vol-a",NodeID:"node-a",BootID:"boot-a",Epoch:9},verified:true},
		{name:"wrong node",record:volume.OwnerRecord{VolumeID:"vol-a",NodeID:"node-b",BootID:"boot-a",Epoch:9},verified:true},
		{name:"reservation unverified",record:volume.OwnerRecord{VolumeID:"vol-a",NodeID:"node-a",BootID:"boot-a",Epoch:9},verified:false},
		{name:"wrong epoch",record:volume.OwnerRecord{VolumeID:"vol-a",NodeID:"node-a",BootID:"boot-a",Epoch:8},epoch:9,verified:true},
	} {
		t.Run(tc.name,func(t *testing.T){
			lease:=&poolRuntimeProofLease{record:tc.record,epoch:tc.epoch,verified:tc.verified,lost:make(chan error)}
			cfg:=validPoolRuntimeConfig()
			cfg.Owner=poolRuntimeProofOwner{lease:lease}
			backend,err:=NewPoolVolumeRuntimeBackend(cfg)
			if err!=nil{t.Fatal(err)}
			got,err:=backend.AcquireOwnerLease(context.Background(),spec)
			if tc.name=="matching" {
				if err!=nil||got!=lease{t.Fatalf("matching proof rejected: lease=%v err=%v",got,err)}
			} else {
				if err==nil||got!=nil{t.Fatalf("invalid proof accepted: lease=%v err=%v",got,err)}
				if !lease.closed{t.Fatal("invalid proof lease was not closed")}
			}
		})
	}
}
