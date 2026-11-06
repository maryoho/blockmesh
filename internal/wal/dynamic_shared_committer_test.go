package wal

import (
	"fmt"
	"errors"
	"testing"

	"blockmesh/internal/format"
	"blockmesh/internal/lock"
	"blockmesh/internal/metastore"
)

func TestDynamicSharedCommitterRequiresDistributedLock(t *testing.T){
	dev,sb:=dynamicWALFixture(t)
	if _,err:=NewDynamicSharedCommitter(dev,sb,0,nil,"wal");err==nil{t.Fatal("local lock must not be accepted")}
}

func TestDynamicSharedCommitterAutomaticallyCheckpointsAndRetries(t *testing.T){
	dev,sb:=dynamicWALFixture(t)
	locker,err:=lock.NewFileLocker(t.TempDir());if err!=nil{t.Fatal(err)}
	committer,err:=NewDynamicSharedCommitter(dev,sb,0,testSharedLocker{inner:locker},"dynamic-auto-checkpoint")
	if err!=nil{t.Fatal(err)}
	const writes=24
	for i:=0;i<writes;i++{
		image:=make([]byte,format.PageSize)
		copy(image,[]byte(fmt.Sprintf("auto-checkpoint-%02d",i)))
		off:=sb.Geometry.MappingOffset+uint64(i)*format.PageSize
		if err=committer.CommitImages([]metastore.PageImage{{Offset:off,Data:image}});err!=nil{t.Fatalf("commit %d: %v",i,err)}
	}
	j,err:=OpenDynamicJournal(dev,sb,0);if err!=nil{t.Fatal(err)}
	if j.lastTx!=writes{t.Fatalf("last transaction ID=%d, want %d",j.lastTx,writes)}
	for i:=0;i<writes;i++{
		got:=make([]byte,format.PageSize)
		off:=sb.Geometry.MappingOffset+uint64(i)*format.PageSize
		if _,err=dev.ReadAt(got,int64(off));err!=nil{t.Fatal(err)}
		want:=fmt.Sprintf("auto-checkpoint-%02d",i)
		if string(got[:len(want)])!=want{t.Fatalf("page %d=%q want %q",i,got[:len(want)],want)}
	}
}

type testDynamicOwnerLease struct{valid bool;volumeID string;epoch uint64}
func(l *testDynamicOwnerLease)Resource()string{return "test-owner-resource"}
func(l *testDynamicOwnerLease)VolumeID()string{return l.volumeID}
func(l *testDynamicOwnerLease)Epoch()uint64{return l.epoch}
func(l *testDynamicOwnerLease)Valid()bool{return l!=nil&&l.valid}

func TestDynamicSharedCommitterRequiresLiveOwnerAndMatchingDescriptorEpoch(t *testing.T){
	dev,sb:=dynamicWALFixture(t)
	locker,err:=lock.NewFileLocker(t.TempDir());if err!=nil{t.Fatal(err)}
	committer,err:=NewDynamicSharedCommitter(dev,sb,0,testSharedLocker{inner:locker},"owner-guard-wal");if err!=nil{t.Fatal(err)}
	lease:=&testDynamicOwnerLease{valid:true,volumeID:"guard-volume",epoch:9}
	descriptorEpoch:=uint64(9)
	readEpoch:=func()(uint64,error){return descriptorEpoch,nil}
	if err=GuardDynamicSharedCommitter(committer,lease,"guard-volume",readEpoch);err!=nil{t.Fatal(err)}
	image:=make([]byte,format.PageSize);copy(image,"owner-guard-commit")
	if err=committer.CommitImages([]metastore.PageImage{{Offset:sb.Geometry.MappingOffset,Data:image}});err!=nil{t.Fatal(err)}
	descriptorEpoch=10
	if err=committer.CommitImages([]metastore.PageImage{{Offset:sb.Geometry.MappingOffset+format.PageSize,Data:image}});!errors.Is(err,ErrDynamicOwnerEpochMismatch){
		t.Fatalf("expected stale descriptor OwnerEpoch rejection, got %v",err)
	}
	descriptorEpoch=9;lease.valid=false
	if err=committer.CommitImages([]metastore.PageImage{{Offset:sb.Geometry.MappingOffset+2*format.PageSize,Data:image}});!errors.Is(err,ErrDynamicOwnerLeaseLost){
		t.Fatalf("expected lost Owner lease rejection, got %v",err)
	}
}
