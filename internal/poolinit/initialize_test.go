package poolinit

import (
	"context"
	"testing"

	"blockmesh/internal/device"
)

type testLocker struct{}
func(testLocker)Distributed()bool{return true}
func(testLocker)Lock(context.Context,string)(func()error,error){return func()error{return nil},nil}

type testOwner struct{epoch uint64}
func(o testOwner)Epoch()uint64{return o.epoch}
func(o testOwner)Valid()bool{return o.epoch!=0}

func TestInitializeCreatesAndReopensPool(t *testing.T) {
	f,err:=device.OpenFile(t.TempDir()+"/pool.img")
	if err!=nil { t.Fatal(err) }
	defer f.Close()
	const size=int64(2<<40)
	if _,err=f.WriteAt([]byte{0},size-1);err!=nil { t.Fatal(err) }
	cfg:=Config{VolumeSlots:64}
	first,err:=Initialize(context.Background(),f,"test-pool","wwid:test-lun",testLocker{},testOwner{epoch:1},cfg)
	if err!=nil { t.Fatal(err) }
	if first.PoolName!="test-pool" || first.PoolUUID=="" || !first.ReservedAllocator {
		t.Fatalf("incomplete Pool initialization result: %+v",first)
	}
	second,err:=Initialize(context.Background(),f,"test-pool","wwid:test-lun",testLocker{},testOwner{epoch:1},cfg)
	if err!=nil { t.Fatal(err) }
	if second.PoolUUID!=first.PoolUUID || second.ViewGeneration!=first.ViewGeneration {
		t.Fatalf("retry changed Pool identity or view generation: first=%+v second=%+v",first,second)
	}
}

func TestInitializeRejectsWrongCatalogName(t *testing.T) {
	f,err:=device.OpenFile(t.TempDir()+"/pool.img")
	if err!=nil { t.Fatal(err) }
	defer f.Close()
	const size=int64(2<<40)
	if _,err=f.WriteAt([]byte{0},size-1);err!=nil { t.Fatal(err) }
	if _,err=Initialize(context.Background(),f,"first","wwid:test-lun",testLocker{},testOwner{epoch:1},Config{});err!=nil { t.Fatal(err) }
	if _,err=Initialize(context.Background(),f,"second","wwid:test-lun",testLocker{},testOwner{epoch:1},Config{});err==nil {
		t.Fatal("expected Pool catalog name mismatch")
	}
}
