package main

import (
	"context"
	"blockmesh/internal/device"
	"blockmesh/internal/poolspace"
	"os"
	"path/filepath"
	"testing"
)

type testBootstrapLocker struct{}
func(testBootstrapLocker)Distributed()bool{return true}
func(testBootstrapLocker)Lock(context.Context,string)(func()error,error){return func()error{return nil},nil}
type testBootstrapOwner struct{epoch uint64}
func(o *testBootstrapOwner)Epoch()uint64{return o.epoch}
func(o *testBootstrapOwner)Valid()bool{return o.epoch>0}

func writeOwnerHelper(t *testing.T,body string)string{
	t.Helper()
	path:=filepath.Join(t.TempDir(),"owner-helper.sh")
	if err:=os.WriteFile(path,[]byte("#!/bin/sh\n"+body),0700);err!=nil{t.Fatal(err)}
	return path
}

func TestOwnerLeaseHelperLifetimeAndEpoch(t *testing.T){
	helper:=writeOwnerHelper(t,"echo '{\"epoch\":17}'\nwhile IFS= read -r line; do :; done\n")
	lease,err:=acquireOwnerLease(poolConfig{OwnerLeaseHelper:helper,DevicePath:"/dev/test"},"pool-x")
	if err!=nil{t.Fatal(err)}
	if lease.Epoch()!=17||!lease.Valid(){t.Fatalf("invalid helper lease epoch=%d valid=%v",lease.Epoch(),lease.Valid())}
	if err=lease.Close();err!=nil{t.Fatal(err)}
	if lease.Valid(){t.Fatal("helper lease remained valid after helper exit")}
}

func TestOwnerLeaseHelperRejectsZeroEpoch(t *testing.T){
	helper:=writeOwnerHelper(t,"echo '{\"epoch\":0}'\nwhile IFS= read -r line; do :; done\n")
	if _,err:=acquireOwnerLease(poolConfig{OwnerLeaseHelper:helper,DevicePath:"/dev/test"},"pool-x");err==nil{t.Fatal("expected zero OwnerEpoch rejection")}
}

func TestAllocateInitialDataExtentsPublishesFirstDataView(t *testing.T){
 f,err:=device.OpenFile(filepath.Join(t.TempDir(),"pool.img"));if err!=nil{t.Fatal(err)}
 defer f.Close()
 if _,err=f.WriteAt([]byte{0},(2<<40)-1);err!=nil{t.Fatal(err)}
 store,err:=poolspace.FormatBootstrap(f,16,512);if err!=nil{t.Fatal(err)}
 if err=store.SetCommitLocker(testBootstrapLocker{});err!=nil{t.Fatal(err)}
 owner:=&testBootstrapOwner{epoch:1}
 if err=store.SetOwnerLease(owner);err!=nil{t.Fatal(err)}
 extents,err:=allocateInitialDataExtents(store,8<<20);if err!=nil{t.Fatal(err)}
 if len(extents)==0{t.Fatal("expected allocated Data extent")}
 _,data,generation,err:=store.OpenViews();if err!=nil{t.Fatal(err)}
 if data==nil||data.Size()!=8<<20||generation!=2{t.Fatalf("DataView=%v generation=%d",data,generation)}
 if _,err=allocateInitialDataExtents(store,8<<20);err==nil{t.Fatal("expected duplicate initialization to be rejected")}
 if store.Record().Generation!=2{t.Fatalf("duplicate init changed generation to %d",store.Record().Generation)}
}
func TestAllocateInitialDataExtentsRejectsInvalidCapacity(t *testing.T){
 f,err:=device.OpenFile(filepath.Join(t.TempDir(),"pool.img"));if err!=nil{t.Fatal(err)}
 defer f.Close()
 if _,err=f.WriteAt([]byte{0},(2<<40)-1);err!=nil{t.Fatal(err)}
 store,err:=poolspace.FormatBootstrap(f,16,512);if err!=nil{t.Fatal(err)}
 if _,err=allocateInitialDataExtents(store,(4<<20)+1);err==nil{t.Fatal("expected alignment rejection")}
}
