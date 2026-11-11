package main

import (
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
