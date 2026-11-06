package wal

import (
	"testing"

	"blockmesh/internal/format"
	"blockmesh/internal/lock"
)

func TestDynamicSharedCommitterRequiresDistributedLock(t *testing.T){
	dev,sb:=dynamicWALFixture(t)
	if _,err:=NewDynamicSharedCommitter(dev,sb,0,lock.NewLocalLocker(),"wal");err==nil{t.Fatal("local lock must not be accepted")}
}
