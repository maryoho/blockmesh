package wal

import (
	"testing"

	)

func TestDynamicSharedCommitterRequiresDistributedLock(t *testing.T){
	dev,sb:=dynamicWALFixture(t)
	if _,err:=NewDynamicSharedCommitter(dev,sb,0,nil,"wal");err==nil{t.Fatal("local lock must not be accepted")}
}
