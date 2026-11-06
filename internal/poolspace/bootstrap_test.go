package poolspace

import (
	"errors"
	"testing"

	"blockmesh/internal/device"
)

func TestBootstrapFormatOpenAndExpand(t *testing.T) {
	f,err:=device.OpenFile(t.TempDir()+"/pool.img");if err!=nil{t.Fatal(err)};defer f.Close()
	// The planner requires >=2TiB. Use a sparse regular file with that logical size.
	if _,err=f.WriteAt([]byte{0},(2<<40)-1);err!=nil{t.Fatal(err)}
	s,err:=FormatBootstrap(f,64,512);if err!=nil{t.Fatal(err)}
	if s.Record().Generation!=1{t.Fatalf("generation=%d",s.Record().Generation)}
	reopened,err:=OpenBootstrap(f);if err!=nil{t.Fatal(err)}
	if reopened.Record().PoolUUID!=s.Record().PoolUUID{t.Fatal("Pool UUID changed on reopen")}
	if _,err=reopened.ExpandMetadata(1<<30,1<<30);err!=nil{t.Fatal(err)}
	reopened,err=OpenBootstrap(f);if err!=nil{t.Fatal(err)}
	if reopened.Record().Generation!=2{t.Fatalf("generation=%d",reopened.Record().Generation)}
}

func TestBootstrapRejectsLegacyFormat(t *testing.T) {
	d,err:=device.OpenFile(t.TempDir()+"/legacy.img");if err!=nil{t.Fatal(err)};defer d.Close()
	page:=make([]byte,PageSize);binaryPutUint64(page,0x424D504F4F4C3032)
	if _,err=d.WriteAt(page,0);err!=nil{t.Fatal(err)}
	if _,err=FormatBootstrap(d,64,512);!errors.Is(err,ErrLegacyPoolFormat){t.Fatalf("expected legacy format rejection, got %v",err)}
}

func binaryPutUint64(b []byte,v uint64){for i:=0;i<8;i++{b[i]=byte(v>>(8*i))}}
