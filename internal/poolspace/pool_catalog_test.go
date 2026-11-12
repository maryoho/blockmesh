package poolspace

import (
	"testing"

	"blockmesh/internal/device"
)

func TestBootstrapPersistsPoolCatalogName(t *testing.T) {
	f,err:=device.OpenFile(t.TempDir()+"/pool-catalog.img")
	if err!=nil { t.Fatal(err) }
	defer f.Close()
	if _,err=f.WriteAt([]byte{0},(2<<40)-1);err!=nil { t.Fatal(err) }
	s,err:=FormatBootstrapNamed(f,"prod-pool_01",64,512)
	if err!=nil { t.Fatal(err) }
	if got:=s.Record().PoolName;got!="prod-pool_01" { t.Fatalf("PoolName=%q",got) }
	reopened,err:=OpenBootstrap(f)
	if err!=nil { t.Fatal(err) }
	if got:=reopened.Record().PoolName;got!="prod-pool_01" { t.Fatalf("reopened PoolName=%q",got) }
}

func TestPoolCatalogNameValidation(t *testing.T) {
	for _,name:=range []string{"", "a", "pool-01", "pool_01", "pool.name"} {
		if !validPoolName(name) { t.Fatalf("expected valid name %q",name) }
	}
	for _,name:=range []string{"pool/name", "pool name", "设备池", "123456789012345678901234"} {
		if validPoolName(name) { t.Fatalf("expected invalid name %q",name) }
	}
}

func TestLegacyBootstrapNameRemainsReadable(t *testing.T) {
	f,s:=newSparsePool(t)
	defer f.Close()
	reopened,err:=OpenBootstrap(f)
	if err!=nil { t.Fatal(err) }
	if reopened.Record().PoolName!="" { t.Fatalf("legacy name=%q",reopened.Record().PoolName) }
	if s.Record().PoolName!="" { t.Fatalf("fixture name=%q",s.Record().PoolName) }
}
