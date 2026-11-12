package poolspace

import (
	"errors"
	"testing"

	"blockmesh/internal/device"
)

func TestReadPoolCatalogFromLUN(t *testing.T) {
	f,err:=device.OpenFile(t.TempDir()+"/catalog.img")
	if err!=nil { t.Fatal(err) }
	defer f.Close()
	if _,err=f.WriteAt([]byte{0},(2<<40)-1);err!=nil { t.Fatal(err) }
	if _,err=FormatBootstrapNamed(f,"catalog-pool",64,512);err!=nil { t.Fatal(err) }
	entry,err:=ReadPoolCatalog(f)
	if err!=nil { t.Fatal(err) }
	if entry.Name!="catalog-pool"||entry.UUID==""||entry.DeviceBytes!=uint64(f.Size())||entry.MetadataBytes==0 {
		t.Fatalf("invalid Pool Catalog entry: %+v",entry)
	}
}

func TestReadPoolCatalogRejectsUnnamedLegacyBootstrap(t *testing.T) {
	f,_,err:=newSparsePool(t)
	if err!=nil { t.Fatal(err) }
	defer f.Close()
	if _,err:=ReadPoolCatalog(f);!errors.Is(err,ErrPoolCatalogNameMissing) {
		t.Fatalf("expected unnamed Pool catalog error, got %v",err)
	}
}
