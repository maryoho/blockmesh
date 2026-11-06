package poolspace

import (
	"errors"
	"testing"

	"blockmesh/internal/device"
)

func TestDetectFormatKeepsLegacyDistinct(t *testing.T) {
	d,err:=device.OpenFile(t.TempDir()+"/legacy");if err!=nil{t.Fatal(err)};defer d.Close()
	if _,err=d.WriteAt(make([]byte,2*PageSize),0);err!=nil{t.Fatal(err)}
	magic:=[]byte{0x32,0x30,0x4c,0x4f,0x4f,0x50,0x4d,0x42}
	if _,err=d.WriteAt(magic,0);err!=nil{t.Fatal(err)}
	version,err:=DetectFormat(d);if err!=nil{t.Fatal(err)}
	if version!=FormatBMPOOL02{t.Fatalf("version=%d",version)}
	if _,err=OpenPoolSpace(d);!errors.Is(err,ErrLegacyPoolFormat){t.Fatalf("expected legacy read-only error, got %v",err)}
}

func TestDetectUnknownFormat(t *testing.T) {
	d,err:=device.OpenFile(t.TempDir()+"/blank");if err!=nil{t.Fatal(err)};defer d.Close()
	if _,err=d.WriteAt(make([]byte,2*PageSize),0);err!=nil{t.Fatal(err)}
	if _,err=DetectFormat(d);!errors.Is(err,ErrUnknownPoolFormat){t.Fatalf("expected unknown format, got %v",err)}
}
