package metadata

import (
	"errors"
	"os"
	"testing"

	"blockmesh/internal/device"
	"blockmesh/internal/format"
)

func TestInitAndReadPoolVolumeDescriptor(t *testing.T) {
	f,err:=os.CreateTemp(t.TempDir(),"pool-volume-*.img");if err!=nil{t.Fatal(err)}
	if err=f.Truncate(int64(format.MetadataSize));err!=nil{t.Fatal(err)}
	defer f.Close()
	d,err:=device.Open(f.Name(),true);if err!=nil{t.Fatal(err)};defer d.Close()
	sb,err:=format.NewSuperblock(0);if err!=nil{t.Fatal(err)}
	if err=writeAtFull(d,sb.Encode(),int64(format.SuperAOffset));err!=nil{t.Fatal(err)}
	if err=d.Sync();err!=nil{t.Fatal(err)}
	got,err:=InitVolume(d,sb,7,"volume-seven",3,8);if err!=nil{t.Fatal(err)}
	if got.VolumeID!="volume-seven"||got.OwnerEpoch!=3||got.Generation!=1||got.ArenaPages==0||got.LifecyclePages!=8 {t.Fatalf("unexpected descriptor: %+v",got)}
	read,err:=ReadVolumeDescriptor(d,sb,7);if err!=nil{t.Fatal(err)}
	if read!=got {t.Fatalf("read descriptor %+v != written %+v",read,got)}
	if _,err=InitVolume(d,sb,7,"volume-seven",3,8);!errors.Is(err,ErrVolumeSlotOccupied){t.Fatalf("expected occupied slot, got %v",err)}
}

func TestReadVolumeDescriptorRejectsWrongPoolExtent(t *testing.T) {
	f,err:=os.CreateTemp(t.TempDir(),"pool-volume-bad-*.img");if err!=nil{t.Fatal(err)}
	if err=f.Truncate(int64(format.MetadataSize));err!=nil{t.Fatal(err)}
	defer f.Close()
	d,err:=device.Open(f.Name(),true);if err!=nil{t.Fatal(err)};defer d.Close()
	sb,err:=format.NewSuperblock(0);if err!=nil{t.Fatal(err)}
	pair,err:=format.VolumeDescriptorPairOffset(sb,0);if err!=nil{t.Fatal(err)}
	desc:=format.VolumeDescriptor{VolumeID:"bad",Generation:1,VolumeSequence:1,OwnerEpoch:1,ArenaStart:2,ArenaPages:4,RootPage:^uint64(0)}
	image,err:=desc.Encode();if err!=nil{t.Fatal(err)}
	if err=writeAtFull(d,image,int64(pair));err!=nil{t.Fatal(err)}
	if _,err=ReadVolumeDescriptor(d,sb,0);err==nil{t.Fatal("descriptor outside Reserved region accepted")}
}
