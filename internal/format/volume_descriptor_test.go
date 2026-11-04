package format

import (
	"bytes"
	"testing"
)

func TestVolumeDescriptorV2RoundTrip(t *testing.T) {
	want:=VolumeDescriptor{VolumeID:"volume-a",AG:3,Generation:9,VolumeSequence:12,MappingRootOffset:MappingOffset,BitmapSequence:7,OwnerEpoch:4,ArenaStart:3000000,ArenaPages:10000,RootPage:3000001,MappingSequence:11,LifecycleStart:3010000,LifecyclePages:16}
	b,err:=want.Encode(); if err!=nil { t.Fatal(err) }
	got,err:=DecodeVolumeDescriptor(b); if err!=nil { t.Fatal(err) }
	if got!=want { t.Fatalf("round trip mismatch: got %+v want %+v",got,want) }
	if string(b[:8])!="BMDVOL02" { t.Fatalf("unexpected descriptor magic %q",b[:8]) }
}

func TestVolumeDescriptorRejectsLegacyAndCorruption(t *testing.T) {
	d:=VolumeDescriptor{VolumeID:"v",Generation:1,VolumeSequence:1,OwnerEpoch:1,ArenaStart:10,ArenaPages:20,RootPage:^uint64(0)}
	b,err:=d.Encode(); if err!=nil { t.Fatal(err) }
	legacy:=append([]byte(nil),b...); copy(legacy[:8],[]byte("BMDVOL01"))
	if _,err=DecodeVolumeDescriptor(legacy); err==nil { t.Fatal("legacy descriptor accepted") }
	corrupt:=append([]byte(nil),b...); corrupt[130]^=1
	if _,err=DecodeVolumeDescriptor(corrupt); err==nil { t.Fatal("corrupt descriptor accepted") }
}

func TestVolumeDescriptorRejectsOverlappingRegions(t *testing.T) {
	d:=VolumeDescriptor{VolumeID:"v",Generation:1,VolumeSequence:1,OwnerEpoch:1,ArenaStart:10,ArenaPages:20,RootPage:^uint64(0),LifecycleStart:25,LifecyclePages:8}
	if _,err:=d.Encode(); err==nil { t.Fatal("overlapping lifecycle and arena accepted") }
}

func TestPoolSuperblockV2MagicAndLayout(t *testing.T) {
	if err:=ValidateLayout(); err!=nil { t.Fatal(err) }
	sb,err:=NewSuperblock(0); if err!=nil { t.Fatal(err) }
	b:=sb.Encode()
	if !bytes.Equal(b[:8],[]byte{0x32,0x30,0x4c,0x4f,0x4f,0x50,0x4d,0x42}) { t.Fatalf("unexpected Pool v2 magic bytes %x",b[:8]) }
	got,err:=DecodeSuperblock(b); if err!=nil { t.Fatal(err) }
	if err=got.Validate(); err!=nil { t.Fatal(err) }
}

func TestPoolSuperblockRejectsLegacyMagic(t *testing.T) {
	sb,err:=NewSuperblock(0); if err!=nil { t.Fatal(err) }
	b:=sb.Encode()
	// Legacy P1/P2 Pool superblocks used BMMETA01.
	copy(b[:8],[]byte("BMMETA01"))
	if _,err:=DecodeSuperblock(b); err==nil { t.Fatal("legacy Pool superblock accepted") }
}
