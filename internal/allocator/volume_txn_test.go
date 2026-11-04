package allocator_test

import (
 "context"
 "encoding/binary"
 "os"
 "testing"
 "blockmesh/internal/allocator"
 "blockmesh/internal/format"
 "blockmesh/internal/lock"
 "blockmesh/internal/metadata"
)

func TestAllocateOneTxnPublishesAllMetadata(t *testing.T) {
 path := t.TempDir()+"/metadata.img"
 f, err := os.Create(path); if err != nil { t.Fatal(err) }
 if err=f.Truncate(int64(format.MetadataSize)); err != nil { t.Fatal(err) }; _=f.Close()
 if err=metadata.Init(path,0,true); err != nil { t.Fatal(err) }
 store,err:=metadata.OpenStoreWritable(path); if err!=nil { t.Fatal(err) }; defer store.Close()
 pair,err:=allocator.VolumeDescriptorPairOffset(store.SB,0); if err!=nil { t.Fatal(err) }
 initial,err:=allocator.EncodeVolumeDescriptor(allocator.VolumeDescriptor{VolumeID:"vol-atomic",AG:0,Generation:1,VolumeSequence:1,MappingRootOffset:store.SB.MappingOffset,BitmapSequence:1,OwnerEpoch:7})
 if err!=nil { t.Fatal(err) }
 if n,e:=store.Device().WriteAt(initial,int64(pair)); e!=nil || n!=len(initial) { t.Fatalf("write descriptor n=%d err=%v",n,e) }
 if err=store.Device().Sync(); err!=nil { t.Fatal(err) }
 locks,err:=lock.NewFileLocker(t.TempDir()+"/locks"); if err!=nil { t.Fatal(err) }
 lease,err:=locks.Acquire(context.Background(),"volume:vol-atomic"); if err!=nil { t.Fatal(err) }; defer lease.Close()
 a,err:=allocator.New(store,locks); if err!=nil { t.Fatal(err) }
 off:=store.SB.MappingOffset+format.PageSize
 physical,err:=a.AllocateOneTxn(context.Background(),lease,"vol-atomic",7,0,off,pair,func(old []byte,p uint64)([]byte,error){
  next:=append([]byte(nil),old...); copy(next[:8],[]byte("BMMAP001")); binary.LittleEndian.PutUint64(next[8:16],99); binary.LittleEndian.PutUint64(next[16:24],p); return next,nil
 })
 if err!=nil { t.Fatal(err) }; if physical!=0 { t.Fatalf("physical=%d",physical) }
 allocated,free,err:=store.BitmapStats(0); if err!=nil { t.Fatal(err) }; if allocated!=1 || free!=format.AGBlocks-1 { t.Fatalf("bitmap allocated=%d free=%d",allocated,free) }
 mapping,err:=store.ReadPage(off); if err!=nil { t.Fatal(err) }; if string(mapping[:8])!="BMMAP001" || binary.LittleEndian.Uint64(mapping[16:24])!=physical { t.Fatal("mapping not committed") }
 image:=make([]byte,format.PageSize); if n,e:=store.Device().ReadAt(image,int64(pair+format.PageSize)); e!=nil || n!=len(image) { t.Fatalf("read descriptor n=%d err=%v",n,e) }
 vd,err:=allocator.DecodeVolumeDescriptor(image); if err!=nil { t.Fatal(err) }
 if vd.MappingRootOffset!=off { t.Fatalf("descriptor root=%d want=%d",vd.MappingRootOffset,off) }
 if vd.VolumeID!="vol-atomic" || vd.VolumeSequence!=2 || vd.Generation!=2 || vd.MappingRootOffset!=off || vd.BitmapSequence!=2 || vd.OwnerEpoch!=7 { t.Fatalf("unexpected descriptor: %+v",vd) }
}
