package lock

import (
 "crypto/sha256"
 "encoding/binary"
 "errors"
 "fmt"
 "math"
 "sync"
)

// HashedResourceOffsets maps arbitrary, stable sanlock resource names into an
// administrator-reserved byte range. A hash-slot collision is rejected rather
// than silently aliasing two lock resources. All cluster nodes must use the
// same base/slot configuration and discover the same Pool/Volume identities.
type HashedResourceOffsets struct {
 base uint64
 slots uint64
 alignment uint64
 mu sync.Mutex
 owners map[uint64]string
}

func NewHashedResourceOffsets(base,slots,alignment uint64)(*HashedResourceOffsets,error){
 if alignment!=1<<20 && alignment!=2<<20 && alignment!=4<<20 && alignment!=8<<20 {
  return nil,errors.New("sanlock resource alignment must be 1, 2, 4 or 8 MiB")
 }
 if base==0 || base%alignment!=0 || slots==0 || slots-1>(math.MaxUint64-base)/alignment {
  return nil,errors.New("dynamic sanlock resource range requires a nonzero base aligned to the resource alignment and a non-overflowing positive slot count")
 }
 return &HashedResourceOffsets{base:base,slots:slots,alignment:alignment,owners:make(map[uint64]string)},nil
}

// Offset returns a deterministic sanlock-aligned resource slot. It remembers every
// mapping observed by this daemon and fails closed if two names collide.
func(r *HashedResourceOffsets)Offset(name string)(uint64,error){
 if r==nil || name=="" { return 0,errors.New("dynamic sanlock resource name is required") }
 sum:=sha256.Sum256([]byte(name))
 slot:=binary.BigEndian.Uint64(sum[:8])%r.slots
 if slot>(math.MaxUint64-r.base)/r.alignment { return 0,errors.New("dynamic sanlock resource offset overflow") }
 offset:=r.base+slot*r.alignment
 r.mu.Lock();defer r.mu.Unlock()
 if previous,ok:=r.owners[offset];ok && previous!=name {
  return 0,fmt.Errorf("dynamic sanlock resource hash collision: %q and %q map to offset %d",previous,name,offset)
 }
 r.owners[offset]=name
 return offset,nil
}

// Range returns the configured inclusive byte range for config validation.
func(r *HashedResourceOffsets)Range()(uint64,uint64){
 if r==nil{return 0,0};return r.base,r.base+(r.slots-1)*r.alignment
}
