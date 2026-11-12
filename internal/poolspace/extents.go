package poolspace

import (
 "fmt"
 "sort"
)

type Kind uint8
const ( Free Kind=iota; Metadata; Data )
type Extent struct { Start uint64 `json:"start"`; Length uint64 `json:"length"`; Kind Kind `json:"kind"` }
type Allocator struct { deviceBytes uint64; extents []Extent }

func NewAllocator(deviceBytes,bootstrapBytes,metadataBytes uint64)(*Allocator,error){
 if deviceBytes==0||bootstrapBytes%PageSize!=0||metadataBytes%ExtentSize!=0||bootstrapBytes>deviceBytes||metadataBytes>deviceBytes-bootstrapBytes{return nil,ErrInvalidGeometry}
 a:=&Allocator{deviceBytes:deviceBytes,extents:[]Extent{{0,bootstrapBytes,Metadata},{bootstrapBytes,metadataBytes,Metadata}}}
 if tail:=deviceBytes-bootstrapBytes-metadataBytes;tail>0{a.extents=append(a.extents,Extent{bootstrapBytes+metadataBytes,tail,Free})}
 return a,nil
}
func(a *Allocator)Rescan(n uint64)error{
 if a==nil||n<a.deviceBytes{return fmt.Errorf("device shrink from %d to %d is not supported",a.deviceBytes,n)}
 if n==a.deviceBytes{return nil}
 next:=append([]Extent(nil),a.extents...);next=append(next,Extent{a.deviceBytes,n-a.deviceBytes,Free})
 a.extents=next;a.deviceBytes=n;a.normalize();return nil
}
func(a *Allocator)AllocateMetadata(n uint64)([]Extent,error){return a.allocate(n,ExtentSize,Metadata,"metadata")}
func(a *Allocator)AllocateData(n uint64)([]Extent,error){return a.allocate(n,AllocationSize,Data,"data")}
func(a *Allocator)allocate(n,alignment uint64,kind Kind,label string)([]Extent,error){
 if a==nil||n==0||n%alignment!=0{return nil,ErrInvalidGeometry}
 // Plan against a private copy so allocation failure never consumes a prefix.
	next:=append([]Extent(nil),a.extents...);remaining:=n;var allocated []Extent
 for i:=range next{e:=&next[i];if e.Kind!=Free{continue};take:=minU64(e.Length,remaining)/alignment*alignment;if take==0{continue}
  allocated=append(allocated,Extent{e.Start,take,kind});e.Start+=take;e.Length-=take;remaining-=take;if remaining==0{break}
 }
 if remaining!=0{return nil,fmt.Errorf("insufficient free physical extents for %s expansion",label)}
 a.extents=append(next,allocated...);a.normalize();return allocated,nil
}
func(a *Allocator)Extents()[]Extent{return append([]Extent(nil),a.extents...)}
func(a *Allocator)DeviceBytes()uint64{return a.deviceBytes}
func(a *Allocator)normalize(){
 out:=make([]Extent,0,len(a.extents));for _,e:=range a.extents{if e.Length>0{out=append(out,e)}}
 sort.Slice(out,func(i,j int)bool{return out[i].Start<out[j].Start})
 merged:=out[:0];for _,e:=range out{if len(merged)>0{p:=&merged[len(merged)-1];if p.Start+p.Length==e.Start&&p.Kind==e.Kind{p.Length+=e.Length;continue}};merged=append(merged,e)}
 a.extents=merged
}
