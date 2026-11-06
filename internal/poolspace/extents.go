package poolspace

import (
	"errors"
	"fmt"
	"sort"
)

type Kind uint8
const (
	Free Kind = iota
	Metadata
	Data
)

type Extent struct {
	Start uint64 `json:"start"`
	Length uint64 `json:"length"`
	Kind Kind `json:"kind"`
}

type Allocator struct { deviceBytes uint64; extents []Extent }

func NewAllocator(deviceBytes, bootstrapBytes, metadataBytes uint64) (*Allocator,error) {
	if deviceBytes==0 || bootstrapBytes%ExtentSize!=0 || metadataBytes%ExtentSize!=0 ||
		bootstrapBytes>deviceBytes || metadataBytes>deviceBytes-bootstrapBytes { return nil,ErrInvalidGeometry }
	a:=&Allocator{deviceBytes:deviceBytes,extents:[]Extent{{0,bootstrapBytes,Metadata},{bootstrapBytes,metadataBytes,Metadata}}}
	if tail:=deviceBytes-bootstrapBytes-metadataBytes;tail>0 { a.extents=append(a.extents,Extent{bootstrapBytes+metadataBytes,tail,Free}) }
	return a,nil
}

// Rescan appends only newly visible device capacity. Shrink is always rejected;
// newly discovered bytes remain Free and are never silently exposed to data I/O.
func (a *Allocator) Rescan(newDeviceBytes uint64) error {
	if a==nil || newDeviceBytes<a.deviceBytes { return fmt.Errorf("device shrink from %d to %d is not supported",a.deviceBytes,newDeviceBytes) }
	if newDeviceBytes==a.deviceBytes{return nil}
	a.extents=append(a.extents,Extent{Start:a.deviceBytes,Length:newDeviceBytes-a.deviceBytes,Kind:Free})
	a.deviceBytes=newDeviceBytes
	a.normalize()
	return nil
}

// AllocateMetadata reserves the lowest-address free extents sufficient for
// bytes. Allocation is all-or-nothing; no Data extent is moved or rewritten.
func (a *Allocator) AllocateMetadata(bytes uint64) ([]Extent,error) {
	if a==nil || bytes==0 || bytes%ExtentSize!=0 { return nil,ErrInvalidGeometry }
	remaining:=bytes
	var allocated []Extent
	for i:=range a.extents {
		e:=&a.extents[i]
		if e.Kind!=Free {continue}
		take:=minU64(e.Length,remaining)
		take=take/ExtentSize*ExtentSize
		if take==0 {continue}
		allocated=append(allocated,Extent{e.Start,take,Metadata})
		e.Start+=take;e.Length-=take;remaining-=take
		if remaining==0 {break}
	}
	if remaining!=0 { return nil,errors.New("insufficient free physical extents for metadata expansion") }
	a.extents=append(a.extents,allocated...)
	a.normalize()
	return allocated,nil
}

func (a *Allocator) AllocateData(bytes uint64)([]Extent,error){
	if a==nil || bytes==0 || bytes%AllocationSize!=0{return nil,ErrInvalidGeometry}
	remaining:=bytes
	var allocated []Extent
	for i:=range a.extents {
		e:=&a.extents[i]; if e.Kind!=Free {continue}
		take:=minU64(e.Length,remaining)/AllocationSize*AllocationSize
		if take==0{continue}
		allocated=append(allocated,Extent{e.Start,take,Data})
		e.Start+=take;e.Length-=take;remaining-=take
		if remaining==0{break}
	}
	if remaining!=0{return nil,errors.New("insufficient free physical extents for data allocation")}
	a.extents=append(a.extents,allocated...);a.normalize();return allocated,nil
}

func (a *Allocator) Extents()[]Extent{out:=append([]Extent(nil),a.extents...);return out}
func (a *Allocator) DeviceBytes()uint64{return a.deviceBytes}
func (a *Allocator) normalize(){
	out:=make([]Extent,0,len(a.extents))
	for _,e:=range a.extents{if e.Length==0{continue};out=append(out,e)}
	sort.Slice(out,func(i,j int)bool{return out[i].Start<out[j].Start})
	merged:=out[:0]
	for _,e:=range out{
		if len(merged)>0{p:=&merged[len(merged)-1];if p.Start+p.Length==e.Start&&p.Kind==e.Kind{p.Length+=e.Length;continue}}
		merged=append(merged,e)
	}
	a.extents=merged
}
