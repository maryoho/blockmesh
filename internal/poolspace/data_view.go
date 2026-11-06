package poolspace

import (
	"errors"
	"io"
	"math"

	"blockmesh/internal/device"
)

// DataView presents fragmented physical Data extents as one logical byte device.
// Metadata and Data I/O always target the same parent Device.
type DataView struct {
	parent device.Device
	extents []Extent
	starts []uint64
	size uint64
}

func NewDataView(parent device.Device,extents []Extent)(*DataView,error){
	if parent==nil||parent.Size()<0||len(extents)==0{return nil,ErrInvalidGeometry}
	v:=&DataView{parent:parent,extents:append([]Extent(nil),extents...),starts:make([]uint64,len(extents))}
	var logical,previousEnd uint64
	for i,e:=range v.extents{
		if e.Kind!=Data||e.Length==0||e.Start>math.MaxUint64-e.Length||e.Start+e.Length>uint64(parent.Size()){return nil,ErrInvalidGeometry}
		if i>0&&e.Start<previousEnd{return nil,ErrInvalidGeometry}
		v.starts[i]=logical
		if logical>math.MaxUint64-e.Length{return nil,ErrInvalidGeometry}
		logical+=e.Length;previousEnd=e.Start+e.Length
	}
	if logical>uint64(math.MaxInt64){return nil,ErrInvalidGeometry}
	v.size=logical
	return v,nil
}
func(v *DataView)Size()int64{if v==nil{return 0};return int64(v.size)}
func(v *DataView)Sync()error{if v==nil||v.parent==nil{return ErrInvalidGeometry};return v.parent.Sync()}
func(v *DataView)ReadAt(p []byte,off int64)(int,error){
	if v==nil||v.parent==nil{return 0,ErrInvalidGeometry}
	if off<0{return 0,errors.New("negative DataView read offset")}
	if uint64(off)>=v.size{if len(p)==0&&uint64(off)==v.size{return 0,nil};return 0,io.EOF}
	remaining:=v.size-uint64(off);want:=uint64(len(p));short:=want>remaining;if short{want=remaining}
	var done uint64
	for done<want{
		logical:=uint64(off)+done;i:=v.findExtent(logical);if i<0{return int(done),ErrInvalidGeometry}
		within:=logical-v.starts[i];n:=v.extents[i].Length-within;if n>want-done{n=want-done}
		got,err:=v.parent.ReadAt(p[int(done):int(done+n)],int64(v.extents[i].Start+within));done+=uint64(got)
		if err!=nil{return int(done),err};if uint64(got)!=n{return int(done),io.ErrUnexpectedEOF}
	}
	if short{return int(done),io.EOF};return int(done),nil
}
func(v *DataView)WriteAt(p []byte,off int64)(int,error){
	if v==nil||v.parent==nil{return 0,ErrInvalidGeometry}
	if off<0{return 0,errors.New("negative DataView write offset")}
	if uint64(off)>v.size||uint64(len(p))>v.size-uint64(off){return 0,io.ErrShortWrite}
	var done uint64
	for done<uint64(len(p)){
		logical:=uint64(off)+done;i:=v.findExtent(logical);if i<0{return int(done),ErrInvalidGeometry}
		within:=logical-v.starts[i];n:=v.extents[i].Length-within;if n>uint64(len(p))-done{n=uint64(len(p))-done}
		wrote,err:=v.parent.WriteAt(p[int(done):int(done+n)],int64(v.extents[i].Start+within));done+=uint64(wrote)
		if err!=nil{return int(done),err};if uint64(wrote)!=n{return int(done),io.ErrShortWrite}
	}
	return int(done),nil
}
func(v *DataView)findExtent(logical uint64)int{
	lo,hi:=0,len(v.starts)
	for lo<hi{mid:=(lo+hi)/2;if v.starts[mid]<=logical{lo=mid+1}else{hi=mid}}
	i:=lo-1;if i<0||logical-v.starts[i]>=v.extents[i].Length{return -1};return i
}
