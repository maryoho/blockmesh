package poolspace

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
)

const (
	ExtentMapMagic uint64 = 0x424D4558544D4150 // BMEXTMAP
	ExtentMapVersion uint32 = 1
	extentMapHeader = 64
	extentMapEntry = 24
)

type ExtentMap struct { Generation uint64; DeviceBytes uint64; Extents []Extent }

func (m ExtentMap) Encode()([]byte,error){
	if m.Generation==0||m.DeviceBytes==0||len(m.Extents)==0{return nil,ErrInvalidGeometry}
	if len(m.Extents)>(math.MaxInt-extentMapHeader)/extentMapEntry{return nil,ErrInvalidGeometry}
	b:=make([]byte,extentMapHeader+len(m.Extents)*extentMapEntry)
	binary.LittleEndian.PutUint64(b[0:8],ExtentMapMagic)
	binary.LittleEndian.PutUint32(b[8:12],ExtentMapVersion)
	binary.LittleEndian.PutUint32(b[12:16],uint32(len(m.Extents)))
	binary.LittleEndian.PutUint64(b[16:24],m.Generation)
	binary.LittleEndian.PutUint64(b[24:32],m.DeviceBytes)
	var cursor uint64
	for i,e:=range m.Extents{
		if e.Length==0||e.Start!=cursor||e.Length>m.DeviceBytes-cursor||e.Kind>Data{return nil,ErrInvalidGeometry}
		off:=extentMapHeader+i*extentMapEntry
		binary.LittleEndian.PutUint64(b[off:off+8],e.Start)
		binary.LittleEndian.PutUint64(b[off+8:off+16],e.Length)
		b[off+16]=byte(e.Kind)
		cursor+=e.Length
	}
	if cursor!=m.DeviceBytes{return nil,ErrInvalidGeometry}
	binary.LittleEndian.PutUint32(b[32:36],crc32.Checksum(b,crc32.MakeTable(crc32.Castagnoli)))
	return b,nil
}
func DecodeExtentMap(b []byte)(ExtentMap,error){
	var m ExtentMap
	if len(b)<extentMapHeader||binary.LittleEndian.Uint64(b[:8])!=ExtentMapMagic||binary.LittleEndian.Uint32(b[8:12])!=ExtentMapVersion{return m,errors.New("invalid extent map header")}
	n:=int(binary.LittleEndian.Uint32(b[12:16]))
	if n==0||n>(math.MaxInt-extentMapHeader)/extentMapEntry||len(b)!=extentMapHeader+n*extentMapEntry{return m,ErrInvalidGeometry}
	want:=binary.LittleEndian.Uint32(b[32:36]);c:=append([]byte(nil),b...);binary.LittleEndian.PutUint32(c[32:36],0)
	if crc32.Checksum(c,crc32.MakeTable(crc32.Castagnoli))!=want{return m,errors.New("extent map checksum mismatch")}
	m.Generation=binary.LittleEndian.Uint64(b[16:24]);m.DeviceBytes=binary.LittleEndian.Uint64(b[24:32])
	m.Extents=make([]Extent,n)
	var cursor uint64
	for i:=0;i<n;i++{off:=extentMapHeader+i*extentMapEntry;e:=Extent{Start:binary.LittleEndian.Uint64(b[off:off+8]),Length:binary.LittleEndian.Uint64(b[off+8:off+16]),Kind:Kind(b[off+16])};if e.Start!=cursor||e.Length==0||e.Length>m.DeviceBytes-cursor||e.Kind>Data{return ExtentMap{},ErrInvalidGeometry};for _,v:=range b[off+17:off+24]{if v!=0{return ExtentMap{},ErrInvalidGeometry}};cursor+=e.Length;m.Extents[i]=e}
	if m.Generation==0||cursor!=m.DeviceBytes{return ExtentMap{},ErrInvalidGeometry}
	return m,nil
}
