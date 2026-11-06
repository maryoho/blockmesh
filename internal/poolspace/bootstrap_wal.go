package poolspace

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"

	"blockmesh/internal/device"
)

const (
	bootstrapWALIntentOffset uint64 = 4<<20 + 2*PageSize
	bootstrapWALCommitOffset uint64 = bootstrapWALIntentOffset + PageSize
	bootstrapWALIntentMagic uint64 = 0x424D4257414C4931
	bootstrapWALCommitMagic uint64 = 0x424D4257414C4331
	bootstrapWALCRCOffset = 40
)

var ErrBootstrapWALCorrupt=errors.New("Bootstrap WAL is corrupt")
type bootstrapIntent struct{TxID uint64;TargetSlot uint64;RecordCRC uint32;IntentCRC uint32}

func encodeBootstrapIntent(txID,targetSlot uint64,record []byte)([]byte,bootstrapIntent,error){
	var in bootstrapIntent
	if txID==0||(targetSlot!=BootstrapSlotA&&targetSlot!=BootstrapSlotB)||len(record)!=int(PageSize){return nil,in,ErrInvalidGeometry}
	b:=make([]byte,PageSize)
	binary.LittleEndian.PutUint64(b[0:8],bootstrapWALIntentMagic)
	binary.LittleEndian.PutUint32(b[8:12],BootstrapVersion)
	binary.LittleEndian.PutUint64(b[16:24],txID)
	binary.LittleEndian.PutUint64(b[24:32],targetSlot)
	in.RecordCRC=crc32.Checksum(record,crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(b[32:36],in.RecordCRC)
	binary.LittleEndian.PutUint32(b[bootstrapWALCRCOffset:bootstrapWALCRCOffset+4],0)
	in.IntentCRC=crc32.Checksum(b,crc32.MakeTable(crc32.Castagnoli))
	binary.LittleEndian.PutUint32(b[bootstrapWALCRCOffset:bootstrapWALCRCOffset+4],in.IntentCRC)
	in.TxID,in.TargetSlot=txID,targetSlot
	return b,in,nil
}
func decodeBootstrapIntent(b []byte)(bootstrapIntent,error){
	var in bootstrapIntent
	if len(b)!=int(PageSize)||binary.LittleEndian.Uint64(b[:8])!=bootstrapWALIntentMagic||binary.LittleEndian.Uint32(b[8:12])!=BootstrapVersion{return in,ErrBootstrapWALCorrupt}
	c:=append([]byte(nil),b...)
	want:=binary.LittleEndian.Uint32(c[bootstrapWALCRCOffset:bootstrapWALCRCOffset+4])
	binary.LittleEndian.PutUint32(c[bootstrapWALCRCOffset:bootstrapWALCRCOffset+4],0)
	if crc32.Checksum(c,crc32.MakeTable(crc32.Castagnoli))!=want{return in,ErrBootstrapWALCorrupt}
	in.TxID=binary.LittleEndian.Uint64(b[16:24]);in.TargetSlot=binary.LittleEndian.Uint64(b[24:32])
	in.RecordCRC=binary.LittleEndian.Uint32(b[32:36]);in.IntentCRC=want
	if in.TxID==0||(in.TargetSlot!=BootstrapSlotA&&in.TargetSlot!=BootstrapSlotB){return bootstrapIntent{},ErrBootstrapWALCorrupt}
	return in,nil
}
func encodeBootstrapCommit(in bootstrapIntent)[]byte{
	b:=make([]byte,PageSize)
	binary.LittleEndian.PutUint64(b[0:8],bootstrapWALCommitMagic)
	binary.LittleEndian.PutUint32(b[8:12],BootstrapVersion)
	binary.LittleEndian.PutUint64(b[16:24],in.TxID)
	binary.LittleEndian.PutUint32(b[24:28],in.IntentCRC)
	binary.LittleEndian.PutUint32(b[bootstrapWALCRCOffset:bootstrapWALCRCOffset+4],0)
	binary.LittleEndian.PutUint32(b[bootstrapWALCRCOffset:bootstrapWALCRCOffset+4],crc32.Checksum(b,crc32.MakeTable(crc32.Castagnoli)))
	return b
}
func validBootstrapCommit(b []byte,in bootstrapIntent)bool{
	if len(b)!=int(PageSize)||binary.LittleEndian.Uint64(b[:8])!=bootstrapWALCommitMagic||binary.LittleEndian.Uint32(b[8:12])!=BootstrapVersion||binary.LittleEndian.Uint64(b[16:24])!=in.TxID||binary.LittleEndian.Uint32(b[24:28])!=in.IntentCRC{return false}
	c:=append([]byte(nil),b...);want:=binary.LittleEndian.Uint32(c[bootstrapWALCRCOffset:bootstrapWALCRCOffset+4])
	binary.LittleEndian.PutUint32(c[bootstrapWALCRCOffset:bootstrapWALCRCOffset+4],0)
	return crc32.Checksum(c,crc32.MakeTable(crc32.Castagnoli))==want
}

// recoverBootstrapWAL returns a slot to ignore if its after-image has no
// matching durable Commit. It is safe to replay after an uncertain write.
func recoverBootstrapWAL(dev device.Device)(uint64,error){
	intentPage:=make([]byte,PageSize);commitPage:=make([]byte,PageSize)
	if err:=readFull(dev,intentPage,int64(bootstrapWALIntentOffset));err!=nil{return 0,err}
	if err:=readFull(dev,commitPage,int64(bootstrapWALCommitOffset));err!=nil{return 0,err}
	if allZero(intentPage)&&allZero(commitPage){return math.MaxUint64,nil}
	if allZero(intentPage){return 0,ErrBootstrapWALCorrupt}
	in,err:=decodeBootstrapIntent(intentPage);if err!=nil{return 0,err}
	if !validBootstrapCommit(commitPage,in){return in.TargetSlot,nil}
	recordPage:=make([]byte,PageSize)
	if err=readFull(dev,recordPage,int64(in.TargetSlot));err!=nil{return 0,err}
	if crc32.Checksum(recordPage,crc32.MakeTable(crc32.Castagnoli))!=in.RecordCRC{return 0,ErrBootstrapWALCorrupt}
	r,err:=decodeBootstrap(recordPage,in.TargetSlot)
	if err!=nil||r.Generation!=in.TxID{return 0,ErrBootstrapWALCorrupt}
	return math.MaxUint64,nil
}

func allZero(b []byte) bool { for _, v := range b { if v != 0 { return false } }; return true }
