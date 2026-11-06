package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	DynamicCheckpointMagic uint64 = 0x424D434850303031 // BMCHP001
	DynamicCheckpointCRCOffset = 40
)

type DynamicCheckpoint struct {
	AG uint32
	Generation uint64
	CheckpointTxID uint64
	ActiveBank uint32
}

func (c DynamicCheckpoint) Encode()([]byte,error){
	if c.Generation==0||c.ActiveBank>1{return nil,errors.New("invalid BMPOOL03 checkpoint descriptor")}
	b:=make([]byte,PageSize)
	binary.LittleEndian.PutUint64(b[:8],DynamicCheckpointMagic)
	binary.LittleEndian.PutUint32(b[8:12],1)
	binary.LittleEndian.PutUint32(b[12:16],c.AG)
	binary.LittleEndian.PutUint64(b[16:24],c.Generation)
	binary.LittleEndian.PutUint64(b[24:32],c.CheckpointTxID)
	binary.LittleEndian.PutUint32(b[32:36],c.ActiveBank)
	binary.LittleEndian.PutUint32(b[DynamicCheckpointCRCOffset:DynamicCheckpointCRCOffset+4],crc32.Checksum(b,castagnoli))
	return b,nil
}

func DecodeDynamicCheckpoint(b []byte,expectedAG uint32)(DynamicCheckpoint,error){
	var c DynamicCheckpoint
	if len(b)!=int(PageSize)||binary.LittleEndian.Uint64(b[:8])!=DynamicCheckpointMagic||binary.LittleEndian.Uint32(b[8:12])!=1{return c,errors.New("invalid BMPOOL03 checkpoint descriptor")}
	copyBuf:=append([]byte(nil),b...)
	stored:=binary.LittleEndian.Uint32(copyBuf[DynamicCheckpointCRCOffset:DynamicCheckpointCRCOffset+4])
	binary.LittleEndian.PutUint32(copyBuf[DynamicCheckpointCRCOffset:DynamicCheckpointCRCOffset+4],0)
	if crc32.Checksum(copyBuf,castagnoli)!=stored{return c,errors.New("BMPOOL03 checkpoint CRC32C mismatch")}
	c=DynamicCheckpoint{AG:binary.LittleEndian.Uint32(b[12:16]),Generation:binary.LittleEndian.Uint64(b[16:24]),CheckpointTxID:binary.LittleEndian.Uint64(b[24:32]),ActiveBank:binary.LittleEndian.Uint32(b[32:36])}
	if c.AG!=expectedAG||c.Generation==0||c.ActiveBank>1{return DynamicCheckpoint{},errors.New("BMPOOL03 checkpoint descriptor fields invalid")}
	return c,nil
}

func DynamicCheckpointOffset(g DynamicGeometry,ag,slot uint32)(uint64,error){
	if ag>=g.AGCount||slot>1{return 0,errors.New("BMPOOL03 checkpoint index out of range")}
	need:=uint64(g.AGCount)*2*PageSize
	if need>g.CheckpointSize{return 0,fmt.Errorf("%w: checkpoint region needs %d bytes, has %d",ErrInvalidDynamicGeometry,need,g.CheckpointSize)}
	rel:=(uint64(ag)*2+uint64(slot))*PageSize
	if g.CheckpointOffset>g.MetadataBytes||rel>g.MetadataBytes-g.CheckpointOffset||PageSize>g.MetadataBytes-g.CheckpointOffset-rel{return 0,ErrInvalidDynamicGeometry}
	return g.CheckpointOffset+rel,nil
}

// InitializeDynamicCheckpoints writes identical initial descriptors for every
// AG. It is called during format before the BMPOOL03 superblock is published.
func InitializeDynamicCheckpoints(w io.WriterAt,g DynamicGeometry)error{
	if w==nil{return errors.New("nil checkpoint writer")}
	if uint64(g.AGCount)*2*PageSize>g.CheckpointSize{return ErrInvalidDynamicGeometry}
	for ag:=uint32(0);ag<g.AGCount;ag++{
		b,err:=(DynamicCheckpoint{AG:ag,Generation:1,ActiveBank:0}).Encode();if err!=nil{return err}
		for slot:=uint32(0);slot<2;slot++{
			off,err:=DynamicCheckpointOffset(g,ag,slot);if err!=nil{return err}
			n,err:=w.WriteAt(b,int64(off));if err!=nil{return err};if n!=len(b){return io.ErrShortWrite}
		}
	}
	return nil
}
