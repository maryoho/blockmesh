package poolspace

import (
	"bytes"
	"io"
	"testing"

	"blockmesh/internal/device"
)

func TestDataViewMapsFragmentedExtentsOnSameDevice(t *testing.T){
	f,err:=device.OpenFile(t.TempDir()+"/data.img");if err!=nil{t.Fatal(err)};defer f.Close()
	if _,err=f.WriteAt([]byte{0},(32<<20)-1);err!=nil{t.Fatal(err)}
	extents:=[]Extent{{Start:4<<20,Length:4<<20,Kind:Data},{Start:16<<20,Length:4<<20,Kind:Data}}
	view,err:=NewDataView(f,extents);if err!=nil{t.Fatal(err)}
	payload:=bytes.Repeat([]byte{0x5a},5<<20)
	if n,err:=view.WriteAt(payload,0);err!=nil||n!=len(payload){t.Fatalf("write n=%d err=%v",n,err)}
	first:=make([]byte,4<<20);if _,err=f.ReadAt(first,4<<20);err!=nil{t.Fatal(err)}
	second:=make([]byte,1<<20);if _,err=f.ReadAt(second,16<<20);err!=nil{t.Fatal(err)}
	if !bytes.Equal(first,payload[:4<<20])||!bytes.Equal(second,payload[4<<20:]){t.Fatal("DataView did not translate fragmented physical extents")}
	if _,err=view.WriteAt([]byte{1},view.Size());err==nil{t.Fatal("write at end should fail")}
	if _,err=view.ReadAt(make([]byte,2),view.Size()-1);err!=io.EOF{t.Fatalf("short read err=%v",err)}
}
