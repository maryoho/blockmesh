package device

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

type memoryDevice struct {
	data []byte
	closed bool
}
func (m *memoryDevice) ReadAt(p []byte, off int64) (int,error) {
	if off<0 || off>=int64(len(m.data)) { return 0,io.EOF }
	n:=copy(p,m.data[off:])
	if n<len(p) { return n,io.EOF }
	return n,nil
}
func (m *memoryDevice) WriteAt(p []byte, off int64) (int,error) {
	if m.closed { return 0,ErrClosed }
	if off<0 || off+int64(len(p))>int64(len(m.data)) { return 0,io.ErrShortWrite }
	return copy(m.data[off:],p),nil
}
func (m *memoryDevice) Sync() error { return nil }
func (m *memoryDevice) Size() int64 { return int64(len(m.data)) }
func (m *memoryDevice) Close() error { m.closed=true; return nil }

func TestRangeSharesParentAndTranslatesOffsets(t *testing.T) {
	parent:=&memoryDevice{data:make([]byte,64)}
	meta,err:=NewRange(parent,0,16); if err!=nil { t.Fatal(err) }
	data,err:=NewRange(parent,16,48); if err!=nil { t.Fatal(err) }
	payload:=[]byte("blockmesh")
	if n,err:=data.WriteAt(payload,3); err!=nil || n!=len(payload) { t.Fatalf("write: n=%d err=%v",n,err) }
	got:=make([]byte,len(payload))
	if n,err:=parent.ReadAt(got,19); err!=nil || n!=len(got) { t.Fatalf("parent read: n=%d err=%v",n,err) }
	if !bytes.Equal(got,payload) { t.Fatalf("translated data mismatch: %q",got) }
	if _,err:=meta.WriteAt(payload,0); err!=nil { t.Fatal(err) }
	if bytes.Equal(parent.data[:len(payload)],payload)==false { t.Fatal("metadata view did not address its own region") }
}

func TestRangeRejectsOutOfBoundsWritesWithoutPartialMutation(t *testing.T) {
	parent:=&memoryDevice{data:bytes.Repeat([]byte{0xaa},32)}
	view,err:=NewRange(parent,8,8); if err!=nil { t.Fatal(err) }
	before:=append([]byte(nil),parent.data...)
	if n,err:=view.WriteAt([]byte("0123456789"),0); err==nil || n!=0 { t.Fatalf("expected full-request rejection, n=%d err=%v",n,err) }
	if !bytes.Equal(before,parent.data) { t.Fatal("out-of-range write partially modified parent") }
	if _,err:=view.ReadAt(make([]byte,4),7); !errors.Is(err,io.EOF) { t.Fatalf("expected EOF for read crossing end, got %v",err) }
}

func TestRangeDoesNotOwnParentLifetime(t *testing.T) {
	parent:=&memoryDevice{data:make([]byte,16)}
	view,err:=NewRange(parent,4,8); if err!=nil { t.Fatal(err) }
	if err:=view.Close(); err!=nil { t.Fatal(err) }
	if _,err:=view.ReadAt(make([]byte,1),0); !errors.Is(err,ErrRangeClosed) { t.Fatalf("expected closed view error, got %v",err) }
	if parent.closed { t.Fatal("closing range must not close shared parent") }
	if _,err:=parent.WriteAt([]byte{1},0); err!=nil { t.Fatalf("parent should remain usable: %v",err) }
}

func TestNewRangeRejectsInvalidGeometry(t *testing.T) {
	parent:=&memoryDevice{data:make([]byte,16)}
	for _,tc:=range []struct{off,length uint64}{{17,0},{8,9},{0,17}} {
		if _,err:=NewRange(parent,tc.off,tc.length); err==nil { t.Fatalf("expected invalid range for %+v",tc) }
	}
}
