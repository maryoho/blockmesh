package uapi
import("encoding/binary";"testing")
func TestQueueQueryUnmarshal(t *testing.T){
 b:=make([]byte,QueueQuerySize);binary.LittleEndian.PutUint32(b[:4],QueueQuerySize);binary.LittleEndian.PutUint32(b[4:8],3)
 binary.LittleEndian.PutUint32(b[8:12],9);binary.LittleEndian.PutUint32(b[12:16],6);binary.LittleEndian.PutUint64(b[16:24],77);binary.LittleEndian.PutUint64(b[24:32],0x1234)
 got,err:=UnmarshalQueueQuery(b);if err!=nil{t.Fatal(err)}
 want:=QueueQuery{AfterQueueID:3,QueueID:9,RingOrder:6,QueueGeneration:77,TargetCookie:0x1234};if got!=want{t.Fatalf("got %+v want %+v",got,want)}
}
func TestQueueQueryRejectsMalformedFields(t *testing.T){
 base:=make([]byte,QueueQuerySize);binary.LittleEndian.PutUint32(base[:4],QueueQuerySize);binary.LittleEndian.PutUint32(base[4:8],1)
 binary.LittleEndian.PutUint32(base[8:12],2);binary.LittleEndian.PutUint32(base[12:16],MinRingOrder);binary.LittleEndian.PutUint64(base[16:24],1);binary.LittleEndian.PutUint64(base[24:32],1)
 tests:=[]struct{name string;edit func([]byte)}{
 {"zero generation",func(b []byte){binary.LittleEndian.PutUint64(b[16:24],0)}},
 {"zero cookie",func(b []byte){binary.LittleEndian.PutUint64(b[24:32],0)}},
 {"bad cursor",func(b []byte){binary.LittleEndian.PutUint32(b[8:12],1)}},
 {"reserved",func(b []byte){binary.LittleEndian.PutUint64(b[32:40],1)}},
 {"bad order",func(b []byte){binary.LittleEndian.PutUint32(b[12:16],0)}},
 }
 for _,tt:=range tests{t.Run(tt.name,func(t *testing.T){b:=append([]byte(nil),base...);tt.edit(b);if _,err:=UnmarshalQueueQuery(b);err==nil{t.Fatal("expected validation error")}})}
}