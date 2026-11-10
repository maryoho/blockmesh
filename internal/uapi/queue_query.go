package uapi
import("encoding/binary";"errors";"fmt")
const QueueQuerySize=40
type QueueQuery struct{AfterQueueID,QueueID,RingOrder uint32;QueueGeneration,TargetCookie uint64}
func UnmarshalQueueQuery(b []byte)(QueueQuery,error){
 var q QueueQuery
 if len(b)!=QueueQuerySize{return q,fmt.Errorf("queue query size %d, want %d",len(b),QueueQuerySize)}
 if binary.LittleEndian.Uint32(b[:4])!=QueueQuerySize{return q,errors.New("queue query ABI size mismatch")}
 if binary.LittleEndian.Uint64(b[32:40])!=0{return q,errors.New("queue query reserved field is non-zero")}
 q.AfterQueueID=binary.LittleEndian.Uint32(b[4:8]);q.QueueID=binary.LittleEndian.Uint32(b[8:12]);q.RingOrder=binary.LittleEndian.Uint32(b[12:16])
 q.QueueGeneration=binary.LittleEndian.Uint64(b[16:24]);q.TargetCookie=binary.LittleEndian.Uint64(b[24:32])
 if q.AfterQueueID>MaxQueueID||q.QueueID<=q.AfterQueueID||q.QueueID>MaxQueueID{return q,errors.New("queue query returned invalid queue ID")}
 if q.QueueGeneration==0||q.TargetCookie==0{return q,errors.New("queue query returned zero generation or target cookie")}
 if _,err:=Layout(q.RingOrder);err!=nil{return q,fmt.Errorf("queue query ring order: %w",err)}
 return q,nil
}