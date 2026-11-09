package metaservice

import (
    "context"
    "encoding/binary"
    "fmt"
    "testing"

    "blockmesh/internal/uapi"
)

// BenchmarkQueueMemoryDrain measures the userspace SPSC consumer and completion
// producer independently of SAN latency using reusable mapped queue memory.
func BenchmarkQueueMemoryDrain(b *testing.B) {
    for _, batch := range []int{16, 64, 256, 1024} {
        b.Run(fmt.Sprintf("requests=%d", batch), func(b *testing.B) {
            const order = uint32(10)
            const generation = uint64(41)
            const queueID = uint32(7)
            layout, err := uapi.Layout(order); if err != nil { b.Fatal(err) }
            info := uapi.SetupQueue{RingOrder:order, QueueGeneration:generation, QueueID:queueID, MaxBatch:uapi.MaxBatch,
                RequestControlOffset:uapi.EncodeMmapOffset(queueID,layout.RequestControl), RequestRingOffset:uapi.EncodeMmapOffset(queueID,layout.RequestRing),
                CompletionControlOffset:uapi.EncodeMmapOffset(queueID,layout.CompletionControl), CompletionRingOffset:uapi.EncodeMmapOffset(queueID,layout.CompletionRing),
                PayloadOffset:uapi.EncodeMmapOffset(queueID,layout.Payload)}
            q := &QueueMemory{Info:info, RequestControl:make([]byte,4096), RequestRing:make([]byte,layout.Entries*uapi.RequestSize),
                CompletionControl:make([]byte,4096), CompletionRing:make([]byte,layout.Entries*uapi.CompletionSize), Payload:make([]byte,layout.PayloadBytes)}
            for _, c := range [][]byte{q.RequestControl,q.CompletionControl} { binary.LittleEndian.PutUint32(c[8:12],layout.Entries); binary.LittleEndian.PutUint64(c[16:24],generation) }
            for i:=0;i<batch;i++ {
                req:=uapi.Request{Operation:uapi.OpLookupBatch,RequestID:uint64(i+1),QueueGeneration:generation,OwnerEpoch:9,LogicalStart:uint64(i),BlockCount:1}
                req.VolumeID[0],req.NodeID[0],req.BootID[0]=1,2,3
                raw,e:=req.MarshalBinary();if e!=nil{b.Fatal(e)};copy(q.RequestRing[i*uapi.RequestSize:],raw)
            }
            processor:=ProcessorFunc(func(_ context.Context,r uapi.Request)(Result,error){
                return Result{Mapping:&uapi.Mapping{LogicalBlock:r.LogicalStart,PhysicalBlock:r.LogicalStart+4096,BlockCount:1}},nil
            })
            b.ReportAllocs();b.SetBytes(int64(batch*uapi.RequestSize));b.ResetTimer()
            for i:=0;i<b.N;i++ {
                binary.LittleEndian.PutUint32(q.RequestControl[0:4],0);binary.LittleEndian.PutUint32(q.RequestControl[4:8],uint32(batch))
                binary.LittleEndian.PutUint32(q.CompletionControl[0:4],0);binary.LittleEndian.PutUint32(q.CompletionControl[4:8],0)
                if _,produced,e:=q.Drain(context.Background(),processor,batch);e!=nil||produced!=batch{b.Fatalf("Drain produced=%d want=%d err=%v",produced,batch,e)}
            }
        })
    }
}

// BenchmarkLookupContiguous isolates cached mapping-run lookup from device I/O.
func BenchmarkLookupContiguous(b *testing.B) {
    for _, count := range []uint32{1,4,16,64,256} {
        b.Run(fmt.Sprintf("blocks=%d",count),func(b *testing.B){
            tree:=make(testMappingTree,count);for i:=uint32(0);i<count;i++{tree[1000+uint64(i)]=9000+uint64(i)}
            b.ReportAllocs();b.ResetTimer()
            for i:=0;i<b.N;i++{if _,e:=lookupContiguous(tree,1000,count);e!=nil{b.Fatal(e)}}
        })
    }
}


// BenchmarkLookupMappingVector compares coalesced and highly fragmented
// physical layouts while keeping the logical request contiguous.
func BenchmarkLookupMappingVector(b *testing.B) {
	for _, count := range []uint32{1, 4, 16, 64, 256} {
		for _, fragmented := range []bool{false, true} {
			name := fmt.Sprintf("blocks=%d/fragmented=%t", count, fragmented)
			b.Run(name, func(b *testing.B) {
				tree := make(testMappingTree, count)
				for i := uint32(0); i < count; i++ {
					physical := uint64(9000 + i)
					if fragmented { physical = uint64(9000 + i*2) }
					tree[1000+uint64(i)] = physical
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := lookupMappingVector(tree, 1000, count); err != nil { b.Fatal(err) }
				}
			})
		}
	}
}
