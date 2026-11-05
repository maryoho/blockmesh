package runtimeevents

import ("testing"; "time")

func validRecoveryRecords() []Record {
	names:=[]string{"sanlock.owner_lease_acquired","scsi_pr.registration_prepared","scsi_pr.preempt_and_abort","old_owner.metadata_io_drain","owner_record.durable","scsi_pr.owner_reservation_verified","dm.owner_gate.fenced","wal.recovery.completed","descriptor.owner_epoch.durable","dm.owner_gate.prepared","dm.owner_gate.enabled"}
	var out []Record
	prev:=""
	for i,name:=range names{
		epoch:=uint64(0)
		if name=="owner_record.durable" || name=="scsi_pr.owner_reservation_verified" || name=="descriptor.owner_epoch.durable" || name=="dm.owner_gate.prepared" || name=="dm.owner_gate.enabled"{epoch=12}
		e:=Event{SchemaVersion:SchemaVersion,OperationID:"op",VolumeID:"vol",NodeID:"node-b",BootID:"boot-b",OwnerEpoch:epoch,EventType:name,Phase:"test",Result:"success",Proof:"test",ObservedAt:time.Now().UTC()}
		h,_:=recordHash(uint64(i+1),prev,e)
		out=append(out,Record{Sequence:uint64(i+1),PreviousHash:prev,Event:e,Hash:h})
		prev=h
	}
	return out
}

func rechain(records []Record) {
	prev:=""
	for i:=range records{
		records[i].Sequence=uint64(i+1)
		records[i].PreviousHash=prev
		records[i].Hash,_=recordHash(records[i].Sequence,prev,records[i].Event)
		prev=records[i].Hash
	}
}

func TestVerifyRecoveryAcceptsOrderedRuntimeEvidence(t *testing.T){
	got,err:=VerifyRecovery(validRecoveryRecords());if err!=nil{t.Fatal(err)}
	if got.OwnerEpoch!=12 || got.OperationID!="op" || got.VolumeID!="vol"{t.Fatalf("bad export: %+v",got)}
}

func TestVerifyRecoveryRejectsMissingReorderedAndMismatchedEpoch(t *testing.T){
	t.Run("missing",func(t *testing.T){r:=validRecoveryRecords();r=r[:len(r)-1];if _,err:=VerifyRecovery(r);err==nil{t.Fatal("missing gate enable event accepted")}})
	t.Run("reordered",func(t *testing.T){r:=validRecoveryRecords();r[7],r[8]=r[8],r[7];rechain(r);if _,err:=VerifyRecovery(r);err==nil{t.Fatal("out-of-order recovery accepted")}})
	t.Run("epoch",func(t *testing.T){r:=validRecoveryRecords();r[8].Event.OwnerEpoch=11;rechain(r);if _,err:=VerifyRecovery(r);err==nil{t.Fatal("descriptor epoch mismatch accepted")}})
	t.Run("failure",func(t *testing.T){r:=validRecoveryRecords();r[5].Event.Result="failure";rechain(r);if _,err:=VerifyRecovery(r);err==nil{t.Fatal("failure event accepted")}})
}
