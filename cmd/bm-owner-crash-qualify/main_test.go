package main

import "testing"

func TestVerifySnapshotsRequiresMonotonicEpochAndMatchingDescriptor(t *testing.T) {
	before:=snapshot{
		Schema:"blockmesh-owner-crash-evidence/v1",VolumeID:"vol-a",PoolUUID:"pool-a",VolumeIndex:3,
		DescriptorPages:[]pageEvidence{{Valid:true,Generation:7,OwnerEpoch:12}},
		OwnerRecordPages:[]pageEvidence{{Valid:true,Generation:9,OwnerEpoch:13}},
		SelectedOwner:&ownerIdentity{VolumeID:"vol-a",NodeID:"node-a",BootID:"boot-a",Epoch:13},
	}
	after:=snapshot{
		Schema:"blockmesh-owner-crash-evidence/v1",VolumeID:"vol-a",PoolUUID:"pool-a",VolumeIndex:3,
		DescriptorPages:[]pageEvidence{{Valid:true,Generation:8,OwnerEpoch:14}},
		OwnerRecordPages:[]pageEvidence{{Valid:true,Generation:10,OwnerEpoch:14}},
		SelectedOwner:&ownerIdentity{VolumeID:"vol-a",NodeID:"node-b",BootID:"boot-b",Epoch:14},
	}
	got,err:=verifySnapshots(before,after,"node-b","boot-b")
	if err!=nil || !got.Passed || got.PreviousMaxEpoch!=13 || got.RecoveredOwnerEpoch!=14 {
		t.Fatalf("valid recovery rejected: %+v err=%v",got,err)
	}
}

func TestVerifySnapshotsRejectsEpochRollbackAndDescriptorMismatch(t *testing.T) {
	before:=snapshot{
		Schema:"blockmesh-owner-crash-evidence/v1",VolumeID:"vol-a",PoolUUID:"pool-a",VolumeIndex:0,
		DescriptorPages:[]pageEvidence{{Valid:true,Generation:4,OwnerEpoch:8}},
		SelectedOwner:&ownerIdentity{VolumeID:"vol-a",NodeID:"a",BootID:"a",Epoch:8},
	}
	after:=snapshot{
		Schema:"blockmesh-owner-crash-evidence/v1",VolumeID:"vol-a",PoolUUID:"pool-a",VolumeIndex:0,
		DescriptorPages:[]pageEvidence{{Valid:true,Generation:5,OwnerEpoch:8}},
		SelectedOwner:&ownerIdentity{VolumeID:"vol-a",NodeID:"b",BootID:"b",Epoch:8},
	}
	if _,err:=verifySnapshots(before,after,"b","b");err==nil {t.Fatal("Epoch rollback/equality must be rejected")}
	after.SelectedOwner.Epoch=9
	if _,err:=verifySnapshots(before,after,"b","b");err==nil {t.Fatal("descriptor/OwnerRecord mismatch must be rejected")}
}

func TestVerifySnapshotsRejectsIdentityAndPoolMismatch(t *testing.T) {
	before:=snapshot{Schema:"blockmesh-owner-crash-evidence/v1",VolumeID:"vol-a",PoolUUID:"pool-a"}
	after:=snapshot{Schema:"blockmesh-owner-crash-evidence/v1",VolumeID:"vol-a",PoolUUID:"pool-b"}
	if _,err:=verifySnapshots(before,after,"node","boot");err==nil {t.Fatal("different Pool UUID accepted")}
}

func TestVerifyDynamicSnapshotsRequiresMonotonicOwnerEpoch(t *testing.T){
	before:=dynamicSnapshot{Schema:"blockmesh-owner-crash-evidence/v2",VolumeID:"vol-d",PoolUUID:"pool-d",VolumeIndex:2,
		ViewGeneration:7,SuperblockGeneration:11,
		DescriptorPages:[]pageEvidence{{Valid:true,Generation:9,OwnerEpoch:20}},
		OwnerRecordPages:[]pageEvidence{{Valid:true,Generation:8,OwnerEpoch:21}},
		SelectedOwner:&ownerIdentity{VolumeID:"vol-d",NodeID:"node-a",BootID:"boot-a",Epoch:21}}
	after:=dynamicSnapshot{Schema:"blockmesh-owner-crash-evidence/v2",VolumeID:"vol-d",PoolUUID:"pool-d",VolumeIndex:2,
		ViewGeneration:8,SuperblockGeneration:12,
		DescriptorPages:[]pageEvidence{{Valid:true,Generation:10,OwnerEpoch:22}},
		OwnerRecordPages:[]pageEvidence{{Valid:true,Generation:9,OwnerEpoch:22}},
		SelectedOwner:&ownerIdentity{VolumeID:"vol-d",NodeID:"node-b",BootID:"boot-b",Epoch:22}}
	got,err:=verifyDynamicSnapshots(before,after,"node-b","boot-b")
	if err!=nil||!got.Passed||got.PreviousMaxEpoch!=21||got.RecoveredOwnerEpoch!=22{t.Fatalf("valid BMPOOL03 failover rejected: %+v err=%v",got,err)}
}

func TestVerifyDynamicSnapshotsRejectsRollbackAndIdentityMismatch(t *testing.T){
	before:=dynamicSnapshot{VolumeID:"vol-d",PoolUUID:"pool-d",VolumeIndex:0,ViewGeneration:4,SuperblockGeneration:5,
		DescriptorPages:[]pageEvidence{{Valid:true,Generation:6,OwnerEpoch:8}},
		SelectedOwner:&ownerIdentity{VolumeID:"vol-d",NodeID:"node-a",BootID:"boot-a",Epoch:8}}
	after:=dynamicSnapshot{VolumeID:"vol-d",PoolUUID:"pool-d",VolumeIndex:0,ViewGeneration:4,SuperblockGeneration:5,
		DescriptorPages:[]pageEvidence{{Valid:true,Generation:7,OwnerEpoch:8}},
		SelectedOwner:&ownerIdentity{VolumeID:"vol-d",NodeID:"node-b",BootID:"boot-b",Epoch:8}}
	if _,err:=verifyDynamicSnapshots(before,after,"node-b","boot-b");err==nil{t.Fatal("OwnerEpoch rollback accepted")}
	after.SelectedOwner.Epoch=9
	if _,err:=verifyDynamicSnapshots(before,after,"node-b","boot-b");err==nil{t.Fatal("descriptor/OwnerRecord mismatch accepted")}
	after.SelectedOwner.Epoch=9;after.PoolUUID="different-pool"
	if _,err:=verifyDynamicSnapshots(before,after,"node-b","boot-b");err==nil{t.Fatal("different Pool UUID accepted")}
}
