package wal

import (
	"errors"
	"fmt"
)

var ErrDynamicOwnerLeaseLost = errors.New("BMPOOL03 Owner lease is not valid")
var ErrDynamicOwnerEpochMismatch = errors.New("BMPOOL03 OwnerEpoch does not match the active Owner lease")

type DynamicOwnerLease interface {
	Resource() string
	VolumeID() string
	Epoch() uint64
	Valid() bool
}

// GuardDynamicSharedCommitter binds every WAL open/recovery/commit/checkpoint
// and JournalFull retry to one live Volume Owner identity. readEpoch should
// read the authoritative redundant Volume Descriptor pair; nil is permitted
// only during the initial descriptor-creation transaction.
func GuardDynamicSharedCommitter(c *DynamicSharedCommitter,lease DynamicOwnerLease,volumeID string,readEpoch func()(uint64,error)) error {
	if c==nil||lease==nil||volumeID==""||lease.Resource()==""||lease.VolumeID()!=volumeID||lease.Epoch()==0 {
		return errors.New("Dynamic WAL committer and matching Volume Owner lease are required")
	}
	expected:=lease.Epoch()
	if proof,ok:=lease.(interface{ReservationVerified()bool;PreviousOwnerFenced()bool;OldOwnerIODrained()bool});ok {
		if !proof.ReservationVerified()||!proof.PreviousOwnerFenced()||!proof.OldOwnerIODrained(){
			return errors.New("Owner lease has not completed SCSI PR reservation, previous-owner fencing and I/O drain")
		}
	}
	guard:=func()error{
		if !lease.Valid(){return ErrDynamicOwnerLeaseLost}
		if lease.VolumeID()!=volumeID||lease.Epoch()!=expected{return ErrDynamicOwnerEpochMismatch}
		if readEpoch!=nil{
			observed,err:=readEpoch();if err!=nil{return fmt.Errorf("read authoritative BMPOOL03 OwnerEpoch: %w",err)}
			if observed!=expected{return fmt.Errorf("%w: descriptor=%d lease=%d",ErrDynamicOwnerEpochMismatch,observed,expected)}
		}
		return nil
	}
	c.SetGuard(guard)
	return guard()
}
