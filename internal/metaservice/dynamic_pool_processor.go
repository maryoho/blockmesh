package metaservice

import (
	"context"
	"errors"
	"fmt"

	"blockmesh/internal/dynamicruntime"
	"blockmesh/internal/metastore"
	"blockmesh/internal/uapi"
)

// DynamicPoolRuntimeProcessor is the BMPOOL03 request path. It resolves every
// request through DynamicVolumeRuntimeManager, so RW OwnerEpoch and RO
// ReaderEpoch/RootGeneration checks use the same Runtime that owns the DM gate.
type DynamicPoolRuntimeProcessor struct {
	manager *dynamicruntime.DynamicVolumeRuntimeManager
}

func NewDynamicPoolRuntimeProcessor(manager *dynamicruntime.DynamicVolumeRuntimeManager) (*DynamicPoolRuntimeProcessor,error) {
	if manager==nil{return nil,errors.New("BMPOOL03 DynamicVolumeRuntimeManager is required")}
	return &DynamicPoolRuntimeProcessor{manager:manager},nil
}

func (p *DynamicPoolRuntimeProcessor) Process(ctx context.Context,req uapi.Request)(Result,error) {
	if p==nil||p.manager==nil{return Result{},ErrFenced}
	if ctx==nil{return Result{},errors.New("request context is required")}
	if err:=ctx.Err();err!=nil{return Result{},err}
	if err:=req.Validate();err!=nil{return Result{},fmt.Errorf("%w: %v",ErrInvalid,err)}
	volumeID,nodeID,bootID:=uuidText(req.VolumeID),uuidText(req.NodeID),uuidText(req.BootID)
	snapshot,err:=p.manager.ResolveRequest(ctx,volumeID,nodeID,bootID,req.OwnerEpoch,req.ReaderEpoch,req.RootGeneration)
	if err!=nil {
		switch {
		case errors.Is(err,metastore.ErrVolumeRuntimeStopping):
			return Result{},errors.Join(ErrRetry,err)
		case errors.Is(err,metastore.ErrRuntimeRequestFenced):
			return Result{},errors.Join(ErrFenced,err)
		case errors.Is(err,metastore.ErrRuntimeRequestStaleEpoch):
			return Result{},errors.Join(ErrStaleEpoch,err)
		case errors.Is(err,metastore.ErrRuntimeRequestInvalid):
			return Result{},errors.Join(ErrInvalid,err)
		default:
			return Result{},err
		}
	}
	if snapshot.Runtime==nil||snapshot.Runtime.Store==nil||snapshot.Runtime.Tree==nil {
		return Result{},ErrFenced
	}
	switch req.Operation {
	case uapi.OpLookupBatch:
		mappings,lookupErr:=lookupMappingVector(snapshot.Runtime.Tree,req.LogicalStart,req.BlockCount)
		if lookupErr!=nil{return Result{},lookupErr}
		return Result{Mappings:mappings,RootGeneration:snapshot.RootGeneration},nil
	case uapi.OpAllocateZeroPublish:
		return p.executeAllocateZeroPublish(ctx, req, snapshot)
	case uapi.OpDiscard:
		return p.executeDiscard(ctx, req, snapshot)
	case uapi.OpFlush:
		if snapshot.Spec.Mode!=metastore.VolumeRuntimeReadWrite||snapshot.OwnerEpoch!=req.OwnerEpoch {
			return Result{},ErrFenced
		}
		if snapshot.Runtime.Data==nil{return Result{},ErrFenced}
		if err=snapshot.Runtime.Data.Sync();err!=nil{return Result{},fmt.Errorf("flush BMPOOL03 DataView: %w",err)}
		if err=snapshot.Runtime.Store.Sync();err!=nil{return Result{},fmt.Errorf("flush BMPOOL03 MetadataView: %w",err)}
		return Result{RootGeneration:snapshot.RootGeneration},nil
	default:
		return Result{},ErrInvalid
	}
}

var _ Processor = (*DynamicPoolRuntimeProcessor)(nil)
