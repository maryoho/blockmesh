package metaservice

import (
 "context"
 "errors"
 "blockmesh/internal/uapi"
)

var (
 ErrUnmapped=errors.New("logical block is unmapped")
 ErrStaleEpoch=errors.New("request epoch is stale")
 ErrReadOnly=errors.New("volume is read-only")
 ErrNoSpace=errors.New("metadata allocator has no space")
 ErrRetry=errors.New("request should be retried")
 ErrFenced=errors.New("request rejected by fencing state")
 ErrInvalid=errors.New("request is invalid")
)

// Processor must enforce request identity and epoch admission. AllocateZeroPublish
// must zero data before publishing the mapping; Flush must durably flush metadata.
type Processor interface { Process(context.Context,uapi.Request)(Result,error) }
type Result struct { Mapping *uapi.Mapping }
type ProcessorFunc func(context.Context,uapi.Request)(Result,error)
func(f ProcessorFunc) Process(c context.Context,r uapi.Request)(Result,error){return f(c,r)}
func resultCode(e error) uapi.ResultCode {
 switch {
 case e==nil:return uapi.CodeOK
 case errors.Is(e,ErrUnmapped):return uapi.CodeUnmapped
 case errors.Is(e,ErrStaleEpoch):return uapi.CodeStaleEpoch
 case errors.Is(e,ErrReadOnly):return uapi.CodeReadOnly
 case errors.Is(e,ErrNoSpace):return uapi.CodeNoSpace
 case errors.Is(e,ErrRetry):return uapi.CodeRetry
 case errors.Is(e,ErrFenced):return uapi.CodeFenced
 case errors.Is(e,ErrInvalid):return uapi.CodeInvalid
 default:return uapi.CodeInternal
 }
}
