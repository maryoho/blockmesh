package control

import (
 "bytes"
 "context"
 "encoding/hex"
 "encoding/json"
 "errors"
 "fmt"
 "io"
 "strings"
)

type PoolCreateArgs struct { Name string `json:"name"`; Device string `json:"device"`; Confirmed bool `json:"confirmed"` }
type PoolNameArgs struct { Name string `json:"name"` }
type VolumeCreateArgs struct { Pool string `json:"pool"`; Name string `json:"name"`; SizeBytes uint64 `json:"size_bytes"`; CreateToken [16]byte }
type VolumeIDArgs struct { UUID string `json:"uuid"` }
type VolumeActivateArgs struct { UUID string `json:"uuid"`; Mode string `json:"mode"` }

// LifecycleBackend is the privileged storage implementation. A production
// implementation must provide SAN fencing, WAL durability and DM drain; the
// router never substitutes local locks or no-op operations.
type LifecycleBackend interface {
 PoolCreate(context.Context,PoolCreateArgs)(any,error)
 PoolList(context.Context)(any,error)
 PoolInfo(context.Context,string)(any,error)
 PoolDestroy(context.Context,string)(any,error)
 VolumeCreate(context.Context,VolumeCreateArgs)(any,error)
 VolumeList(context.Context)(any,error)
 VolumeInfo(context.Context,string)(any,error)
 VolumeDelete(context.Context,string)(any,error)
 VolumeActivate(context.Context,string,string)(any,error)
 VolumeDeactivate(context.Context,string)(any,error)
}

type LifecycleHandler struct { Backend LifecycleBackend }
func(h LifecycleHandler)Handle(ctx context.Context,action string,raw json.RawMessage)(any,error){
 if h.Backend==nil{return nil,errors.New("lifecycle backend is not configured")}
 decode:=func(dst any)error{
  dec:=json.NewDecoder(bytes.NewReader(raw));dec.DisallowUnknownFields()
  if err:=dec.Decode(dst);err!=nil{return fmt.Errorf("decode %s arguments: %w",action,err)}
  var extra any;if err:=dec.Decode(&extra);err!=io.EOF{return fmt.Errorf("%s arguments contain trailing JSON",action)}
  return nil
 }
 switch action {
 case "poolcreate":
  var a PoolCreateArgs;if err:=decode(&a);err!=nil{return nil,err}
  if strings.TrimSpace(a.Name)==""||strings.TrimSpace(a.Device)==""{return nil,errors.New("pool create requires name and device")}
  if !a.Confirmed { return nil, errors.New("pool create requires explicit destructive-operation confirmation") }
  return h.Backend.PoolCreate(ctx,a)
 case "poollist":
  if err:=decode(&struct{}{});err!=nil{return nil,err};return h.Backend.PoolList(ctx)
 case "poolinfo","pooldestroy":
  var a PoolNameArgs;if err:=decode(&a);err!=nil{return nil,err}
  if strings.TrimSpace(a.Name)==""{return nil,errors.New("Pool name is required")}
  if action=="poolinfo"{return h.Backend.PoolInfo(ctx,a.Name)}
  return h.Backend.PoolDestroy(ctx,a.Name)
 case "volumecreate":
  var wire struct { Pool string `json:"pool"`; Name string `json:"name"`; SizeBytes uint64 `json:"size_bytes"`; CreateToken string `json:"create_token"` }
  if err:=decode(&wire);err!=nil{return nil,err}
  if strings.TrimSpace(wire.Pool)==""||strings.TrimSpace(wire.Name)==""||wire.SizeBytes==0{return nil,errors.New("volume create requires Pool, name and positive size_bytes")}
  token,err:=hex.DecodeString(wire.CreateToken);if err!=nil||len(token)!=16{return nil,errors.New("volume create requires a 16-byte hexadecimal create_token")}
  var a VolumeCreateArgs;a.Pool=wire.Pool;a.Name=wire.Name;a.SizeBytes=wire.SizeBytes;copy(a.CreateToken[:],token)
  return h.Backend.VolumeCreate(ctx,a)
 case "volumelist":
  if err:=decode(&struct{}{});err!=nil{return nil,err};return h.Backend.VolumeList(ctx)
 case "volumeinfo","volumedelete","volumedeactivate":
  var a VolumeIDArgs;if err:=decode(&a);err!=nil{return nil,err}
  if strings.TrimSpace(a.UUID)==""{return nil,errors.New("Volume UUID is required")}
  switch action {case "volumeinfo":return h.Backend.VolumeInfo(ctx,a.UUID);case "volumedelete":return h.Backend.VolumeDelete(ctx,a.UUID);default:return h.Backend.VolumeDeactivate(ctx,a.UUID)}
 case "volumeactivate":
  var a VolumeActivateArgs;if err:=decode(&a);err!=nil{return nil,err}
  if strings.TrimSpace(a.UUID)==""||(a.Mode!="rw"&&a.Mode!="ro"){return nil,errors.New("volume activate requires UUID and mode rw|ro")}
  return h.Backend.VolumeActivate(ctx,a.UUID,a.Mode)
 default:
  return nil,fmt.Errorf("unsupported lifecycle action %q",action)
 }
}
