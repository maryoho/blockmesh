package lock

import (
 "context"
 "errors"
 "fmt"
 "os/exec"
 "strconv"
 "strings"
 "time"
)

// SanlockRIndexResolver allocates and discovers resource lease offsets through
// sanlock native RINDEX operations. RINDEX create/lookup is serialized by
// sanlock internal paxos lease; no application hash slots or per-Volume offset
// configuration are needed.
type SanlockRIndexResolver struct {
 Binary string
 Lockspace string
 Device string
 Offset uint64
 Timeout time.Duration
 Runner SanlockCommandRunner
}

type SanlockCommandRunner interface { Run(context.Context,string,...string)([]byte,error) }
type sanlockExecRunner struct{}
func(sanlockExecRunner)Run(ctx context.Context,name string,args ...string)([]byte,error){
 cmd:=exec.CommandContext(ctx,name,args...);return cmd.CombinedOutput()
}

func(r *SanlockRIndexResolver) Resolve(name string)(uint64,error){
 if r==nil||strings.TrimSpace(r.Lockspace)==""||strings.TrimSpace(r.Device)==""||r.Offset==0||r.Offset%512!=0{return 0,errors.New("sanlock RINDEX resolver requires lockspace, device and a nonzero 512-byte-aligned RINDEX offset")}
 if name==""||len(name)>48||strings.ContainsAny(name,":\\r\\n"){return 0,fmt.Errorf("invalid sanlock resource name %q: maximum 48 bytes and no colon/newline",name)}
 binary:=r.Binary;if binary==""{binary="sanlock"}
 runner:=r.Runner;if runner==nil{runner=sanlockExecRunner{}}
 timeout:=r.Timeout;if timeout<=0{timeout=30*time.Second}
 ctx,cancel:=context.WithTimeout(context.Background(),timeout);defer cancel()
 rindex:=fmt.Sprintf("%s:%s:%d",r.Lockspace,r.Device,r.Offset)
 lookup:=func()(uint64,error){
  out,err:=runner.Run(ctx,binary,"client","lookup","-x",rindex,"-e",name)
  if err!=nil{return 0,fmt.Errorf("sanlock RINDEX lookup for %s: %w: %s",name,err,strings.TrimSpace(string(out)))}
  off,err:=parseSanlockRIndexOffset(out);if err!=nil{return 0,fmt.Errorf("parse sanlock RINDEX lookup for %s: %w: %s",name,err,strings.TrimSpace(string(out)))}
  return off,nil
 }
 if off,err:=lookup();err==nil{return off,nil}
 // create is idempotent at the application boundary: if another node won
 // the RINDEX create race or our response was lost, a second lookup returns
 // the single persistent offset chosen by sanlock.
 createOut,createErr:=runner.Run(ctx,binary,"client","create","-x",rindex,"-e",name)
 if createErr==nil { if off,parseErr:=parseSanlockRIndexOffset(createOut);parseErr==nil{return off,nil} }
 if off,lookupErr:=lookup();lookupErr==nil{return off,nil}
 return 0,errors.Join(fmt.Errorf("sanlock RINDEX create for %s failed: %w: %s",name,createErr,strings.TrimSpace(string(createOut))),lookupErr)
}

func parseSanlockRIndexOffset(output []byte)(uint64,error){
 fields:=strings.Fields(string(output))
 for i:=0;i+1<len(fields);i++{if fields[i]=="offset"{off,err:=strconv.ParseUint(fields[i+1],10,64);if err==nil&&off!=0{return off,nil};if err!=nil{return 0,err}}}
 return 0,errors.New("sanlock output does not contain a nonzero offset")
}
