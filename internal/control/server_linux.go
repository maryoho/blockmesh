//go:build linux

// Package control implements the local, versioned bm-metad lifecycle RPC.
// Storage semantics remain in handlers; the transport fails closed.
package control

import (
 "bufio"
 "context"
 "encoding/json"
 "errors"
 "fmt"
 "io"
 "net"
 "os"
 "path/filepath"
 "regexp"
 "sync"
 "syscall"
 "time"
)

const ( ProtocolVersion=1; DefaultMaxMessageBytes=1<<20; DefaultMaxConnections=64 )
var actionPattern=regexp.MustCompile("^[a-z][a-z0-9]{1,31}$")

type Request struct { Version int `json:"version"`; ID string `json:"id"`; Action string `json:"action"`; Args json.RawMessage `json:"args"` }
type Response struct { Version int `json:"version"`; ID string `json:"id"`; OK bool `json:"ok"`; Error string `json:"error,omitempty"`; Result json.RawMessage `json:"result,omitempty"` }
type Handler interface { Handle(context.Context,string,json.RawMessage)(any,error) }
type HandlerFunc func(context.Context,string,json.RawMessage)(any,error)
func(f HandlerFunc)Handle(c context.Context,a string,b json.RawMessage)(any,error){return f(c,a,b)}
type Config struct { Path string; SocketMode os.FileMode; MaxMessageBytes int; MaxConnections int; RequestTimeout time.Duration; Handler Handler }
type Server struct { cfg Config; listener *net.UnixListener; mu sync.Mutex; conns map[*net.UnixConn]struct{}; sem chan struct{}; closeOnce sync.Once }

func New(cfg Config)(*Server,error){
 if cfg.Path==""||cfg.Handler==nil{return nil,errors.New("control socket path and lifecycle handler are required")}
 if cfg.SocketMode==0{cfg.SocketMode=0660}
 if cfg.SocketMode&^os.ModePerm!=0{return nil,errors.New("invalid control socket mode")}
 if cfg.MaxMessageBytes==0{cfg.MaxMessageBytes=DefaultMaxMessageBytes}
 if cfg.MaxMessageBytes<256||cfg.MaxMessageBytes>16<<20{return nil,errors.New("control message limit must be between 256 bytes and 16 MiB")}
 if cfg.MaxConnections==0{cfg.MaxConnections=DefaultMaxConnections}
 if cfg.MaxConnections<1||cfg.MaxConnections>4096{return nil,errors.New("invalid control connection limit")}
 if cfg.RequestTimeout==0{cfg.RequestTimeout=2*time.Minute}
 if cfg.RequestTimeout<time.Second||cfg.RequestTimeout>10*time.Minute{return nil,errors.New("control request timeout must be between 1 second and 10 minutes")}
 return &Server{cfg:cfg,conns:make(map[*net.UnixConn]struct{}),sem:make(chan struct{},cfg.MaxConnections)},nil
}

// Listen refuses to unlink a non-socket or a live socket.
func(s *Server)Listen()error{
 if s==nil{return errors.New("nil control server")}
 if err:=os.MkdirAll(filepath.Dir(s.cfg.Path),0750);err!=nil{return fmt.Errorf("create control socket directory: %w",err)}
 if info,err:=os.Lstat(s.cfg.Path);err==nil{
  if info.Mode()&os.ModeSocket==0{return fmt.Errorf("control socket path exists and is not a socket: %s",s.cfg.Path)}
  conn,dialErr:=net.DialTimeout("unix",s.cfg.Path,250*time.Millisecond)
  if dialErr==nil{_=conn.Close();return fmt.Errorf("control socket is already active: %s",s.cfg.Path)}
  if err=os.Remove(s.cfg.Path);err!=nil{return fmt.Errorf("remove stale control socket: %w",err)}
 }else if !errors.Is(err,os.ErrNotExist){return err}
 ln,err:=net.ListenUnix("unix",&net.UnixAddr{Name:s.cfg.Path,Net:"unix"});if err!=nil{return fmt.Errorf("listen on control socket: %w",err)}
 ln.SetUnlinkOnClose(false)
 if err=os.Chmod(s.cfg.Path,s.cfg.SocketMode);err!=nil{_=ln.Close();_=os.Remove(s.cfg.Path);return fmt.Errorf("set control socket permissions: %w",err)}
 s.listener=ln;return nil
}

func(s *Server)Serve(ctx context.Context)error{
 if ctx==nil{return errors.New("control server context is required")}
 if s.listener==nil{if err:=s.Listen();err!=nil{return err}}
 go func(){<-ctx.Done();_=s.Close()}()
 for{
  conn,err:=s.listener.AcceptUnix()
  if err!=nil{if ctx.Err()!=nil||errors.Is(err,net.ErrClosed){return nil};if errors.Is(err,syscall.EINTR){continue};return fmt.Errorf("accept control connection: %w",err)}
  select{case s.sem<-struct{}{}:default:
   _=conn.SetWriteDeadline(time.Now().Add(time.Second));_=json.NewEncoder(conn).Encode(Response{Version:ProtocolVersion,OK:false,Error:"control server is busy"});_=conn.Close();continue}
  s.mu.Lock();s.conns[conn]=struct{}{};s.mu.Unlock()
  go s.serveConn(ctx,conn)
 }
}
func(s *Server)serveConn(parent context.Context,conn *net.UnixConn){
 defer func(){s.mu.Lock();delete(s.conns,conn);s.mu.Unlock();<-s.sem;_=conn.Close()}()
 _=conn.SetDeadline(time.Now().Add(s.cfg.RequestTimeout))
 reader:=bufio.NewReader(io.LimitReader(conn,int64(s.cfg.MaxMessageBytes)+1))
 line,err:=reader.ReadBytes('\n')
 if err!=nil&&!errors.Is(err,io.EOF){s.writeError(conn,"",fmt.Errorf("read request: %w",err));return}
 if len(line)==0{s.writeError(conn,"",errors.New("empty request"));return}
 if len(line)>s.cfg.MaxMessageBytes{s.writeError(conn,"",errors.New("request exceeds maximum message size"));return}
 var req Request
 dec:=json.NewDecoder(newSliceReader(line));dec.DisallowUnknownFields()
 if err=dec.Decode(&req);err!=nil{s.writeError(conn,"",fmt.Errorf("decode request: %w",err));return}
 var trailing any
 if err=dec.Decode(&trailing);err!=io.EOF{s.writeError(conn,req.ID,errors.New("request contains trailing JSON"));return}
 if req.Version!=ProtocolVersion{s.writeError(conn,req.ID,fmt.Errorf("unsupported protocol version %d",req.Version));return}
 if len(req.ID)==0||len(req.ID)>128{s.writeError(conn,req.ID,errors.New("request ID must contain 1-128 bytes"));return}
 if !actionPattern.MatchString(req.Action){s.writeError(conn,req.ID,errors.New("invalid lifecycle action"));return}
 if len(req.Args)==0||len(req.Args)>s.cfg.MaxMessageBytes||!json.Valid(req.Args){s.writeError(conn,req.ID,errors.New("args must be valid JSON"));return}
 var object map[string]json.RawMessage
 if err=json.Unmarshal(req.Args,&object);err!=nil||object==nil{s.writeError(conn,req.ID,errors.New("args must be a JSON object"));return}
 ctx,cancel:=context.WithTimeout(parent,s.cfg.RequestTimeout);defer cancel()
 result,handleErr:=s.cfg.Handler.Handle(ctx,req.Action,req.Args)
 if handleErr!=nil{s.writeError(conn,req.ID,handleErr);return}
 raw,err:=json.Marshal(result);if err!=nil{s.writeError(conn,req.ID,fmt.Errorf("encode lifecycle result: %w",err));return}
 _=conn.SetWriteDeadline(time.Now().Add(5*time.Second))
 _=json.NewEncoder(conn).Encode(Response{Version:ProtocolVersion,ID:req.ID,OK:true,Result:raw})
}
func(s *Server)writeError(conn *net.UnixConn,id string,cause error){
 if cause==nil{cause=errors.New("unknown lifecycle error")}
 _=conn.SetWriteDeadline(time.Now().Add(5*time.Second))
 _=json.NewEncoder(conn).Encode(Response{Version:ProtocolVersion,ID:id,OK:false,Error:cause.Error()})
}
func(s *Server)Close()error{
 if s==nil{return nil};var closeErr error
 s.closeOnce.Do(func(){
  if s.listener!=nil{closeErr=s.listener.Close()}
  s.mu.Lock();for c:=range s.conns{_=c.Close()};s.mu.Unlock()
  if info,err:=os.Lstat(s.cfg.Path);err==nil&&info.Mode()&os.ModeSocket!=0{if e:=os.Remove(s.cfg.Path);e!=nil&&!errors.Is(e,os.ErrNotExist){closeErr=errors.Join(closeErr,e)}}
 })
 return closeErr
}
type sliceReader struct{b []byte;off int}
func newSliceReader(b []byte)*sliceReader{return &sliceReader{b:b}}
func(r *sliceReader)Read(p []byte)(int,error){if r.off>=len(r.b){return 0,io.EOF};n:=copy(p,r.b[r.off:]);r.off+=n;return n,nil}
