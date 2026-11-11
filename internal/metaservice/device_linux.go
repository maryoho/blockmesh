//go:build linux
package metaservice
import("context";"encoding/binary";"errors";"fmt";"syscall";"sync";"time";"unsafe";"blockmesh/internal/uapi")
const(ioctlGetABI uint32=0xc0404200;ioctlSetupQueue uint32=0xc0504201;ioctlNextQueue uint32=0xc0284204;ioctlKick uint32=uapi.IoctlKick;ioctlCancel uint32=uapi.IoctlCancel;pollIn int16=1;pollPri int16=2;pollFailure int16=0x008|0x010|0x020)
type pollFD struct{FD int32;Events int16;Revents int16}
type mappedQueue struct{generation uint64;mem *QueueMemory;regions [][]byte}
type Device struct{fd int;path string;processor Processor;requestTimeout time.Duration;queuesMu sync.RWMutex;queues map[uint32]*mappedQueue;closed bool}
func ioctl(fd int,cmd uint32,b []byte)error{if len(b)==0{return errors.New("empty ioctl payload")};_,_,e:=syscall.Syscall(syscall.SYS_IOCTL,uintptr(fd),uintptr(cmd),uintptr(unsafe.Pointer(&b[0])));if e!=0{return e};return nil}
func OpenDevice(path string,p Processor)(*Device,error){return OpenDeviceWithTimeout(path,p,DefaultRequestTimeout)}
func OpenDeviceWithTimeout(path string,p Processor,requestTimeout time.Duration)(*Device,error){if path==""||p==nil{return nil,errors.New("device path and processor are required")};if requestTimeout<0{return nil,errors.New("request timeout cannot be negative")};fd,e:=syscall.Open(path,syscall.O_RDWR|syscall.O_CLOEXEC,0);if e!=nil{return nil,e};fail:=func(err error)(*Device,error){_=syscall.Close(fd);return nil,err};b:=make([]byte,uapi.ABIInfoSize);binary.LittleEndian.PutUint32(b[:4],uapi.ABIInfoSize);if e=ioctl(fd,ioctlGetABI,b);e!=nil{return fail(fmt.Errorf("GET_ABI: %w",e))};info,e:=uapi.UnmarshalABIInfo(b);if e!=nil{return fail(e)};need:=uapi.FeatureRequestRing|uapi.FeatureCompletionKick|uapi.FeatureRequestCancel;if info.Features&need!=need{return fail(fmt.Errorf("kernel lacks required feature bits %#x",need))};return &Device{fd:fd,path:path,processor:p,requestTimeout:requestTimeout,queues:make(map[uint32]*mappedQueue)},nil}
func(d *Device)CancelRequest(queueID uint32,requestID,generation uint64) bool {
 if d==nil { return false }
 d.queuesMu.RLock()
 defer d.queuesMu.RUnlock()
 if d.closed { return false }
 q:=d.queues[queueID]
 if q==nil || q.generation!=generation { return false }
 if !q.mem.Cancel(requestID,generation) { return false }
 return q.mem.notifyCancel(requestID,generation)==nil
}
func(d *Device)Reconcile()error{if d==nil{return errors.New("device closed")};return d.scan()}
func(d *Device)scan()error{d.queuesMu.Lock();defer d.queuesMu.Unlock();if d.closed{return errors.New("device closed")};seen:=make(map[uint32]struct{});for after:=uint32(0);;{q:=make([]byte,uapi.QueueQuerySize);binary.LittleEndian.PutUint32(q[:4],uapi.QueueQuerySize);binary.LittleEndian.PutUint32(q[4:8],after);e:=ioctl(d.fd,ioctlNextQueue,q);if errors.Is(e,syscall.ENOENT){break};if e!=nil{return fmt.Errorf("NEXT_QUEUE: %w",e)};query,e:=uapi.UnmarshalQueueQuery(q);if e!=nil{return fmt.Errorf("NEXT_QUEUE returned malformed queue: %w",e)};id:=query.QueueID;after=id;seen[id]=struct{}{};if current:=d.queues[id];current!=nil{if current.generation==query.QueueGeneration&&current.mem.Info.RingOrder==query.RingOrder{continue};d.unmapLocked(id)};if e=d.setup(id,query.RingOrder);e!=nil{if errors.Is(e,syscall.ENOENT)||errors.Is(e,syscall.ENOTCONN){delete(seen,id);continue};return fmt.Errorf("setup Queue %d generation %d: %w",id,query.QueueGeneration,e)}};for id:=range d.queues{if _,ok:=seen[id];!ok{d.unmap(id)}};return nil}
func(d *Device)setup(id,order uint32)error{b:=make([]byte,uapi.SetupSize);binary.LittleEndian.PutUint32(b[:4],uapi.SetupSize);binary.LittleEndian.PutUint32(b[4:8],uint32(uapi.Version));binary.LittleEndian.PutUint32(b[8:12],order);binary.LittleEndian.PutUint32(b[68:72],id);if e:=ioctl(d.fd,ioctlSetupQueue,b);e!=nil{return e};info,e:=uapi.UnmarshalSetupQueue(b);if e!=nil{return e};if info.QueueID!=id||info.RingOrder!=order||info.QueueGeneration==0{return errors.New("SETUP_QUEUE identity differs from discovery result")};l,e:=uapi.Layout(info.RingOrder);if e!=nil{return e};regions:=[][]byte{};mapOne:=func(off,n uint64)([]byte,error){if n==0||n>uint64(int(^uint(0)>>1)){return nil,errors.New("invalid mmap length")};x,er:=syscall.Mmap(d.fd,int64(off),int(n),syscall.PROT_READ|syscall.PROT_WRITE,syscall.MAP_SHARED);if er==nil{regions=append(regions,x)};return x,er};cleanup:=func(){for i:=len(regions)-1;i>=0;i--{_=syscall.Munmap(regions[i])}};rc,er:=mapOne(info.RequestControlOffset,4096);if er!=nil{cleanup();return er};rr,er:=mapOne(info.RequestRingOffset,uint64(l.Entries)*uapi.RequestSize);if er!=nil{cleanup();return er};cc,er:=mapOne(info.CompletionControlOffset,4096);if er!=nil{cleanup();return er};cr,er:=mapOne(info.CompletionRingOffset,uint64(l.Entries)*uapi.CompletionSize);if er!=nil{cleanup();return er};pl,er:=mapOne(info.PayloadOffset,l.PayloadBytes);if er!=nil{cleanup();return er};d.queues[id]=&mappedQueue{generation:info.QueueGeneration,mem:&QueueMemory{Info:info,RequestControl:rc,RequestRing:rr,CompletionControl:cc,CompletionRing:cr,Payload:pl,RequestTimeout:d.requestTimeout,CancelNotify:func(requestID,generation uint64)error{return d.cancelKernelRequest(id,requestID,generation)}},regions:regions};return nil}
func(d *Device)cancelKernelRequest(queueID uint32,requestID,generation uint64)error{
 raw,err:=(uapi.CancelRequest{QueueID:queueID,RequestID:requestID,QueueGeneration:generation}).MarshalBinary()
 if err!=nil{return err}
 err=ioctl(d.fd,ioctlCancel,raw)
 if errors.Is(err,syscall.EALREADY){return nil}
 return err
}
func(d *Device)kick(id uint32)error{b:=make([]byte,4);binary.LittleEndian.PutUint32(b,id);return ioctl(d.fd,ioctlKick,b)}
func(d *Device)Run(ctx context.Context)error{
 d.queuesMu.RLock();closed:=d.closed;d.queuesMu.RUnlock()
 if closed{return errors.New("device closed")}
 if e:=d.scan();e!=nil{return e}
 for{
  if e:=ctx.Err();e!=nil{return e}
  p:=pollFD{FD:int32(d.fd),Events:pollIn|pollPri}
  _,_,errno:=syscall.Syscall(syscall.SYS_POLL,uintptr(unsafe.Pointer(&p)),1,250)
  if errno!=0&&errno!=syscall.EINTR{return fmt.Errorf("poll %s: %w",d.path,errno)}
  if p.Revents&pollFailure!=0{return fmt.Errorf("device poll failure: %#x",p.Revents)}
  if p.Revents&pollPri!=0{if e:=d.scan();e!=nil{return e}}
  if p.Revents&pollIn==0{continue}
  d.queuesMu.RLock();ids:=make([]uint32,0,len(d.queues));for id:=range d.queues{ids=append(ids,id)};d.queuesMu.RUnlock()
  for _,id:=range ids{
   d.queuesMu.RLock();m:=d.queues[id]
   if m==nil{d.queuesMu.RUnlock();continue}
   stale:=false;gone:=false
   for{
    n,w,e:=m.mem.Drain(ctx,d.processor,1<<m.mem.Info.RingOrder)
    if errors.Is(e,ErrQueueGeneration){stale=true;break}
    if e!=nil{d.queuesMu.RUnlock();return fmt.Errorf("Queue %d generation %d: %w",id,m.generation,e)}
    if w>0{
     if e=d.kick(id);e!=nil{
      if errors.Is(e,syscall.ENOENT)||errors.Is(e,syscall.ENOTCONN){gone=true;break}
      d.queuesMu.RUnlock();return fmt.Errorf("KICK Queue %d generation %d: %w",id,m.generation,e)
     }
    }
    if n==0{break}
   }
   d.queuesMu.RUnlock()
   if stale{if e:=d.scan();e!=nil{return e}}
   if gone{d.unmap(id)}
  }
 }
}
func(d *Device)unmap(id uint32){d.queuesMu.Lock();defer d.queuesMu.Unlock();d.unmapLocked(id)}
func(d *Device)unmapLocked(id uint32){m:=d.queues[id];if m==nil{return};for i:=len(m.regions)-1;i>=0;i--{_=syscall.Munmap(m.regions[i])};delete(d.queues,id)}
func(d *Device)Close()error{if d==nil{return nil};d.queuesMu.Lock();defer d.queuesMu.Unlock();if d.closed{return nil};d.closed=true;for id:=range d.queues{d.unmapLocked(id)};if e:=syscall.Close(d.fd);e!=nil{return fmt.Errorf("close %s: %w",d.path,e)};return nil}