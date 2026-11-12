//go:build linux

package device

import (
 "errors"
 "fmt"
 "io"
 "os"
 "sync"
 "syscall"
 "unsafe"
)

const ( blkGetSize64=0x80081272; blkSSZGet=0x1268 )
var ( ErrNotBlockDevice=errors.New("path is not a Linux block device"); ErrInvalidSector=errors.New("invalid logical sector size") )

type BlockDevice struct { mu sync.Mutex; file *os.File; path string; size int64; sector int; readOnly bool; closed bool }

func OpenBlockDevice(path string,writable,exclusive bool)(*BlockDevice,error){
 if path==""{return nil,errors.New("block device path is required")}
 flags:=syscall.O_CLOEXEC;if writable{flags|=syscall.O_RDWR}else{flags|=syscall.O_RDONLY};if exclusive{flags|=syscall.O_EXCL}
 fd,err:=syscall.Open(path,flags,0);if err!=nil{return nil,fmt.Errorf("open block device %q: %w",path,err)}
 f:=os.NewFile(uintptr(fd),path);fail:=func(err error)(*BlockDevice,error){_=f.Close();return nil,err}
 var st syscall.Stat_t;if err=syscall.Fstat(fd,&st);err!=nil{return fail(fmt.Errorf("fstat block device %q: %w",path,err))}
 if st.Mode&syscall.S_IFMT!=syscall.S_IFBLK{return fail(fmt.Errorf("%w: %s",ErrNotBlockDevice,path))}
 d:=&BlockDevice{file:f,path:path,readOnly:!writable}
 if err=d.refreshLocked();err!=nil{return fail(err)}
 return d,nil
}
func ioctl(fd int,request uintptr,arg uintptr)error{_,_,errno:=syscall.Syscall(syscall.SYS_IOCTL,uintptr(fd),request,arg);if errno!=0{return errno};return nil}
func(d *BlockDevice)refreshLocked()error{
 var size uint64;if err:=ioctl(int(d.file.Fd()),blkGetSize64,uintptr(unsafe.Pointer(&size)));err!=nil{return fmt.Errorf("BLKGETSIZE64 %q: %w",d.path,err)}
 var sector int32;if err:=ioctl(int(d.file.Fd()),blkSSZGet,uintptr(unsafe.Pointer(&sector)));err!=nil{return fmt.Errorf("BLKSSZGET %q: %w",d.path,err)}
 if sector<512||sector>65536||(sector&(sector-1))!=0{return fmt.Errorf("%w: %d",ErrInvalidSector,sector)}
 if size==0||size>uint64(^uint64(0)>>1)||size%uint64(sector)!=0{return fmt.Errorf("invalid block device capacity/sector geometry: size=%d sector=%d",size,sector)}
 d.size=int64(size);d.sector=int(sector);return nil
}
// Rescan refreshes the kernel-reported capacity after the administrator has
// rescanned the SCSI/multipath device. It never changes on-disk Pool metadata.
func(d *BlockDevice)Rescan()(uint64,error){
 if d==nil{return 0,ErrClosed};d.mu.Lock();defer d.mu.Unlock()
 if d.closed{return 0,ErrClosed};old:=d.size
 if err:=d.refreshLocked();err!=nil{return 0,err}
 if d.size<old{newSize:=d.size;d.size=old;return uint64(old),fmt.Errorf("device shrink detected: old=%d new=%d; refusing to accept shrink",old,newSize)}
 return uint64(d.size),nil
}
func(d *BlockDevice)Path()string{return d.path}
func(d *BlockDevice)LogicalSectorSize()int{return d.sector}
func(d *BlockDevice)ReadOnly()bool{return d.readOnly}
func(d *BlockDevice)ReadAt(p []byte,off int64)(int,error){
 d.mu.Lock();defer d.mu.Unlock();if d.closed{return 0,ErrClosed};if off<0||off>d.size||int64(len(p))>d.size-off{return 0,io.ErrUnexpectedEOF}
 n,err:=d.file.ReadAt(p,off);if err==io.EOF&&n==len(p){err=nil};return n,err
}
func(d *BlockDevice)WriteAt(p []byte,off int64)(int,error){
 d.mu.Lock();defer d.mu.Unlock();if d.closed{return 0,ErrClosed};if d.readOnly{return 0,ErrReadOnly};if off<0||off>d.size||int64(len(p))>d.size-off{return 0,io.ErrShortWrite};return d.file.WriteAt(p,off)
}
func(d *BlockDevice)Sync()error{d.mu.Lock();defer d.mu.Unlock();if d.closed{return ErrClosed};if d.readOnly{return nil};return d.file.Sync()}
func(d *BlockDevice)Size()int64{if d==nil{return 0};d.mu.Lock();defer d.mu.Unlock();if d.closed{return 0};return d.size}
func(d *BlockDevice)Close()error{d.mu.Lock();defer d.mu.Unlock();if d.closed{return nil};d.closed=true;return d.file.Close()}
var _ Device=(*BlockDevice)(nil)
