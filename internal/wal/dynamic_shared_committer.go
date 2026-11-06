package wal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"blockmesh/internal/format"
	"blockmesh/internal/lock"
	"blockmesh/internal/metastore"
)

type DynamicMetadataDevice interface {
	metastore.Device
	Size() int64
}

// DynamicSharedCommitter reopens the dynamic AG journal while holding the
// distributed SAN WAL resource, so append offsets and checkpoint generations
// are refreshed across all writer processes.
type DynamicSharedCommitter struct {
	Device DynamicMetadataDevice
	Superblock format.DynamicSuperblock
	AG uint32
	Locker lock.DistributedLocker
	Resource string
	Timeout time.Duration
}

func NewDynamicSharedCommitter(dev DynamicMetadataDevice,sb format.DynamicSuperblock,ag uint32,locker lock.DistributedLocker,resource string)(*DynamicSharedCommitter,error){
	if dev==nil||locker==nil||!locker.Distributed()||resource==""{return nil,errors.New("MetadataView, distributed locker and WAL resource are required")}
	if err:=sb.Validate();err!=nil{return nil,err}
	if ag>=sb.Geometry.AGCount{return nil,fmt.Errorf("AG %d out of range",ag)}
	return &DynamicSharedCommitter{Device:dev,Superblock:sb,AG:ag,Locker:locker,Resource:resource},nil
}
func(c *DynamicSharedCommitter)withJournal(fn func(*DynamicJournal)error)error{
	if c==nil||c.Device==nil||c.Locker==nil||!c.Locker.Distributed()||c.Resource==""{return errors.New("dynamic shared WAL committer is not configured")}
	timeout:=c.Timeout;if timeout<=0{timeout=30*time.Second}
	ctx,cancel:=context.WithTimeout(context.Background(),timeout);defer cancel()
	release,err:=c.Locker.Lock(ctx,c.Resource);if err!=nil{return fmt.Errorf("acquire BMPOOL03 AG%d WAL lease: %w",c.AG,err)}
	j,openErr:=OpenDynamicJournal(c.Device,c.Superblock,c.AG);if openErr!=nil{return errors.Join(openErr,release())}
	opErr:=fn(j);releaseErr:=release()
	if opErr!=nil&&releaseErr!=nil{return errors.Join(opErr,releaseErr)}
	if opErr!=nil{return opErr};return releaseErr
}
func(c *DynamicSharedCommitter)CommitImages(images []metastore.PageImage)error{
	return c.withJournal(func(j *DynamicJournal)error{
		err:=j.CommitImages(images)
		if !errors.Is(err,ErrJournalFull){return err}
		// ErrJournalFull is returned only when append could not write a
		// complete record. The durable tail may contain an unmatched Intent;
		// Checkpoint safely retires it because its Commit record is absent.
		if err=j.Checkpoint();err!=nil{return fmt.Errorf("automatic BMPOOL03 WAL checkpoint after JournalFull: %w",err)}
		if err=j.CommitImages(images);err!=nil{return fmt.Errorf("retry BMPOOL03 transaction after checkpoint: %w",err)}
		return nil
	})
}
func(c *DynamicSharedCommitter)Recover()error{
	return c.withJournal(func(j *DynamicJournal)error{_,err:=j.Recover();return err})
}
func(c *DynamicSharedCommitter)Checkpoint()error{
	return c.withJournal(func(j *DynamicJournal)error{return j.Checkpoint()})
}
func(*DynamicSharedCommitter)ClusterSafe()bool{return true}
var _ metastore.ClusterSafeCommitter=(*DynamicSharedCommitter)(nil)
