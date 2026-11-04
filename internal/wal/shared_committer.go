package wal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"blockmesh/internal/device"
	"blockmesh/internal/format"
	"blockmesh/internal/lock"
	"blockmesh/internal/metastore"
)

// SharedCommitter opens a fresh Journal under a distributed sanlock resource
// before every operation. This is essential when multiple processes can write
// one AG WAL: each process must reload the current checkpoint, append offset,
// and next TxID while holding the same exclusive resource.
type SharedCommitter struct {
	Device device.Device
	Superblock format.Superblock
	AG uint32
	Locker lock.Locker
	Resource string
	Timeout time.Duration
}

func NewSharedCommitter(dev device.Device, sb format.Superblock, ag uint32, locker lock.Locker, resource string) (*SharedCommitter, error) {
	if dev == nil || locker == nil || resource == "" { return nil, errors.New("device, distributed locker and WAL resource are required") }
	if ag >= sb.AGCount { return nil, fmt.Errorf("AG %d out of range", ag) }
	return &SharedCommitter{Device:dev,Superblock:sb,AG:ag,Locker:locker,Resource:resource},nil
}

func (c *SharedCommitter) withJournal(fn func(*Journal) error) error {
	if c == nil || c.Device == nil || c.Locker == nil || c.Resource == "" { return errors.New("shared WAL committer is not configured") }
	timeout:=c.Timeout
	if timeout<=0 { timeout=30*time.Second }
	ctx,cancel:=context.WithTimeout(context.Background(),timeout)
	defer cancel()
	release,err:=c.Locker.Lock(ctx,c.Resource)
	if err!=nil { return fmt.Errorf("acquire shared AG%d WAL lease: %w",c.AG,err) }
	journal,openErr:=New(c.Device,c.Superblock,c.AG)
	if openErr!=nil { return errors.Join(openErr,release()) }
	operationErr:=fn(journal)
	releaseErr:=release()
	if operationErr!=nil && releaseErr!=nil { return errors.Join(operationErr,releaseErr) }
	if operationErr!=nil { return operationErr }
	return releaseErr
}

func (c *SharedCommitter) CommitImages(images []metastore.PageImage) error {
	return c.withJournal(func(j *Journal) error { return j.CommitImages(images) })
}

func (c *SharedCommitter) Recover() error {
	return c.withJournal(func(j *Journal) error { _,err:=j.Recover(); return err })
}

func (c *SharedCommitter) Checkpoint() error {
	return c.withJournal(func(j *Journal) error { return j.Checkpoint() })
}

var _ metastore.Committer = (*SharedCommitter)(nil)
