package metastore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"blockmesh/internal/lock"
)

// DistributedReaderCoordinator serializes Reader Table membership and root
// generation changes with the same cluster-wide resource on every SAN node.
// The same instance/resource naming must be used by writers before publishing
// a new root or reclaiming pages pinned by Readers.
type DistributedReaderCoordinator struct {
	Locker lock.DistributedLocker
	ResourceFor func(volumeID string) string
}

func (c *DistributedReaderCoordinator) LockReaderSet(volumeID string) (func(),error) {
	return c.LockReaderSetContext(context.Background(),volumeID)
}

func (c *DistributedReaderCoordinator) LockReaderSetContext(ctx context.Context, volumeID string) (func(),error) {
	if c==nil || ctx==nil || c.Locker==nil || !c.Locker.Distributed() || c.ResourceFor==nil {
		return nil,errors.New("distributed ReaderCoordinator requires a distributed locker and resource resolver")
	}
	if strings.TrimSpace(volumeID)=="" { return nil,errors.New("ReaderCoordinator Volume ID is required") }
	resource:=c.ResourceFor(volumeID)
	if strings.TrimSpace(resource)=="" { return nil,fmt.Errorf("Reader-set resource is empty for Volume %s",volumeID) }
	unlock,err:=c.Locker.Lock(ctx,resource)
	if err!=nil{return nil,fmt.Errorf("acquire distributed Reader-set barrier %s: %w",resource,err)}
	if unlock==nil{return nil,errors.New("distributed locker returned a nil Reader-set unlock")}
	return func(){ _ = unlock() },nil
}

var _ ReaderCoordinator = (*DistributedReaderCoordinator)(nil)
var _ ContextReaderCoordinator = (*DistributedReaderCoordinator)(nil)
