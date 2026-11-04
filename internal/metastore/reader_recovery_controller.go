package metastore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"blockmesh/internal/fence"
)

// MetadataIODrainBarrier must stop/observe every metadata read that can still
// reference retired B+Tree pages. It is deliberately separate from SAN PR
// task abort: PR fencing alone does not prove local DM queues have drained.
type MetadataIODrainBarrier interface {
	WaitForMetadataIOsDrained(context.Context, string) error
}

type ReaderRecoveryConfig struct {
	HeartbeatTimeout time.Duration
	MaxClockSkew time.Duration
	ScanInterval time.Duration
	RetryInterval time.Duration
	Clock func() time.Time
	OnError func(ReaderRecord, error)
}

type ReaderRecoveryReport struct {
	Scanned int
	Stale int
	Fenced int
	SkippedLegacyTimestamp int
	SkippedClockSkew int
	SkippedRenewed int
	SkippedChangedIdentity int
	RetryDeferred int
	Failed int
	ReclaimedPages int
}

// ReaderRecoveryController detects stale Reader records and performs the
// lease-probe -> SAN-fence -> durable-Fenced sequence. It never interprets a
// heartbeat timeout as proof of fencing.
type ReaderRecoveryController struct {
	Table *ReaderTable
	Probe ReaderLeaseProbe
	Fencer fence.Fencer
	Arena *PersistentArena
	Drain MetadataIODrainBarrier
	Config ReaderRecoveryConfig

	mu sync.Mutex
	lastAttempt map[readerRecoveryKey]time.Time
}

type readerRecoveryKey struct {
	Slot uint32
	Epoch uint64
	NodeID string
	BootID string
}

func (c *ReaderRecoveryController) validate() error {
	if c==nil || c.Table==nil || c.Probe==nil || c.Fencer==nil {
		return errors.New("reader recovery controller requires table, sanlock probe and SAN fencer")
	}
	if c.Table.coordinator==nil { return ErrReaderCoordinatorRequired }
	if c.Config.HeartbeatTimeout<=0 { return errors.New("heartbeat timeout must be positive") }
	if c.Config.MaxClockSkew<0 || c.Config.RetryInterval<0 { return errors.New("clock skew and retry interval cannot be negative") }
	return nil
}

func (c *ReaderRecoveryController) now() time.Time {
	if c.Config.Clock!=nil { return c.Config.Clock() }
	return time.Now()
}

func (c *ReaderRecoveryController) shouldAttempt(key readerRecoveryKey, now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastAttempt==nil { c.lastAttempt=make(map[readerRecoveryKey]time.Time) }
	last,ok:=c.lastAttempt[key]
	return !ok || c.Config.RetryInterval==0 || now.Sub(last)>=c.Config.RetryInterval
}

func (c *ReaderRecoveryController) markAttempt(key readerRecoveryKey, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastAttempt==nil { c.lastAttempt=make(map[readerRecoveryKey]time.Time) }
	c.lastAttempt[key]=now
}

func (c *ReaderRecoveryController) clearAttempt(key readerRecoveryKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.lastAttempt,key)
}

// RecoverOnce performs one bounded scan. Per-reader failures do not prevent
// other stale readers from being recovered; the returned error joins them.
func (c *ReaderRecoveryController) RecoverOnce(ctx context.Context) (ReaderRecoveryReport,error) {
	var report ReaderRecoveryReport
	if err:=c.validate();err!=nil{return report,err}
	readers,err:=c.Table.Snapshot()
	if err!=nil{return report,err}
	report.Scanned=len(readers)
	now:=c.now()
	var failures []error
	for _,reader:=range readers {
		if reader.State!=ReaderActive && reader.State!=ReaderLeaving { continue }
		if reader.HeartbeatUnixNano<=0 {
			report.SkippedLegacyTimestamp++
			continue // v1 Reader records have no trustworthy wall-clock heartbeat
		}
		last:=time.Unix(0,reader.HeartbeatUnixNano)
		age:=now.Sub(last)
		if age < -c.Config.MaxClockSkew {
			report.SkippedClockSkew++
			continue
		}
		if age < 0 || age <= c.Config.HeartbeatTimeout+c.Config.MaxClockSkew { continue }
		report.Stale++
		key:=readerRecoveryKey{Slot:reader.Slot,Epoch:reader.Epoch,NodeID:reader.NodeID,BootID:reader.BootID}
		if !c.shouldAttempt(key,now) { report.RetryDeferred++;continue }
		c.markAttempt(key,now)
		staleBefore:=now.Add(-c.Config.HeartbeatTimeout-c.Config.MaxClockSkew)
		if err:=c.Table.FenceExpiredReaderIfStale(ctx,reader.Slot,reader.NodeID,reader.BootID,reader.Epoch,staleBefore,c.Probe,c.Fencer);err!=nil {
			if errors.Is(err,ErrReaderNotStale) { c.clearAttempt(key);report.SkippedRenewed++;continue }
			if errors.Is(err,ErrReaderIdentity) { c.clearAttempt(key);report.SkippedChangedIdentity++;continue }
			report.Failed++
			failures=append(failures,fmt.Errorf("recover reader slot %d (%s/%s): %w",reader.Slot,reader.NodeID,reader.BootID,err))
			if c.Config.OnError!=nil { c.Config.OnError(reader,err) }
			continue
		}
		c.clearAttempt(key)
		report.Fenced++
	}
	// Revisit pending reclamation on every scan, not only after a newly
	// fenced reader. This recovers a crash between durable ReaderFenced and
	// Retire Queue collection.
	if c.Arena!=nil && len(c.Arena.Pending())>0 {
		if c.Drain==nil {
			failures=append(failures,errors.New("metadata I/O drain barrier is required before retire collection"))
		} else if err:=c.Drain.WaitForMetadataIOsDrained(ctx,c.Table.store.Superblock().VolumeID);err!=nil {
			failures=append(failures,fmt.Errorf("wait for metadata I/O drain: %w",err))
		} else {
			n,collectErr:=c.Arena.CollectWithReaders(c.Table,true)
			report.ReclaimedPages=n
			if collectErr!=nil { failures=append(failures,fmt.Errorf("collect retired metadata pages: %w",collectErr)) }
		}
	}
	return report,errors.Join(failures...)
}

// Run scans until ctx is cancelled. Transient per-reader errors are reported
// through OnError and RecoverOnce; they do not terminate the controller.
func (c *ReaderRecoveryController) Run(ctx context.Context) error {
	if err:=c.validate();err!=nil{return err}
	interval:=c.Config.ScanInterval
	if interval<=0 { interval=time.Second }
	ticker:=time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if _,err:=c.RecoverOnce(ctx);err!=nil && c.Config.OnError!=nil {
			c.Config.OnError(ReaderRecord{},err)
		}
		select {
		case <-ctx.Done(): return ctx.Err()
		case <-ticker.C:
		}
	}
}
