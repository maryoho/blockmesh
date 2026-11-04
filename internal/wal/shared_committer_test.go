package wal

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"blockmesh/internal/device"
	"blockmesh/internal/format"
	"blockmesh/internal/lock"
	"blockmesh/internal/metastore"
)

func TestSharedCommitterReloadsJournalForEveryProcessLikeWriter(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "shared-wal-*.img")
	if err != nil { t.Fatal(err) }
	defer f.Close()
	if err = f.Truncate(int64(format.MetadataSize)); err != nil { t.Fatal(err) }
	d, err := device.Open(f.Name(), true)
	if err != nil { t.Fatal(err) }
	defer d.Close()
	sb, err := format.NewSuperblock(0)
	if err != nil { t.Fatal(err) }
	if _, err = New(d,sb,0); err != nil { t.Fatal(err) }

	locker, err := lock.NewFileLocker(t.TempDir())
	if err != nil { t.Fatal(err) }
	a, err := NewSharedCommitter(d,sb,0,locker,"pool-ag0-wal")
	if err != nil { t.Fatal(err) }
	b, err := NewSharedCommitter(d,sb,0,locker,"pool-ag0-wal")
	if err != nil { t.Fatal(err) }

	const writes = 20
	var wg sync.WaitGroup
	errs := make(chan error,writes)
	for i:=0;i<writes;i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			image:=make([]byte,format.PageSize)
			copy(image,[]byte(fmt.Sprintf("shared-wal-%02d",i)))
			offset:=sb.MappingOffset+uint64(i)*format.PageSize
			committer:=a
			if i%2!=0 {committer=b}
			errs<-committer.CommitImages([]metastore.PageImage{{Offset:offset,Data:image}})
		}(i)
	}
	wg.Wait()
	close(errs)
	for err:=range errs {if err!=nil {t.Fatal(err)}}

	reopened,err:=New(d,sb,0)
	if err!=nil {t.Fatal(err)}
	if reopened.lastTx!=writes {t.Fatalf("WAL TxID=%d, want %d",reopened.lastTx,writes)}
	for i:=0;i<writes;i++ {
		got:=make([]byte,format.PageSize)
		offset:=sb.MappingOffset+uint64(i)*format.PageSize
		if n,readErr:=d.ReadAt(got,int64(offset));readErr!=nil||n!=len(got) {t.Fatalf("read %d: n=%d err=%v",i,n,readErr)}
		want:=fmt.Sprintf("shared-wal-%02d",i)
		if string(got[:len(want)])!=want {t.Fatalf("page %d=%q want %q",i,got[:len(want)],want)}
	}
}
