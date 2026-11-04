package device

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestFileDeviceDiscardPunchesHoleAndReadsAsZero(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.img")
	f, err := os.Create(path)
	if err != nil { t.Fatal(err) }
	if err = f.Truncate(4 << 20); err != nil { t.Fatal(err) }
	if _, err = f.WriteAt(bytes.Repeat([]byte{0xa5}, 4096), 1<<20); err != nil { t.Fatal(err) }
	if err = f.Close(); err != nil { t.Fatal(err) }

	d, err := Open(path, true)
	if err != nil { t.Fatal(err) }
	defer d.Close()
	if err = d.Discard(1<<20, 4096); err != nil { t.Fatal(err) }
	if err = d.Sync(); err != nil { t.Fatal(err) }
	got := make([]byte, 4096)
	if _, err = d.ReadAt(got, 1<<20); err != nil { t.Fatal(err) }
	if !bytes.Equal(got, make([]byte, len(got))) { t.Fatal("discarded range did not read back as zero") }
	if d.Size() != 4<<20 { t.Fatalf("discard changed file size: %d", d.Size()) }
}
