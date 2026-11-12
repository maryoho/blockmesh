package poolspace

import (
	"bytes"
	"io"
	"testing"

	"blockmesh/internal/device"
)

func TestMetadataViewSkipsBootstrapAndMapsFragmentedMetadata(t *testing.T) {
	f, err := device.OpenFile(t.TempDir()+"/metadata.img")
	if err != nil { t.Fatal(err) }
	defer f.Close()
	if _, err = f.WriteAt([]byte{0}, (48<<20)-1); err != nil { t.Fatal(err) }
	metadataExtents := []Extent{
		{Start:0, Length:BootstrapSize, Kind:Metadata},
		{Start:16<<20, Length:4<<20, Kind:Metadata},
		{Start:32<<20, Length:4<<20, Kind:Metadata},
	}
	view, err := NewMetadataView(f, metadataExtents)
	if err != nil { t.Fatal(err) }
	payload := bytes.Repeat([]byte{0x6b}, 5<<20)
	if n, err := view.WriteAt(payload, 0); err != nil || n != len(payload) {
		t.Fatalf("write n=%d err=%v", n, err)
	}
	first := make([]byte, 4<<20)
	if _, err = f.ReadAt(first, 16<<20); err != nil { t.Fatal(err) }
	second := make([]byte, 1<<20)
	if _, err = f.ReadAt(second, 32<<20); err != nil { t.Fatal(err) }
	if !bytes.Equal(first, payload[:4<<20]) || !bytes.Equal(second, payload[4<<20:]) {
		t.Fatal("MetadataView did not translate fragmented Metadata extents")
	}
	if _, err = view.WriteAt([]byte{1}, view.Size()); err == nil {
		t.Fatal("write at end should fail")
	}
	if _, err = view.ReadAt(make([]byte, 2), view.Size()-1); err != io.EOF {
		t.Fatalf("short read err=%v", err)
	}
	bootstrap := make([]byte, 16)
	if _, err = f.ReadAt(bootstrap, 0); err != nil { t.Fatal(err) }
	if !bytes.Equal(bootstrap, make([]byte, len(bootstrap))) {
		t.Fatal("MetadataView write modified Bootstrap reservation")
	}
}

func TestMetadataViewRejectsMissingBootstrapCoverage(t *testing.T) {
	f, err := device.OpenFile(t.TempDir()+"/metadata.img")
	if err != nil { t.Fatal(err) }
	defer f.Close()
	if _, err = f.WriteAt([]byte{0}, (16<<20)-1); err != nil { t.Fatal(err) }
	_, err = NewMetadataView(f, []Extent{{Start:BootstrapSize, Length:8<<20, Kind:Metadata}})
	if err == nil { t.Fatal("expected missing Bootstrap coverage to fail") }
}
