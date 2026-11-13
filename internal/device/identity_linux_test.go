//go:build linux

package device

import (
	"os"
	"path/filepath"
	"testing"
)

func makeSysfsBlock(t *testing.T, root, target string) string {
	t.Helper()
	base := filepath.Join(root, "devices", target)
	if err := os.MkdirAll(base, 0755); err != nil { t.Fatal(err) }
	link := filepath.Join(root, "dev", "block", "8:16")
	if err := os.MkdirAll(filepath.Dir(link), 0755); err != nil { t.Fatal(err) }
	if err := os.Symlink(base, link); err != nil { t.Fatal(err) }
	return base
}

func TestStableBlockIdentityPrefersMultipathWWID(t *testing.T) {
	root := t.TempDir()
	base := makeSysfsBlock(t, root, "virtual/block/dm-0")
	if err := os.MkdirAll(filepath.Join(base, "dm"), 0755); err != nil { t.Fatal(err) }
	if err := os.WriteFile(filepath.Join(base, "dm", "uuid"), []byte("mpath-3600508b400105e210000900000490000\n"), 0644); err != nil { t.Fatal(err) }
	got, err := stableBlockIdentityAt(root, 8, 16)
	if err != nil { t.Fatal(err) }
	if got != "3600508b400105e210000900000490000" { t.Fatalf("identity = %q", got) }
}

func TestStableBlockIdentityReadsDirectSCSIWWID(t *testing.T) {
	root := t.TempDir()
	base := makeSysfsBlock(t, root, "pci0000/host0/target0/block/sdb")
	if err := os.MkdirAll(filepath.Join(base, "device"), 0755); err != nil { t.Fatal(err) }
	if err := os.WriteFile(filepath.Join(base, "device", "wwid"), []byte("naa.6001405abc123\n"), 0644); err != nil { t.Fatal(err) }
	got, err := stableBlockIdentityAt(root, 8, 16)
	if err != nil { t.Fatal(err) }
	if got != "naa.6001405abc123" { t.Fatalf("identity = %q", got) }
}

func TestStableBlockIdentityRejectsMissingStableID(t *testing.T) {
	root := t.TempDir()
	_ = makeSysfsBlock(t, root, "virtual/block/sdb")
	if _, err := stableBlockIdentityAt(root, 8, 16); err == nil { t.Fatal("expected missing stable identity error") }
}

func TestStableBlockIdentityRejectsMissingSysfsDevice(t *testing.T) {
	if _, err := stableBlockIdentityAt(t.TempDir(), 8, 16); err == nil { t.Fatal("expected missing sysfs device error") }
}
