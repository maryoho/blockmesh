//go:build linux

package device

import (
	"os"
	"path/filepath"
	"testing"
	"strings"
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

func writeSysfsID(t *testing.T, base, rel, value string) {
	t.Helper()
	path := filepath.Join(base, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil { t.Fatal(err) }
	if err := os.WriteFile(path, []byte(value+"\n"), 0644); err != nil { t.Fatal(err) }
}

func TestStableBlockIdentityResolvesMultipathThroughBackingWWIDs(t *testing.T) {
	root := t.TempDir()
	base := makeSysfsBlock(t, root, "virtual/block/dm-0")
	for _, name := range []string{"sdb", "sdc"} {
		slave := filepath.Join(root, "devices", "pci0000", "block", name)
		writeSysfsID(t, slave, "device/wwid", "naa.6001405abc123")
		slaveLink := filepath.Join(base, "slaves", name)
		if err := os.MkdirAll(filepath.Dir(slaveLink), 0755); err != nil { t.Fatal(err) }
		if err := os.Symlink(slave, slaveLink); err != nil { t.Fatal(err) }
	}
	// No dm/uuid is present: the identity comes from the common backing LUN WWID.
	got, err := stableBlockIdentityAt(root, 8, 16)
	if err != nil { t.Fatal(err) }
	if got != "naa.6001405abc123" { t.Fatalf("identity = %q", got) }
}

func TestStableBlockIdentityRejectsMultipathWithDifferentBackingWWIDs(t *testing.T) {
	root := t.TempDir()
	base := makeSysfsBlock(t, root, "virtual/block/dm-0")
	for i, name := range []string{"sdb", "sdc"} {
		slave := filepath.Join(root, "devices", "pci0000", "block", name)
		writeSysfsID(t, slave, "device/wwid", []string{"naa.lun-a", "naa.lun-b"}[i])
		slaveLink := filepath.Join(base, "slaves", name)
		if err := os.MkdirAll(filepath.Dir(slaveLink), 0755); err != nil { t.Fatal(err) }
		if err := os.Symlink(slave, slaveLink); err != nil { t.Fatal(err) }
	}
	if _, err := stableBlockIdentityAt(root, 8, 16); err == nil { t.Fatal("expected ambiguous backing identity error") }
}

func TestStableBlockIdentityReadsDirectSCSIWWID(t *testing.T) {
	root := t.TempDir()
	base := makeSysfsBlock(t, root, "pci0000/host0/target0/block/sdb")
	writeSysfsID(t, base, "device/wwid", "naa.6001405abc123")
	got, err := stableBlockIdentityAt(root, 8, 16)
	if err != nil { t.Fatal(err) }
	if got != "naa.6001405abc123" { t.Fatalf("identity = %q", got) }
}

func TestStableBlockIdentityReadsLocalDiskSerial(t *testing.T) {
	root := t.TempDir()
	base := makeSysfsBlock(t, root, "pci0000/0000:01:00.0/nvme/nvme1/nvme1n1")
	writeSysfsID(t, base, "device/serial", "LOCAL-DISK-SERIAL")
	got, err := stableBlockIdentityAt(root, 8, 16)
	if err != nil { t.Fatal(err) }
	if got != "local-disk-serial" { t.Fatalf("identity = %q", got) }
}

func TestStableBlockIdentityPrefersNVMeNamespaceIDOverControllerSerial(t *testing.T) {
	root := t.TempDir()
	base := makeSysfsBlock(t, root, "devices/virtual/block/nvme1n1")
	writeSysfsID(t, base, "device/serial", "controller-serial")
	writeSysfsID(t, base, "nguid", "00112233445566778899aabbccddeeff")
	got, err := stableBlockIdentityAt(root, 8, 16)
	if err != nil { t.Fatal(err) }
	if got != "00112233445566778899aabbccddeeff" { t.Fatalf("identity = %q", got) }
}

func TestStableBlockIdentityIncludesPartitionNumber(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "devices", "pci0000", "block", "sdb")
	writeSysfsID(t, parent, "device/wwid", "naa.6001405abc123")
	part := filepath.Join(parent, "sdb1")
	if err := os.MkdirAll(part, 0755); err != nil { t.Fatal(err) }
	writeSysfsID(t, part, "partition", "1")
	link := filepath.Join(root, "dev", "block", "8:16")
	if err := os.MkdirAll(filepath.Dir(link), 0755); err != nil { t.Fatal(err) }
	if err := os.Symlink(part, link); err != nil { t.Fatal(err) }
	got, err := stableBlockIdentityAt(root, 8, 16)
	if err != nil { t.Fatal(err) }
	if got != "naa.6001405abc123-part-1" { t.Fatalf("identity = %q", got) }
}

func TestStableBlockIdentityFallsBackToLocalTopologyWhenStableIDMissing(t *testing.T) {
	root := t.TempDir()
	_ = makeSysfsBlock(t, root, "virtual/block/sdb")
	got, err := stableBlockIdentityAt(root, 8, 16)
	if err != nil { t.Fatal(err) }
	if !strings.HasPrefix(got, "local-topology:") { t.Fatalf("identity = %q", got) }
}

func TestStableBlockIdentityRejectsMissingSysfsDevice(t *testing.T) {
	if _, err := stableBlockIdentityAt(t.TempDir(), 8, 16); err == nil { t.Fatal("expected missing sysfs device error") }
}

func TestStableBlockIdentityUsesHostLocalTopologyWhenHardwareIDMissing(t *testing.T) {
	root := t.TempDir()
	_ = makeSysfsBlock(t, root, "pci0000/0000:00:10.0/host2/target2:0:1/2:0:1:0/block/sdb")
	got, err := stableBlockIdentityAt(root, 8, 16)
	if err != nil { t.Fatal(err) }
	if !strings.HasPrefix(got, "local-topology:") { t.Fatalf("identity = %q", got) }
	again, err := stableBlockIdentityAt(root, 8, 16)
	if err != nil { t.Fatal(err) }
	if got != again { t.Fatalf("identity changed: %q != %q", got, again) }
}

func TestStableBlockIdentityDoesNotUseTopologyFallbackForMultipathLeaves(t *testing.T) {
	root := t.TempDir()
	base := makeSysfsBlock(t, root, "virtual/block/dm-0")
	for _, name := range []string{"sdb", "sdc"} {
		slave := filepath.Join(root, "devices", "pci0000", "host0", name)
		if err := os.MkdirAll(slave, 0755); err != nil { t.Fatal(err) }
		slaveLink := filepath.Join(base, "slaves", name)
		if err := os.MkdirAll(filepath.Dir(slaveLink), 0755); err != nil { t.Fatal(err) }
		if err := os.Symlink(slave, slaveLink); err != nil { t.Fatal(err) }
	}
	if _, err := stableBlockIdentityAt(root, 8, 16); err == nil { t.Fatal("expected multipath backing identities to be rejected without real LUN IDs") }
}
