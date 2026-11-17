//go:build linux

package device

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// StableBlockIdentity returns the persistent identity of a block device.
// It uses identifiers exported by the underlying device (WWID, NGUID, EUI or
// serial), never the transient /dev name or kernel major:minor number.
//
// For stacked devices it follows sysfs "slaves" and requires every backing
// device to resolve to the same identity. This naturally handles multipath
// maps without depending on Device Mapper UUIDs or dm-N names.
func StableBlockIdentity(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("block device path is required")
	}
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return "", fmt.Errorf("stat block device %q: %w", path, err)
	}
	if st.Mode&syscall.S_IFMT != syscall.S_IFBLK {
		return "", fmt.Errorf("%w: %s", ErrNotBlockDevice, path)
	}
	major := uint32((uint64(st.Rdev) >> 8) & 0xfff)
	major |= uint32((uint64(st.Rdev) >> 32) & 0xfffff000)
	minor := uint32(uint64(st.Rdev) & 0xff)
	minor |= uint32((uint64(st.Rdev) >> 12) & 0xffffff00)
	return stableBlockIdentityAt("/sys", major, minor)
}

func stableBlockIdentityAt(sysfsRoot string, major, minor uint32) (string, error) {
	base := filepath.Join(sysfsRoot, "dev", "block", fmt.Sprintf("%d:%d", major, minor))
	resolved, err := filepath.EvalSymlinks(base)
	if err != nil {
		return "", fmt.Errorf("resolve sysfs block device %d:%d: %w", major, minor, err)
	}
	return stableIdentityForSysfsNode(resolved, make(map[string]bool))
}

func stableIdentityForSysfsNode(node string, visiting map[string]bool) (string, error) {
	resolved, err := filepath.EvalSymlinks(node)
	if err != nil {
		return "", fmt.Errorf("resolve sysfs node %q: %w", node, err)
	}
	if visiting[resolved] {
		return "", fmt.Errorf("sysfs backing-device cycle at %q", resolved)
	}
	visiting[resolved] = true
	defer delete(visiting, resolved)

	// A partition shares its parent disk's persistent identifier, but must
	// retain the partition number so two Pools on different partitions do not
	// collide.
	if raw, readErr := os.ReadFile(filepath.Join(resolved, "partition")); readErr == nil {
		part := strings.TrimSpace(string(raw))
		if part != "" {
			parentID, parentErr := stableIdentityForSysfsNode(filepath.Dir(resolved), visiting)
			if parentErr != nil {
				return "", fmt.Errorf("resolve parent identity for partition %s: %w", part, parentErr)
			}
			return fmt.Sprintf("%s-part-%s", parentID, part), nil
		}
	}

	// Stacked block devices expose their backing devices in "slaves".
	// Requiring a single common identity avoids treating an arbitrary
	// dm-linear/crypt aggregate as one physical LUN.
	entries, readErr := os.ReadDir(filepath.Join(resolved, "slaves"))
	if readErr == nil && len(entries) > 0 {
		var identity string
		for _, entry := range entries {
			backingID, backingErr := stableIdentityForSysfsNode(filepath.Join(resolved, "slaves", entry.Name()), visiting)
			if backingErr != nil {
				return "", fmt.Errorf("resolve backing device %q: %w", entry.Name(), backingErr)
			}
			if identity == "" {
				identity = backingID
				continue
			}
			if identity != backingID {
				return "", fmt.Errorf("stacked block device %q has multiple distinct backing identities %q and %q", resolved, identity, backingID)
			}
		}
		if identity != "" {
			return identity, nil
		}
	}

	// These are device-independent sysfs attributes. WWID is preferred for
	// SCSI/iSCSI LUNs; NGUID/EUI identify NVMe namespaces; serial is the
	// fallback for local disks that do not expose the stronger identifiers.
	for _, candidate := range []string{
		filepath.Join(resolved, "device", "wwid"),
		filepath.Join(resolved, "wwid"),
		filepath.Join(resolved, "nguid"),
		filepath.Join(resolved, "device", "nguid"),
		filepath.Join(resolved, "eui"),
		filepath.Join(resolved, "device", "eui"),
		filepath.Join(resolved, "device", "serial"),
		filepath.Join(resolved, "serial"),
	} {
		raw, candidateErr := os.ReadFile(candidate)
		if candidateErr != nil {
			continue
		}
		value := strings.ToLower(strings.TrimSpace(string(raw)))
		if value != "" && value != "0" {
			return value, nil
		}
	}
	// Local devices without exported WWID/serial use a host-local sysfs
	// topology identity. It is not a shared-SAN identifier and must never be
	// used as a substitute for cross-node fencing.
	topology := filepath.Clean(resolved)
	marker := string(filepath.Separator) + "devices" + string(filepath.Separator)
	if idx := strings.Index(topology, marker); idx >= 0 {
		topology = topology[idx+len(marker):]
	}
	sum := sha256.Sum256([]byte(topology))
	return "local-topology:" + hex.EncodeToString(sum[:]), nil
}
