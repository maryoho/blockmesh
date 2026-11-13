//go:build linux

package device

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// StableBlockIdentity returns a device identity suitable for cluster-wide
// sanlock resource naming. It deliberately refuses to fall back to /dev path
// names, which are not stable across hosts or reboots.
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
	// Device Mapper multipath exposes the canonical WWID in dm/uuid as
	// "mpath-<WWID>". Prefer it over the transient dm-N device number.
	if raw, readErr := os.ReadFile(filepath.Join(resolved, "dm", "uuid")); readErr == nil {
		value := strings.TrimSpace(string(raw))
		if strings.HasPrefix(strings.ToLower(value), "mpath-") && len(value) > len("mpath-") {
			return strings.ToLower(strings.TrimPrefix(value, "mpath-")), nil
		}
	}
	for _, candidate := range []string{
		filepath.Join(resolved, "device", "wwid"),
		filepath.Join(resolved, "wwid"),
		filepath.Join(resolved, "device", "serial"),
	} {
		raw, readErr := os.ReadFile(candidate)
		if readErr != nil {
			continue
		}
		value := strings.TrimSpace(string(raw))
		if value != "" {
			return strings.ToLower(value), nil
		}
	}
	return "", fmt.Errorf("block device %d:%d has no stable WWID or serial in sysfs", major, minor)
}
