package poolspace

import (
	"encoding/binary"
	"errors"
	"fmt"

	"blockmesh/internal/device"
)

type FormatVersion uint32
const (
	FormatUnknown FormatVersion = 0
	FormatBMPOOL02 FormatVersion = 2
	FormatBMPOOL03 FormatVersion = 3
)

var ErrUnknownPoolFormat = errors.New("unrecognized BlockMesh Pool format")

// DetectFormat reads only the two fixed bootstrap pages. It recognizes legacy
// BMPOOL02 by magic even when its CRC is damaged, preventing a damaged legacy
// Pool from being mistaken for a blank device and reformatted as BMPOOL03.
func DetectFormat(dev device.Device) (FormatVersion,error) {
	if dev==nil || dev.Size()<int64(2*PageSize) { return FormatUnknown,ErrUnknownPoolFormat }
	magic:=make([]byte,8)
	seenLegacy,seenDynamic:=false,false
	for _,off:=range []int64{0,int64(PageSize)} {
		n,err:=dev.ReadAt(magic,off)
		if err!=nil && n!=len(magic) { return FormatUnknown,fmt.Errorf("read Pool format marker at %d: %w",off,err) }
		if n!=len(magic) { return FormatUnknown,ErrUnknownPoolFormat }
		switch binary.LittleEndian.Uint64(magic) {
		case 0x424D504F4F4C3032: seenLegacy=true
		case BootstrapMagic: seenDynamic=true
		}
	}
	if seenLegacy && seenDynamic { return FormatUnknown,errors.New("conflicting legacy and dynamic Pool format markers") }
	if seenLegacy { return FormatBMPOOL02,nil }
	if seenDynamic { return FormatBMPOOL03,nil }
	return FormatUnknown,ErrUnknownPoolFormat
}

// OpenPoolSpace dispatches only the dynamic format. Legacy Pools are
// deliberately not converted or rewritten by this package.
func OpenPoolSpace(dev device.Device) (*BootstrapStore,error) {
	version,err:=DetectFormat(dev)
	if err!=nil { return nil,err }
	switch version {
	case FormatBMPOOL02: return nil,ErrLegacyPoolFormat
	case FormatBMPOOL03: return OpenBootstrap(dev)
	default: return nil,ErrUnknownPoolFormat
	}
}
