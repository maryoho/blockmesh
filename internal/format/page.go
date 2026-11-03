package format

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

const (
	PageMagic      = uint64(0x424D504147453031) // BMPAGE01
	PageVersion    = uint32(1)
	PageHeaderSize = 64
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// EncodePage creates a fixed 4KiB metadata page. CRC field at byte offset 40 is zeroed while calculating CRC.
func EncodePage(pageType uint32, pageID, sequence uint64, payload []byte) ([]byte, error) {
	if len(payload) > int(PageSize)-PageHeaderSize {
		return nil, fmt.Errorf("payload too large: %d", len(payload))
	}
	b := make([]byte, PageSize)
	binary.LittleEndian.PutUint64(b[0:8], PageMagic)
	binary.LittleEndian.PutUint32(b[8:12], PageVersion)
	binary.LittleEndian.PutUint32(b[12:16], pageType)
	binary.LittleEndian.PutUint64(b[16:24], pageID)
	binary.LittleEndian.PutUint64(b[24:32], sequence)
	binary.LittleEndian.PutUint32(b[32:36], uint32(len(payload)))
	copy(b[PageHeaderSize:], payload)
	binary.LittleEndian.PutUint32(b[40:44], 0)
	binary.LittleEndian.PutUint32(b[40:44], crc32.Checksum(b, castagnoli))
	return b, nil
}

func DecodePage(b []byte, expectedType uint32) (pageID, sequence uint64, payload []byte, err error) {
	if len(b) != int(PageSize) {
		err = fmt.Errorf("page length %d != %d", len(b), PageSize)
		return
	}
	if binary.LittleEndian.Uint64(b[0:8]) != PageMagic {
		err = fmt.Errorf("bad page magic")
		return
	}
	if binary.LittleEndian.Uint32(b[8:12]) != PageVersion {
		err = fmt.Errorf("unsupported page version")
		return
	}
	if expectedType != 0 && binary.LittleEndian.Uint32(b[12:16]) != expectedType {
		err = fmt.Errorf("unexpected page type")
		return
	}
	stored := binary.LittleEndian.Uint32(b[40:44])
	copyBuf := append([]byte(nil), b...)
	binary.LittleEndian.PutUint32(copyBuf[40:44], 0)
	if crc32.Checksum(copyBuf, castagnoli) != stored {
		err = fmt.Errorf("page CRC32C mismatch")
		return
	}
	pageID = binary.LittleEndian.Uint64(b[16:24])
	sequence = binary.LittleEndian.Uint64(b[24:32])
	plen := binary.LittleEndian.Uint32(b[32:36])
	if plen > uint32(len(b))-PageHeaderSize {
		err = fmt.Errorf("invalid payload length")
		return
	}
	payload = append([]byte(nil), b[PageHeaderSize:PageHeaderSize+plen]...)
	return
}
