
// isVolumeDescriptor recognizes the Pool-level P3 Volume Descriptor page.
// The descriptor lives in the Node Table reserved region and is deliberately
// distinct from the metastore B+Tree superblock format.
func isVolumeDescriptor(b []byte) bool {
	return len(b) == int(format.PageSize) && string(b[:8]) == "BMDVOL01"
}

func validVolumeDescriptor(b []byte) bool {
	if !isVolumeDescriptor(b) || binary.LittleEndian.Uint32(b[8:12]) != 1 {
		return false
	}
	return crc32.Checksum(b[:len(b)-4], journalCRC) == binary.LittleEndian.Uint32(b[len(b)-4:])
}
