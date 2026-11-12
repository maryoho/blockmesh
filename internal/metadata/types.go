package metadata

import "errors"

var (
	ErrReadOnly        = errors.New("volume activated read-only")
	ErrNotOwner        = errors.New("node is not current RW owner")
	ErrMappingNotFound = errors.New("mapping not found")
	ErrReaderNotFound  = errors.New("reader not found")
	ErrUnsafeReclaim   = errors.New("cannot reclaim while a reader may use old generation")
)

const (
	ModeRO = "ro"
	ModeRW = "rw"
)

type Extent struct {
	LogicalStart  uint64 `json:"logical_start"`
	PhysicalStart uint64 `json:"physical_start"`
	Length        uint64 `json:"length"`
}
type Reader struct {
	NodeID     string `json:"node_id"`
	BootID     string `json:"boot_id"`
	Epoch      uint64 `json:"epoch"`
	Generation uint64 `json:"generation"`
	State      string `json:"state"`
}
type Retired struct {
	PhysicalBlock uint64 `json:"physical_block"`
	Generation    uint64 `json:"generation"`
	TxID          uint64 `json:"tx_id"`
}
type Descriptor struct {
	VolumeID        string `json:"volume_id"`
	OwnerNode       string `json:"owner_node"`
	OwnerEpoch      uint64 `json:"owner_epoch"`
	MappingSequence uint64 `json:"mapping_sequence"`
	RootGeneration  uint64 `json:"root_generation"`
	Mode            string `json:"mode"`
}
type diskState struct {
	Descriptor  Descriptor `json:"descriptor"`
	Mappings    []Extent   `json:"mappings"`
	Readers     []Reader   `json:"readers"`
	Retired     []Retired  `json:"retired"`
	Bitmap      []byte     `json:"bitmap"`
	TotalBlocks uint64     `json:"total_blocks"`
	LastTxID    uint64     `json:"last_tx_id"`
}
