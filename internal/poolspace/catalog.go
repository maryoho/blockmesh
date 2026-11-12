package poolspace

import (
	"errors"
	"fmt"

	"blockmesh/internal/device"
)

var ErrPoolCatalogNameMissing = errors.New("BMPOOL03 Pool has no persisted catalog name")

// PoolCatalogEntry is the discoverable, read-only catalog view stored on the
// Pool's own LUN. The Bootstrap record is authoritative; this struct is a
// decoded projection and is safe to rebuild after a daemon restart.
type PoolCatalogEntry struct {
	Name string `json:"name"`
	UUID string `json:"uuid"`
	Generation uint64 `json:"generation"`
	DeviceBytes uint64 `json:"device_bytes"`
	MetadataBytes uint64 `json:"metadata_bytes"`
	DataBytes uint64 `json:"data_bytes"`
	FreeBytes uint64 `json:"free_bytes"`
	LogicalSectorBytes uint32 `json:"logical_sector_bytes"`
	ExtentCount int `json:"extent_count"`
}

func (s *BootstrapStore) CatalogEntry() (PoolCatalogEntry,error) {
	var entry PoolCatalogEntry
	if s==nil { return entry,ErrInvalidGeometry }
	r:=s.Record()
	if r.PoolName=="" { return entry,ErrPoolCatalogNameMissing }
	entry=PoolCatalogEntry{Name:r.PoolName,UUID:fmt.Sprintf("%x",r.PoolUUID),
		Generation:r.Generation,DeviceBytes:r.DeviceBytes,MetadataBytes:r.MetadataBytes,
		LogicalSectorBytes:r.LogicalSectorBytes,ExtentCount:len(s.Extents())}
	for _,e:=range s.Extents() {
		switch e.Kind {
		case Data: entry.DataBytes+=e.Length
		case Free: entry.FreeBytes+=e.Length
		}
	}
	return entry,nil
}

// ReadPoolCatalog discovers the catalog entry directly from a candidate LUN.
// It performs no writes and never treats an unknown or legacy device as blank.
func ReadPoolCatalog(dev device.Device) (PoolCatalogEntry,error) {
	store,err:=OpenPoolSpace(dev)
	if err!=nil { return PoolCatalogEntry{},err }
	entry,err:=store.CatalogEntry()
	if err!=nil { return PoolCatalogEntry{},err }
	return entry,nil
}
