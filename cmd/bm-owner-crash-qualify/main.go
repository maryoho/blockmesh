package main

import (
	"encoding/binary"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"os"
	
	"strings"
	"time"

	"blockmesh/internal/device"
	"blockmesh/internal/format"
	"blockmesh/internal/metadata"
	"blockmesh/internal/metastore"
	"blockmesh/internal/volume"
)

type pageEvidence struct {
	Index int `json:"index"`
	Offset uint64 `json:"offset_bytes"`
	SHA256 string `json:"sha256"`
	NonZero bool `json:"nonzero"`
	Valid bool `json:"valid"`
	Generation uint64 `json:"generation,omitempty"`
	OwnerEpoch uint64 `json:"owner_epoch,omitempty"`
	VolumeID string `json:"volume_id,omitempty"`
	Error string `json:"error,omitempty"`
}

type snapshot struct {
	Schema string `json:"schema"`
	CapturedAt time.Time `json:"captured_at"`
	Hostname string `json:"hostname"`
	BootID string `json:"boot_id"`
	Kernel string `json:"kernel"`
	Device string `json:"device"`
	VolumeID string `json:"volume_id"`
	VolumeIndex uint32 `json:"volume_index"`
	PoolUUID string `json:"pool_uuid"`
	DescriptorPages []pageEvidence `json:"descriptor_pages"`
	OwnerRecordPages []pageEvidence `json:"owner_record_pages"`
	SelectedOwner *ownerIdentity `json:"selected_owner,omitempty"`
}

type ownerIdentity struct {
	VolumeID string `json:"volume_id"`
	NodeID string `json:"node_id"`
	BootID string `json:"boot_id"`
	Epoch uint64 `json:"epoch"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "bm-owner-crash-qualify:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args)==0 { return errors.New("usage: snapshot --device <multipath> --index <n> --volume-id <id> [--output file] | verify --before file --after file --node-id id --boot-id id") }
	switch args[0] {
	case "snapshot": return runSnapshot(args[1:])
	case "verify": return runVerify(args[1:])
	default: return fmt.Errorf("unknown command %q", args[0])
	}
}

func runSnapshot(args []string) error {
	fs:=flag.NewFlagSet("snapshot",flag.ContinueOnError)
	path:=fs.String("device","","read-only metadata multipath device")
	index:=fs.Uint("index",0,"Pool Volume index")
	volumeID:=fs.String("volume-id","","expected Volume ID")
	out:=fs.String("output","","new JSON evidence file; existing files are never overwritten")
	if err:=fs.Parse(args);err!=nil{return err}
	if fs.NArg()!=0 || *path=="" || *volumeID=="" { return errors.New("--device and --volume-id are required; no positional arguments are accepted") }
	if *index>=format.MaxVolumeDescriptors { return errors.New("Volume index exceeds Node Table capacity") }

	dev,err:=device.OpenBlockDevice(*path,false,false)
	if err!=nil{return err}
	defer dev.Close()
	pool,err:=metadata.OpenStoreDevice(dev)
	if err!=nil{return fmt.Errorf("read Pool superblocks: %w",err)}
	desc,err:=metadata.ReadVolumeDescriptor(dev,pool.SB,uint32(*index))
	if err!=nil{return fmt.Errorf("read Volume Descriptor: %w",err)}
	if desc.VolumeID!=*volumeID{return fmt.Errorf("Volume ID mismatch: device has %q, expected %q",desc.VolumeID,*volumeID)}
	store,err:=metastore.OpenPoolVolume(dev,pool.SB,uint32(*index),*volumeID)
	if err!=nil{return fmt.Errorf("validate Pool Volume metadata geometry: %w",err)}
	descPair,err:=format.VolumeDescriptorPairOffset(pool.SB,uint32(*index))
	if err!=nil{return err}
	descPages,err:=readDescriptorPages(dev,descPair,pool.SB)
	if err!=nil{return err}
	ownerA,ownerB,err:=store.OwnerRecordRange()
	if err!=nil{return err}
	ownerPages,err:=readOwnerPages(dev,[2]uint64{ownerA,ownerB})
	if err!=nil{return err}
	ownerStore,err:=volume.NewSANOwnerRecordStore(volume.SANOwnerRecordStoreConfig{
		Device:dev,VolumeID:*volumeID,SlotAOffset:ownerA,SlotBOffset:ownerB,
	})
	if err!=nil{return err}
	record,exists,err:=ownerStore.Load(nilSafeContext(),*volumeID)
	if err!=nil{return fmt.Errorf("decode durable OwnerRecord: %w",err)}
	var selected *ownerIdentity
	if exists { selected=&ownerIdentity{VolumeID:record.VolumeID,NodeID:record.NodeID,BootID:record.BootID,Epoch:record.Epoch} }
	host,_:=os.Hostname()
	bootBytes,_:=os.ReadFile("/proc/sys/kernel/random/boot_id")
	kernelBytes,_:=os.ReadFile("/proc/sys/kernel/osrelease")
	e:=snapshot{
		Schema:"blockmesh-owner-crash-evidence/v1",CapturedAt:time.Now().UTC(),
		Hostname:host,BootID:strings.TrimSpace(string(bootBytes)),Kernel:strings.TrimSpace(string(kernelBytes)),
		Device:*path,VolumeID:*volumeID,VolumeIndex:uint32(*index),PoolUUID:fmt.Sprintf("%x",pool.SB.PoolUUID),
		DescriptorPages:descPages,OwnerRecordPages:ownerPages,SelectedOwner:selected,
	}
	if *out=="" { enc:=json.NewEncoder(os.Stdout);enc.SetIndent("","  ");return enc.Encode(e) }
	f,err:=os.OpenFile(*out,os.O_CREATE|os.O_EXCL|os.O_WRONLY,0600)
	if err!=nil{return fmt.Errorf("create evidence (existing files are never overwritten): %w",err)}
	enc:=json.NewEncoder(f);enc.SetIndent("","  ")
	writeErr:=enc.Encode(e)
	syncErr:=f.Sync()
	closeErr:=f.Close()
	if writeErr!=nil{return writeErr};if syncErr!=nil{return syncErr};if closeErr!=nil{return closeErr}
	fmt.Fprintf(os.Stderr,"evidence written: %s\n",*out)
	return nil
}

// nilSafeContext is intentionally non-cancellable for a short local read-only
// snapshot. The store still checks its context before touching SAN.
func nilSafeContext() context.Context { return context.Background() }

func readDescriptorPages(dev *device.BlockDevice, pair uint64, pool format.Superblock) ([]pageEvidence,error) {
	out:=make([]pageEvidence,0,2)
	for i:=0;i<2;i++ {
		off:=pair+uint64(i)*format.PageSize
		b:=make([]byte,format.PageSize)
		n,err:=dev.ReadAt(b,int64(off))
		if err!=nil || n!=len(b) { return nil,fmt.Errorf("read descriptor page %d: n=%d err=%v",i,n,err) }
		p:=pageEvidence{Index:i,Offset:off,SHA256:sha256Hex(b),NonZero:!allZero(b)}
		d,decodeErr:=format.DecodeVolumeDescriptor(b)
		if decodeErr==nil { decodeErr=d.ValidatePool(pool) }
		if decodeErr!=nil { p.Error=decodeErr.Error() } else {
			p.Valid=true;p.Generation=d.Generation;p.OwnerEpoch=d.OwnerEpoch;p.VolumeID=d.VolumeID
		}
		out=append(out,p)
	}
	return out,nil
}

func readOwnerPages(dev *device.BlockDevice, offsets [2]uint64) ([]pageEvidence,error) {
	out:=make([]pageEvidence,0,2)
	for i,off:=range offsets {
		b:=make([]byte,4096)
		n,err:=dev.ReadAt(b,int64(off))
		if err!=nil || n!=len(b) { return nil,fmt.Errorf("read OwnerRecord page %d: n=%d err=%v",i,n,err) }
		p:=pageEvidence{Index:i,Offset:off,SHA256:sha256Hex(b),NonZero:!allZero(b)}
		if len(b)==4096 && string(b[:8])=="BMOWNR01" && binary.LittleEndian.Uint32(b[8:12])==1 &&
			crc32.Checksum(b[:4092],crc32.MakeTable(crc32.Castagnoli))==binary.LittleEndian.Uint32(b[4092:]) {
			p.Valid=true;p.Generation=binary.LittleEndian.Uint64(b[16:24]);p.OwnerEpoch=binary.LittleEndian.Uint64(b[24:32])
		} else if p.NonZero { p.Error="invalid magic/version/CRC32C" }
		out=append(out,p)
	}
	return out,nil
}

func runVerify(args []string) error {
	fs:=flag.NewFlagSet("verify",flag.ContinueOnError)
	beforePath:=fs.String("before","","pre-fault snapshot JSON")
	afterPath:=fs.String("after","","post-recovery snapshot JSON")
	node:=fs.String("node-id","","expected new Owner NodeID")
	boot:=fs.String("boot-id","","expected new Owner BootID")
	if err:=fs.Parse(args);err!=nil{return err}
	if fs.NArg()!=0 || *beforePath=="" || *afterPath=="" || *node=="" || *boot=="" { return errors.New("--before, --after, --node-id and --boot-id are required") }
	before,err:=loadSnapshot(*beforePath);if err!=nil{return err}
	after,err:=loadSnapshot(*afterPath);if err!=nil{return err}
	result,err:=verifySnapshots(before,after,*node,*boot)
	if err!=nil{return err}
	enc:=json.NewEncoder(os.Stdout);enc.SetIndent("","  ")
	return enc.Encode(result)
}

type verification struct {
	Passed bool `json:"passed"`
	VolumeID string `json:"volume_id"`
	PreviousMaxEpoch uint64 `json:"previous_max_epoch"`
	RecoveredOwnerEpoch uint64 `json:"recovered_owner_epoch"`
	DescriptorEpoch uint64 `json:"descriptor_epoch"`
	OwnerRecordIdentity string `json:"owner_record_identity"`
	Checks []string `json:"checks"`
}

func loadSnapshot(path string)(snapshot,error){
	var s snapshot
	b,err:=os.ReadFile(path);if err!=nil{return s,err}
	if err=json.Unmarshal(b,&s);err!=nil{return s,fmt.Errorf("decode %s: %w",path,err)}
	if s.Schema!="blockmesh-owner-crash-evidence/v1"{return s,fmt.Errorf("%s has unsupported evidence schema",path)}
	return s,nil
}

func verifySnapshots(before,after snapshot,node,boot string)(verification,error){
	fail:=func(format string,args ...any)(verification,error){return verification{},fmt.Errorf(format,args...)}
	if before.VolumeID=="" || before.VolumeID!=after.VolumeID || before.PoolUUID!=after.PoolUUID || before.VolumeIndex!=after.VolumeIndex {
		return fail("before/after snapshots refer to different Pool Volume identities")
	}
	if after.SelectedOwner==nil{return fail("post-recovery SAN OwnerRecord is absent")}
	if after.SelectedOwner.NodeID!=node || after.SelectedOwner.BootID!=boot || after.SelectedOwner.VolumeID!=after.VolumeID {
		return fail("post-recovery OwnerRecord identity does not match expected node/boot/volume")
	}
	oldMax:=uint64(0)
	for _,p:=range before.DescriptorPages {if p.Valid && p.OwnerEpoch>oldMax {oldMax=p.OwnerEpoch}}
	for _,p:=range before.OwnerRecordPages {if p.Valid && p.OwnerEpoch>oldMax {oldMax=p.OwnerEpoch}}
	if before.SelectedOwner!=nil && before.SelectedOwner.Epoch>oldMax {oldMax=before.SelectedOwner.Epoch}
	if oldMax==0{return fail("pre-fault evidence has no valid durable OwnerEpoch")}
	if after.SelectedOwner.Epoch<=oldMax{return fail("recovered OwnerEpoch %d is not greater than pre-fault maximum %d",after.SelectedOwner.Epoch,oldMax)}
	var descriptorEpoch uint64
	var descriptorGeneration uint64
	validDescriptors:=0
	for _,p:=range after.DescriptorPages {
		if !p.Valid {continue}
		validDescriptors++
		if p.Generation>descriptorGeneration {descriptorGeneration=p.Generation;descriptorEpoch=p.OwnerEpoch}
	}
	if validDescriptors==0{return fail("post-recovery has no valid BMDVOL02 descriptor copy")}
	if descriptorEpoch!=after.SelectedOwner.Epoch {
		return fail("BMDVOL02 descriptor OwnerEpoch %d does not match SAN OwnerRecord Epoch %d",descriptorEpoch,after.SelectedOwner.Epoch)
	}
	return verification{Passed:true,VolumeID:after.VolumeID,PreviousMaxEpoch:oldMax,RecoveredOwnerEpoch:after.SelectedOwner.Epoch,DescriptorEpoch:descriptorEpoch,OwnerRecordIdentity:node+"/"+boot,Checks:[]string{
		"same Pool UUID, Volume ID and Volume index before/after",
		"new SAN OwnerRecord identity matches expected node and boot",
		"new OwnerRecord Epoch is greater than every valid pre-fault durable epoch",
		"highest-generation valid BMDVOL02 descriptor Epoch matches SAN OwnerRecord",
	}},nil
}

func allZero(b []byte)bool{for _,v:=range b{if v!=0{return false}};return true}
func sha256Hex(b []byte)string{sum:=sha256.Sum256(b);return hex.EncodeToString(sum[:])}

var _ = runtime.GOOS
