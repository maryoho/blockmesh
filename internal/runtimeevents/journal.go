// Package runtimeevents provides a durable, hash-chained event journal for
// BlockMesh Owner recovery qualification. It is an evidence side channel: it
// never writes to the SAN metadata device and never performs fencing.
package runtimeevents

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
)

const SchemaVersion = "blockmesh-runtime-event/v1"

type Event struct {
	SchemaVersion string    `json:"schema_version"`
	OperationID   string    `json:"operation_id"`
	VolumeID      string    `json:"volume_id"`
	NodeID        string    `json:"node_id"`
	BootID        string    `json:"boot_id"`
	OwnerEpoch    uint64    `json:"owner_epoch,omitempty"`
	EventType     string    `json:"event_type"`
	Phase         string    `json:"phase"`
	Result        string    `json:"result"`
	Proof         string    `json:"proof"`
	Detail        string    `json:"detail,omitempty"`
	ObservedAt    time.Time `json:"observed_at"`
}

type Record struct {
	Sequence    uint64 `json:"sequence"`
	PreviousHash string `json:"previous_hash"`
	Event       Event  `json:"event"`
	Hash        string `json:"hash"`
}

type Sink interface { Record(Event) error }

type Journal struct {
	mu sync.Mutex
	f  *os.File
	operationID string
	sequence uint64
	lastHash string
}

func Open(path, operationID string) (*Journal, error) {
	if strings.TrimSpace(path)=="" || strings.TrimSpace(operationID)=="" {
		return nil, errors.New("journal path and operation ID are required")
	}
	f,err:=os.OpenFile(path,os.O_CREATE|os.O_RDWR,0600)
	if err!=nil{return nil,err}
	if err=f.Chmod(0600);err!=nil{_ = f.Close();return nil,err}
	if err=syscall.Flock(int(f.Fd()),syscall.LOCK_EX|syscall.LOCK_NB);err!=nil{_ = f.Close();return nil,fmt.Errorf("lock runtime event journal: %w",err)}
	j:=&Journal{f:f,operationID:operationID}
	if err=j.scan();err!=nil{_ = syscall.Flock(int(f.Fd()),syscall.LOCK_UN);_ = f.Close();return nil,err}
	return j,nil
}

func (j *Journal) scan() error {
	if _,err:=j.f.Seek(0,0);err!=nil{return err}
	s:=bufio.NewScanner(j.f)
	s.Buffer(make([]byte,4096),4<<20)
	var seq uint64
	var prev string
	for s.Scan(){
		line:=append([]byte(nil),s.Bytes()...)
		var r Record
		if err:=json.Unmarshal(line,&r);err!=nil{return fmt.Errorf("decode runtime event record %d: %w",seq+1,err)}
		if r.Sequence!=seq+1 || r.PreviousHash!=prev{return fmt.Errorf("runtime event chain discontinuity at sequence %d",seq+1)}
		want,err:=recordHash(r.Sequence,r.PreviousHash,r.Event);if err!=nil{return err}
		if r.Hash!=want{return fmt.Errorf("runtime event hash mismatch at sequence %d",r.Sequence)}
		if r.Event.SchemaVersion!=SchemaVersion || r.Event.OperationID!=j.operationID{return fmt.Errorf("runtime event identity/schema mismatch at sequence %d",r.Sequence)}
		seq=r.Sequence;prev=r.Hash
	}
	if err:=s.Err();err!=nil{return err}
	info,err:=j.f.Stat();if err!=nil{return err}
	if info.Size()>0 {
		last:=make([]byte,1)
		if _,err=j.f.ReadAt(last,info.Size()-1);err!=nil{return err}
		if last[0]!='\n'{return errors.New("runtime event journal has an incomplete trailing record; preserve it and start a new journal")}
	}
	j.sequence=seq;j.lastHash=prev
	_,err=j.f.Seek(0,2)
	return err
}

func recordHash(sequence uint64, previous string, event Event)(string,error){
	body:=struct{
		Sequence uint64 `json:"sequence"`
		PreviousHash string `json:"previous_hash"`
		Event Event `json:"event"`
	}{sequence,previous,event}
	b,err:=json.Marshal(body);if err!=nil{return "",err}
	sum:=sha256.Sum256(b)
	return hex.EncodeToString(sum[:]),nil
}

func (j *Journal) Record(e Event) error {
	if j==nil{return errors.New("runtime event journal is closed")}
	j.mu.Lock();defer j.mu.Unlock()
	if j.f==nil{return errors.New("runtime event journal is closed")}
	if e.SchemaVersion==""{e.SchemaVersion=SchemaVersion}
	if e.OperationID==""{e.OperationID=j.operationID}
	if e.SchemaVersion!=SchemaVersion || e.OperationID!=j.operationID || e.VolumeID=="" || e.NodeID=="" || e.BootID=="" || e.EventType=="" || e.Phase=="" || (e.Result!="success" && e.Result!="failure") || e.Proof==""{
		return errors.New("runtime event is missing required identity, phase, result or proof fields")
	}
	if e.ObservedAt.IsZero(){e.ObservedAt=time.Now().UTC()}
	seq:=j.sequence+1
	hash,err:=recordHash(seq,j.lastHash,e);if err!=nil{return err}
	r:=Record{Sequence:seq,PreviousHash:j.lastHash,Event:e,Hash:hash}
	b,err:=json.Marshal(r);if err!=nil{return err}
	b=append(b,'\n')
	if _,err=j.f.Write(b);err!=nil{return err}
	if err=j.f.Sync();err!=nil{return fmt.Errorf("sync runtime event sequence %d: %w",seq,err)}
	j.sequence=seq;j.lastHash=hash
	return nil
}

func (j *Journal) Close() error {
	if j==nil{return nil}
	j.mu.Lock();defer j.mu.Unlock()
	if j.f==nil{return nil}
	err1:=j.f.Sync()
	err2:=syscall.Flock(int(j.f.Fd()),syscall.LOCK_UN)
	err3:=j.f.Close();j.f=nil
	return errors.Join(err1,err2,err3)
}

func Read(path string)([]Record,error){
	b,err:=os.ReadFile(path);if err!=nil{return nil,err}
	if len(b)>0 && b[len(b)-1]!='\n'{return nil,errors.New("runtime event journal has an incomplete trailing record")}
	var out []Record
	var seq uint64;var prev string
	for _,line:=range strings.Split(strings.TrimSuffix(string(b),"\n"),"\n"){
		if line==""{continue}
		var r Record
		if err=json.Unmarshal([]byte(line),&r);err!=nil{return nil,err}
		if r.Sequence!=seq+1 || r.PreviousHash!=prev{return nil,fmt.Errorf("runtime event chain discontinuity at sequence %d",seq+1)}
		want,e:=recordHash(r.Sequence,r.PreviousHash,r.Event);if e!=nil{return nil,e}
		if r.Hash!=want{return nil,fmt.Errorf("runtime event hash mismatch at sequence %d",r.Sequence)}
		if r.Event.SchemaVersion!=SchemaVersion{return nil,fmt.Errorf("unsupported runtime event schema %q",r.Event.SchemaVersion)}
		seq=r.Sequence;prev=r.Hash;out=append(out,r)
	}
	return out,nil
}
