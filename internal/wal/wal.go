package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

var (
	ErrCorrupt     = errors.New("WAL corruption")
	ErrUncommitted = errors.New("transaction has no commit record")
)

const (
	magic      uint32 = 0x424d574c
	version    uint16 = 1
	headerSize        = 24
	maxRecord         = 64 << 20
	kindIntent uint16 = 1
	kindCommit uint16 = 2
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type Intent struct {
	TxID       uint64
	VolumeID   string
	Sequence   uint64
	OwnerEpoch uint64
	Payload    []byte
}
type Commit struct {
	TxID      uint64
	IntentCRC uint32
}
type Record struct {
	Intent *Intent
	Commit *Commit
}
type Log struct {
	mu sync.Mutex
	f  *os.File
}

func Open(path string) (*Log, error) {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	return &Log{f: f}, nil
}
func encode(kind uint16, tx uint64, payload []byte) []byte {
	b := make([]byte, headerSize+len(payload))
	binary.LittleEndian.PutUint32(b[0:4], magic)
	binary.LittleEndian.PutUint16(b[4:6], version)
	binary.LittleEndian.PutUint16(b[6:8], kind)
	binary.LittleEndian.PutUint32(b[8:12], uint32(len(payload)))
	binary.LittleEndian.PutUint64(b[12:20], tx)
	copy(b[headerSize:], payload)
	binary.LittleEndian.PutUint32(b[20:24], crc32.Checksum(b[headerSize:], castagnoli))
	return b
}
func (l *Log) append(kind uint16, tx uint64, payload []byte) error {
	if len(payload) > maxRecord {
		return errors.New("WAL record too large")
	}
	b := encode(kind, tx, payload)
	if _, e := l.f.Seek(0, io.SeekEnd); e != nil {
		return e
	}
	if _, e := l.f.Write(b); e != nil {
		return e
	}
	return l.f.Sync()
}
func (l *Log) AppendIntent(i Intent) (uint32, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(i.VolumeID) > 65535 {
		return 0, errors.New("volume id too long")
	}
	p := make([]byte, 2+len(i.VolumeID)+8+8+4+len(i.Payload))
	binary.LittleEndian.PutUint16(p[:2], uint16(len(i.VolumeID)))
	copy(p[2:], i.VolumeID)
	off := 2 + len(i.VolumeID)
	binary.LittleEndian.PutUint64(p[off:], i.Sequence)
	off += 8
	binary.LittleEndian.PutUint64(p[off:], i.OwnerEpoch)
	off += 8
	binary.LittleEndian.PutUint32(p[off:], uint32(len(i.Payload)))
	off += 4
	copy(p[off:], i.Payload)
	c := crc32.Checksum(p, castagnoli)
	return c, l.append(kindIntent, i.TxID, p)
}
func (l *Log) AppendCommit(c Commit) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	p := make([]byte, 4)
	binary.LittleEndian.PutUint32(p, c.IntentCRC)
	return l.append(kindCommit, c.TxID, p)
}
func (l *Log) Replay() ([]Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, e := l.f.Seek(0, io.SeekStart); e != nil {
		return nil, e
	}
	var out []Record
	intents := map[uint64]Intent{}
	intentCRCs := map[uint64]uint32{}
	var validEnd int64
	for {
		h := make([]byte, headerSize)
		_, e := io.ReadFull(l.f, h)
		if e == io.EOF || e == io.ErrUnexpectedEOF {
			// A torn final header is an incomplete tail, not a committed record.
			if te := l.f.Truncate(validEnd); te != nil {
				return nil, te
			}
			break
		}
		if e != nil {
			return nil, e
		}
		if binary.LittleEndian.Uint32(h[:4]) != magic || binary.LittleEndian.Uint16(h[4:6]) != version {
			return nil, ErrCorrupt
		}
		kind := binary.LittleEndian.Uint16(h[6:8])
		n := binary.LittleEndian.Uint32(h[8:12])
		tx := binary.LittleEndian.Uint64(h[12:20])
		if n > maxRecord {
			return nil, ErrCorrupt
		}
		p := make([]byte, n)
		if _, e = io.ReadFull(l.f, p); e != nil {
			if e == io.EOF || e == io.ErrUnexpectedEOF {
				if te := l.f.Truncate(validEnd); te != nil {
					return nil, te
				}
				break
			}
			return nil, ErrCorrupt
		}
		if crc32.Checksum(p, castagnoli) != binary.LittleEndian.Uint32(h[20:24]) {
			return nil, ErrCorrupt
		}
		switch kind {
		case kindIntent:
			if len(p) < 22 {
				return nil, ErrCorrupt
			}
			vl := int(binary.LittleEndian.Uint16(p[:2]))
			if len(p) < 2+vl+20 {
				return nil, ErrCorrupt
			}
			off := 2 + vl
			seq := binary.LittleEndian.Uint64(p[off:])
			off += 8
			epoch := binary.LittleEndian.Uint64(p[off:])
			off += 8
			pl := int(binary.LittleEndian.Uint32(p[off:]))
			off += 4
			if pl != len(p)-off {
				return nil, ErrCorrupt
			}
			intents[tx] = Intent{TxID: tx, VolumeID: string(p[2 : 2+vl]), Sequence: seq, OwnerEpoch: epoch, Payload: append([]byte(nil), p[off:]...)}
			intentCRCs[tx] = crc32.Checksum(p, castagnoli)
		case kindCommit:
			if len(p) != 4 {
				return nil, ErrCorrupt
			}
			want := binary.LittleEndian.Uint32(p)
			in, ok := intents[tx]
			if !ok || intentCRCs[tx] != want {
				return nil, ErrCorrupt
			}
			out = append(out, Record{Intent: &in, Commit: &Commit{TxID: tx, IntentCRC: want}})
			delete(intents, tx)
			delete(intentCRCs, tx)
		default:
			return nil, ErrCorrupt
		}
		validEnd, _ = l.f.Seek(0, io.SeekCurrent)
	}
	_, e := l.f.Seek(0, io.SeekEnd)
	return out, e
}
func (l *Log) Close() error { l.mu.Lock(); defer l.mu.Unlock(); return l.f.Close() }
