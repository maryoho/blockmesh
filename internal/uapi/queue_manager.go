package uapi

import (
    "errors"
    "fmt"
    "math"
    "sync"
)

const (
    MmapQueueShift = 44
    MmapLocalMask uint64 = (1 << MmapQueueShift) - 1
    MaxQueueID uint32 = (1 << 20) - 1
)

// EncodeMmapOffset gives every Queue ID a disjoint 16TiB byte-offset window.
func EncodeMmapOffset(queueID uint32, localOffset uint64) uint64 {
    v, _ := EncodeMmapOffsetChecked(queueID, localOffset)
    return v
}
func EncodeMmapOffsetChecked(queueID uint32, localOffset uint64) (uint64, error) {
    if queueID == 0 || queueID > MaxQueueID { return 0, errors.New("queue ID outside non-zero 20-bit range") }
    if localOffset > MmapLocalMask || localOffset%PageSize != 0 { return 0, errors.New("queue-local mmap offset is unaligned or out of range") }
    return (uint64(queueID) << MmapQueueShift) | localOffset, nil
}
func DecodeMmapOffset(encoded uint64) (uint32, uint64, error) {
    id := uint32(encoded >> MmapQueueShift)
    off := encoded & MmapLocalMask
    if id == 0 || id > MaxQueueID || off%PageSize != 0 { return 0, 0, errors.New("invalid encoded mmap offset") }
    return id, off, nil
}

type QueueInfo struct {
    QueueID uint32
    TargetID string
    RingOrder uint32
    Generation uint64
    Attached bool
}
type QueueManager struct {
    mu sync.Mutex
    nextID uint32
    queues map[uint32]QueueInfo
    session uint64
}
func NewQueueManager() *QueueManager { return &QueueManager{nextID: 1, queues: make(map[uint32]QueueInfo)} }
func (m *QueueManager) Create(targetID string, ringOrder uint32) (QueueInfo, error) {
    if targetID == "" { return QueueInfo{}, errors.New("target identity is required") }
    if _, err := Layout(ringOrder); err != nil { return QueueInfo{}, err }
    m.mu.Lock(); defer m.mu.Unlock()
    if m.nextID == 0 || m.nextID > MaxQueueID { return QueueInfo{}, errors.New("queue ID space exhausted; refusing ID reuse") }
    q := QueueInfo{QueueID:m.nextID, TargetID:targetID, RingOrder:ringOrder, Generation:1}
    m.nextID++
    m.queues[q.QueueID] = q
    return q, nil
}
func (m *QueueManager) Destroy(queueID uint32, targetID string) error {
    m.mu.Lock(); defer m.mu.Unlock()
    q, ok := m.queues[queueID]
    if !ok { return errors.New("queue not found") }
    if q.TargetID != targetID { return errors.New("queue owner mismatch") }
    delete(m.queues, queueID)
    return nil
}
func (m *QueueManager) Get(queueID uint32) (QueueInfo, bool) {
    m.mu.Lock(); defer m.mu.Unlock()
    q, ok := m.queues[queueID]
    return q, ok
}
func (m *QueueManager) OpenService(session uint64) error {
    if session == 0 { return errors.New("service session must be non-zero") }
    m.mu.Lock(); defer m.mu.Unlock()
    if m.session != 0 { return errors.New("metadata service already attached") }
    for id, q := range m.queues {
        if q.Generation == math.MaxUint64 { return fmt.Errorf("queue %d generation exhausted", id) }
    }
    m.session = session
    for id, q := range m.queues { q.Generation++; q.Attached = true; m.queues[id] = q }
    return nil
}
func (m *QueueManager) CloseService(session uint64) error {
    m.mu.Lock(); defer m.mu.Unlock()
    if session == 0 || m.session != session { return errors.New("stale service session") }
    for id, q := range m.queues {
        if q.Generation == math.MaxUint64 { return fmt.Errorf("queue %d generation exhausted", id) }
    }
    m.session = 0
    for id, q := range m.queues { q.Generation++; q.Attached = false; m.queues[id] = q }
    return nil
}
