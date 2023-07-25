package storager

import (
	"bytes"
	"context"
	"hash/crc32"
	"sync"
	"time"

	"github.com/weedge/pkg/driver"
)

func (m *DB) SetKeyMeta(t *Batch, key []byte, dataType byte) {
	ek := m.encodeDbIndexSlotTagKey(key)
	t.Put(ek, []byte{dataType})
}

func (m *DB) DelKeyMeta(t *Batch, key []byte, dataType byte) {
	ek := m.encodeDbIndexSlotTagKey(key)
	t.Delete(ek)
}

// HashTag like redis cluster hash tag
func HashTag(key []byte) []byte {
	part := key
	if i := bytes.IndexByte(part, '{'); i != -1 {
		part = part[i+1:]
	} else {
		return key
	}
	if i := bytes.IndexByte(part, '}'); i != -1 {
		return part[:i]
	} else {
		return key
	}
}

type DBSlot struct {
	*DB
	batch *Batch
}

func NewDBSlot(db *DB) *DBSlot {
	batch := NewBatch(db.store, db.IKV.NewWriteBatch(),
		&dbBatchLocker{
			l:      &sync.Mutex{},
			wrLock: &db.store.wLock,
		})
	return &DBSlot{DB: db, batch: batch}
}

func (m *DBSlot) HashTagToSlot(tag []byte) uint32 {
	return crc32.ChecksumIEEE(tag) % uint32(m.store.opts.Slots)
}

func (m *DBSlot) HashKeyToSlot(key []byte) ([]byte, uint32) {
	tag := HashTag(key)
	return tag, m.HashTagToSlot(tag)
}

// SlotsHashKey hash keys to slots, return slot slice
func (m *DBSlot) SlotsHashKey(ctx context.Context, keys ...[]byte) ([]uint64, error) {
	if m.store.opts.Slots <= 0 {
		return nil, ErrUnsupportSlots
	}

	slots := make([]uint64, 0, len(keys))
	for _, key := range keys {
		_, slot := m.HashKeyToSlot(key)
		slots = append(slots, uint64(slot))
	}

	return slots, nil
}

// MigrateSlotOneKey migrate slot one key/val to addr with timeout (ms)
// return 1, success, 0 slot is empty
func (m *DBSlot) MigrateSlotOneKey(ctx context.Context, addr string, timeout time.Duration, slot uint64) (migrateCn int64, err error) {

	return
}

// MigrateSlotKeyWithSameTag migrate slot keys/vals  which have the same tag with one key to addr with timeout (ms)
// return n, success, 0 slot is empty
func (m *DBSlot) MigrateSlotKeyWithSameTag(ctx context.Context, addr string, timeout time.Duration, slot uint64) (migrateCn int64, err error) {
	return
}

// MigrateOneKey migrate one key/val to addr with timeout (ms)
// return 1, success, 0 slot is empty
func (m *DBSlot) MigrateOneKey(ctx context.Context, addr string, timeout time.Duration, key []byte) (migrateCn int64, err error) {
	return
}

// MigrateKeyWithSameTag migrate keys/vals which have the same tag with one key to addr with timeout (ms)
// return n, n migrate success, 0 slot is empty
func (m *DBSlot) MigrateKeyWithSameTag(ctx context.Context, addr string, timeout time.Duration, key []byte) (migrateCn int64, err error) {

	return
}

// SlotsRestore dest migrate addr restore slot obj [key ttlms value ...]
func (m *DBSlot) SlotsRestore(ctx context.Context, objs ...*driver.SlotsRestoreObj) (err error) {
	return
}

// SlotsInfo show slot info with slots range [start,start+count]
// return slotInfo slice
func (m *DBSlot) SlotsInfo(ctx context.Context, startSlot, count uint64) (slotInfos []driver.SlotInfo, err error) {
	return
}

// SlotsDel del slots, return after del slot info
func (m *DBSlot) SlotsDel(ctx context.Context, slots ...uint64) (slotInfos []driver.SlotInfo, err error) {
	return
}

// SlotsCheck slots  must check below case
// - The key stored in each slot can find the corresponding val in the db
// - Keys in each db can be found in the corresponding slot
// WARNING: just used debug/test, don't use in product,
func (m *DBSlot) SlotsCheck(ctx context.Context) (err error) {
	return
}
