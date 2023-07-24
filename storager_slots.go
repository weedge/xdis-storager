package storager

import (
	"bytes"
	"context"
	"hash/crc32"
	"time"

	"github.com/weedge/pkg/driver"
)

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

func (m *Storager) HashTagToSlot(tag []byte) uint32 {
	return crc32.ChecksumIEEE(tag) % uint32(m.opts.Slots)
}

func (m *Storager) HashKeyToSlot(key []byte) ([]byte, uint32) {
	tag := HashTag(key)
	return tag, m.HashTagToSlot(tag)
}

// SlotsHashKey hash keys to slots, return slot slice
func (m *Storager) SlotsHashKey(ctx context.Context, keys ...[]byte) ([]uint64, error) {
	if m.opts.Slots < 0 {
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
func (m *Storager) MigrateSlotOneKey(ctx context.Context, addr string, timeout time.Duration, slot uint64) (int64, error)

// MigrateSlotOneKey migrate slot keys/vals  which have the same tag with one key to addr with timeout (ms)
// return 1, success, 0 slot is empty
func (m *Storager) MigrateSlotOneKeyWithSameTag(ctx context.Context, addr string, timeout time.Duration, slot uint64) (int64, error)

// MigrateOneKey migrate one key/val to addr with timeout (ms)
// return 1, success, 0 slot is empty
func (m *Storager) MigrateOneKey(ctx context.Context, addr string, timeout time.Duration, key []byte) (int64, error)

// MigrateKeyWithSameTag migrate keys/vals which have the same tag with one key to addr with timeout (ms)
// return n, n migrate success, 0 slot is empty
func (m *Storager) MigrateKeyWithSameTag(ctx context.Context, addr string, timeout time.Duration, key []byte) (int64, error)

// SlotsRestore dest migrate addr restore slot obj [key ttlms value ...]
func (m *Storager) SlotsRestore(ctx context.Context, objs ...driver.SlotsRestoreObj) error

// SlotsInfo show slot info with slots range [start,start+count]
// return slotInfo slice
func (m *Storager) SlotsInfo(ctx context.Context, startSlot, count uint64) ([]driver.SlotInfo, error)

// SlotsDel del slots, return after del slot info
func (m *Storager) SlotsDel(ctx context.Context, keys ...[]byte) ([]driver.SlotInfo, error)

// SlotsCheck slots  must check below case
// - The key stored in each slot can find the corresponding val in the db
// - Keys in each db can be found in the corresponding slot
// WARNING: just used debug/test, don't use in product,
func (m *Storager) SlotsCheck(ctx context.Context) error
