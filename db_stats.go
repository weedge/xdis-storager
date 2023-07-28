package storager

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/weedge/pkg/driver"
)

var mapDBStats = map[int]*DBStats{}

type DBStats struct {
	KeyCn           atomic.Int64
	ExpireKeyCn     atomic.Int64
	TotalExpireTime atomic.Int64
	TotalExpireCn   atomic.Int64
}

func (m *DBStats) String() string {
	avgTTL := int64(0)
	expCn := m.ExpireKeyCn.Load()
	expTime := m.TotalExpireTime.Load()
	expTotalCn := m.TotalExpireCn.Load()
	now := time.Now().Unix()
	if expCn > 0 && expTime > now*expTotalCn {
		avgTTL = (expTime - now*expTotalCn) / expTotalCn
	}
	str := fmt.Sprintf("keys=%d,expires=%d,avg_ttl=%d",
		m.KeyCn.Load(), expCn, avgTTL)
	return str
}

func AddDBKeyCn(store *Storager, stats *DBStats, key []byte) {
	store.cbfLock.Lock()
	if !store.cbf.Test(key) {
		store.cbf.Add(key)
		stats.KeyCn.Add(1)
	}
	store.cbfLock.Unlock()
}

func RemoveDBKeyCn(store *Storager, stats *DBStats, key []byte) {
	store.cbfLock.Lock()
	if store.cbf.Test(key) {
		store.cbf.TestAndRemove(key)
		stats.KeyCn.Add(-1)
	}
	store.cbfLock.Unlock()
}

func AddDBExpKeyStats(store *Storager, stats *DBStats, mk, tk []byte, exp int64) {
	now := time.Now().Unix()
	if exp <= now {
		return
	}
	store.cbfLock.Lock()
	if !store.cbf.Test(mk) {
		store.cbf.Add(mk)
		stats.ExpireKeyCn.Add(1)
	}

	if !store.cbf.Test(tk) {
		store.cbf.Add(tk)
		stats.TotalExpireTime.Add(exp)
		stats.TotalExpireCn.Add(1)
	}
	store.cbfLock.Unlock()
}

func RemoveDBExpKeyStats(store *Storager, stats *DBStats, mk, tk []byte, exp int64) {
	store.cbfLock.Lock()
	if store.cbf.Test(mk) {
		store.cbf.TestAndRemove(mk)
		stats.ExpireKeyCn.Add(-1)
	}

	if store.cbf.Test(tk) {
		store.cbf.TestAndRemove(tk)
		stats.TotalExpireTime.Add(-exp)
		stats.TotalExpireCn.Add(-1)
	}
	store.cbfLock.Unlock()
}

func LoadDbStats(dbIndex int) (m *DBStats) {
	if data, ok := mapDBStats[dbIndex]; ok {
		return data
	}
	return
}

// InitDbStats begin init db stats load to map after storager open
func InitAllDbStats(s *Storager) {
	for i := 0; i < s.opts.Databases; i++ {
		stats := GetDbStats(s, i)
		mapDBStats[i] = stats
	}
}

func GetDbStats(s *Storager, index int) *DBStats {
	stats := &DBStats{}

	ek := encodeDbIndexKey(index, CodeTypeMeta)
	sk := encodeDbIndexEndKey(index, CodeTypeMeta)
	it := s.odb.RangeIterator(ek, sk, driver.RangeOpen)
	for ; it.Valid(); it.Next() {
		ek := it.RawKey()
		AddDBKeyCn(s, stats, ek)
	}
	it.Close()

	if stats.KeyCn.Load() == 0 {
		return stats
	}

	ek = encodeExpMetaKey(index, NoneType, nil)
	sk = encodeExpMetaKey(index, maxDataType, nil)
	it = s.odb.RangeIterator(ek, sk, driver.RangeOpen)
	for ; it.Valid(); it.Next() {
		mk := it.RawKey()
		dataType, key, err := decodeExpMetaKey(index, mk)
		if err != nil {
			continue
		}
		val := it.RawValue()
		when, err := Int64(val, nil)
		if err != nil {
			continue
		}
		tk := encodeExpTimeKey(index, dataType, key, when)
		AddDBExpKeyStats(s, stats, mk, tk, when)
	}
	it.Close()

	return stats
}

func DBHasKey(s *Storager, index int) bool {
	ek := encodeDbIndexKey(index, CodeTypeMeta)
	it := s.odb.NewIterator()
	if it.Seek(ek); it.Valid() {
		return true
	}
	it.Close()

	return false
}
