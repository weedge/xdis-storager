package openkv

import (
	"sync"
	"time"

	"github.com/weedge/pkg/driver"
	openkvDriver "github.com/weedge/pkg/driver/openkv"
	"github.com/weedge/pkg/utils"
	"github.com/weedge/xdis-storager/config"
)

// DB wrap driver.IDB interface op
// impl IKV
type DB struct {
	// store common conf
	opts *config.StorgerOptions
	// kv op interface
	openkvDriver.IDB
	// kv store engine name
	name string

	// last commit time for needSyncCommit
	lastCommit time.Time
	mu         sync.Mutex
}

func (m *DB) Close() error {
	if utils.IsNil(m.IDB) {
		return nil
	}
	return m.IDB.Close()
}

func (m *DB) String() string {
	return m.name
}

func (m *DB) NewIterator() *Iterator {
	it := &Iterator{
		IIterator: m.IDB.NewIterator(),
	}

	return it
}

func (db *DB) Get(key []byte) ([]byte, error) {
	v, err := db.IDB.Get(key)
	return v, err
}

func (db *DB) Put(key []byte, value []byte) error {
	if db.needSyncCommit() {
		return db.IDB.SyncPut(key, value)
	}
	return db.IDB.Put(key, value)
}

func (db *DB) Delete(key []byte) error {
	if db.needSyncCommit() {
		return db.IDB.SyncDelete(key)
	}
	return db.IDB.Delete(key)
}

func (db *DB) NewWriteBatch() *WriteBatch {
	wb := new(WriteBatch)
	wb.IWriteBatch = db.IDB.NewWriteBatch()
	wb.db = db
	return wb
}

func (db *DB) NewSnapshot() (*Snapshot, error) {
	var err error
	s := &Snapshot{}
	if s.ISnapshot, err = db.IDB.NewSnapshot(); err != nil {
		return nil, err
	}

	return s, nil
}

func (db *DB) Compact() error {
	err := db.IDB.Compact()

	return err
}

// needSyncCommit
// Sync commit to disk if possible
//
//	0: no sync
//	1: sync every second
//	2: sync every commit
func (m *DB) needSyncCommit() (need bool) {
	if m.opts.DBSyncCommit == 0 {
		return false
	}
	if m.opts.DBSyncCommit == 2 {
		return true
	}

	n := time.Now()
	m.mu.Lock()
	if n.Sub(m.lastCommit) > time.Second {
		need = true
	}
	m.lastCommit = n
	m.mu.Unlock()

	return
}

// for db range Iterator

// RangeIterator
func (db *DB) RangeIterator(min []byte, max []byte, rangeType driver.RangeType) *RangeLimitIterator {
	return NewRangeLimitIterator(db.NewIterator(), &Range{min, max, rangeType}, &Limit{0, -1})
}

// RevRangeIterator
func (db *DB) RevRangeIterator(min []byte, max []byte, rangeType driver.RangeType) *RangeLimitIterator {
	return NewRevRangeLimitIterator(db.NewIterator(), &Range{min, max, rangeType}, &Limit{0, -1})
}

// RangeLimitIterator count < 0, unlimit.
// offset must >= 0, if < 0, will get nothing.
func (db *DB) RangeLimitIterator(min []byte, max []byte, rangeType driver.RangeType, offset int, count int) *RangeLimitIterator {
	return NewRangeLimitIterator(db.NewIterator(), &Range{min, max, rangeType}, &Limit{offset, count})
}

// RevRangeLimitIterator count < 0, unlimit.
// offset must >= 0, if < 0, will get nothing.
func (db *DB) RevRangeLimitIterator(min []byte, max []byte, rangeType driver.RangeType, offset int, count int) *RangeLimitIterator {
	return NewRevRangeLimitIterator(db.NewIterator(), &Range{min, max, rangeType}, &Limit{offset, count})
}

// GetSlice wrap to adapte diff language kv store get op
func (db *DB) GetSlice(key []byte) (openkvDriver.ISlice, error) {
	if d, ok := db.IDB.(openkvDriver.ISliceGeter); ok {
		v, err := d.GetSlice(key)
		return v, err
	}
	v, err := db.Get(key)
	if err != nil {
		return nil, err
	} else if v == nil {
		return nil, nil
	}
	return openkvDriver.GoSlice(v), nil
}
