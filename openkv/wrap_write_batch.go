package openkv

import (
	driver "github.com/weedge/pkg/driver/openkv"
)

// WriteBatch wrap driver.IWriteBatch interface op
type WriteBatch struct {
	driver.IWriteBatch
	db *DB
}

func (m *WriteBatch) Close() {
	m.IWriteBatch.Close()
}

func (m *WriteBatch) Put(key []byte, value []byte) {
	m.IWriteBatch.Put(key, value)
}

func (m *WriteBatch) Delete(key []byte) {
	m.IWriteBatch.Delete(key)
}

func (m *WriteBatch) Commit() (err error) {
	if m.db == nil || !m.db.needSyncCommit() {
		err = m.IWriteBatch.Commit()
	} else {
		err = m.IWriteBatch.SyncCommit()
	}

	return err
}

func (m *WriteBatch) Rollback() error {
	return m.IWriteBatch.Rollback()
}
