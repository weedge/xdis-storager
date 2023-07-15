package openkv

import (
	driver "github.com/weedge/pkg/driver/openkv"
)

// WriteBatch wrap driver.IWriteBatch interface op
type WriteBatch struct {
	driver.IWriteBatch
	db *DB

	batchData *BatchData
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

// BatchData the data will be undefined after commit or rollback
// get from kvstore WriteBatch Data([]byte) load(no cp slice) to BatchData,
// in order to save to log store
func (wb *WriteBatch) BatchData() *BatchData {
	data := wb.IWriteBatch.Data()
	if wb.batchData == nil {
		wb.batchData = new(BatchData)
	}

	wb.batchData.Load(data)
	return wb.batchData
}

// Data load from kvstort Data() to BatchData, then dump BatchData,return raw data []byte
// Batch data format: (kt|klen|key|vlen|value)
func (wb *WriteBatch) Data() []byte {
	b := wb.BatchData()
	return b.Data()
}
