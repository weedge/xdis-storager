package openkv

import (
	"fmt"

	driver "github.com/weedge/pkg/driver/openkv"
	"github.com/weedge/pkg/utils"
)

// WriteBatch wrap driver.IWriteBatch interface op
type WriteBatch struct {
	driver.IWriteBatch
	db *DB

	batchData *BatchData

	// buffer to commit (use list push/pop), not just for one type struct,
	// can batch to commit (one i/o)
	batchOpBuff *utils.BatchOpBuffer
}

func (m *WriteBatch) Close() {
	m.IWriteBatch.Close()
}

func (m *WriteBatch) Put(key []byte, value []byte) {
	if m.db.opts.BufferOpCommit {
		m.batchOpBuff.Put(key, value)
	} else {
		m.IWriteBatch.Put(key, value)
	}
}

func (m *WriteBatch) Delete(key []byte) {
	if m.db.opts.BufferOpCommit {
		m.batchOpBuff.Del(key)
	} else {
		m.IWriteBatch.Delete(key)
	}
}

func (m *WriteBatch) Commit() (err error) {
	if !m.db.opts.BufferOpCommit {
		return m.commit()
	}

	if m.batchOpBuff.Len() == 0 {
		return nil
	}
	for e := m.batchOpBuff.FrontElement(); e != nil; e = e.Next() {
		item := e.Value.(*utils.BatchOp)
		switch item.Type {
		case utils.BatchOpTypePut:
			m.IWriteBatch.Put(item.Key, item.Value)
		case utils.BatchOpTypeDel:
			m.IWriteBatch.Delete(item.Key)
		default:
			panic(fmt.Sprintf("unsupported batch operation: %+v", item))
		}
	}
	err = m.commit()

	return
}

func (m *WriteBatch) commit() (err error) {
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
