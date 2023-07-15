package openkv

import "github.com/syndtr/goleveldb/leveldb"

/*
	see leveldb batch data format for more information
	keyType(kt): Put/Delete
	[]batchIndex (keyType|keyPos,keyLen|valuePos,valueLen)...
	[]data (kt|klen|key|vlen|value)...
*/

// BatchData use it(levedb.Batch) just for data kvStore WriteBatch to commit from log store data ,
// don't use put/delete key,val again for WriteBatch to commit when load from Batch Data to WriteBatch commit
// load raw data([]byte) to BatchData
// dump BatchData to raw data([]byte)
// replay BatchData to WriteBatch impl which use Put/Delete op
type BatchData struct {
	leveldb.Batch
}

func NewBatchData(data []byte) (*BatchData, error) {
	b := new(BatchData)

	if err := b.Load(data); err != nil {
		return nil, err
	}

	return b, nil
}

// Data dump BatchData format []data (kt|klen|key|vlen|value)... -> raw data([]byte)
func (d *BatchData) Data() []byte {
	return d.Dump()
}

// Reset reset batch data
func (d *BatchData) Reset() {
	d.Batch.Reset()
}

// Replay batch data for leveldb.BatchReplay impl put/delete op
// like relay log to replay some w(insert/update/delete) op (CDC)
func (d *BatchData) Replay(impl leveldb.BatchReplay) error {
	return d.Batch.Replay(impl)
}

type BatchReplayItem struct {
	key   []byte
	value []byte
}

type batchReplayItems []BatchReplayItem

func (bs *batchReplayItems) Put(key, value []byte) {
	*bs = append(*bs, BatchReplayItem{key, value})
}

func (bs *batchReplayItems) Delete(key []byte) {
	*bs = append(*bs, BatchReplayItem{key, nil})
}

func (d *BatchData) ReplayItems() ([]BatchReplayItem, error) {
	is := make(batchReplayItems, 0, d.Len())

	if err := d.Replay(&is); err != nil {
		return nil, err
	}

	return []BatchReplayItem(is), nil
}
