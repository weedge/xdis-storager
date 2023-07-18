package storager

import (
	"context"
	"fmt"
)

// FlushDB flushes the data.
func (db *DB) FlushDB(ctx context.Context) (drop int64, err error) {
	all := [](func(ctx context.Context) (int64, error)){
		db.stringFlush,
		db.listFlush,
		db.hashFlush,
		db.setFlush,
		db.zsetFlush,
	}

	for _, flush := range all {
		n, e := flush(ctx)
		if e != nil {
			err = e
			return
		}

		drop += n
	}

	return
}

func (db *DB) flushType(ctx context.Context, t *Batch, dataType byte) (drop int64, err error) {
	var deleteFunc func(t *Batch, key []byte) (int64, error)
	var metaDataType byte
	switch dataType {
	case StringType:
		deleteFunc = db.string.delete
		metaDataType = StringType
	case ListType:
		deleteFunc = db.list.delete
		metaDataType = LMetaType
	case HashType:
		deleteFunc = db.hash.delete
		metaDataType = HSizeType
	case SetType:
		deleteFunc = db.set.delete
		metaDataType = SSizeType
	case ZSetType:
		deleteFunc = db.zset.delete
		metaDataType = ZSizeType
	default:
		return 0, fmt.Errorf("invalid data type: %s", TypeName[dataType])
	}

	var keys [][]byte
	keys, err = db.scanGeneric(metaDataType, nil, 1024, false, "", false)
	for len(keys) != 0 && err == nil {
		for _, key := range keys {
			deleteFunc(t, key)
			db.rmExpire(t, dataType, key)
		}

		if err = t.Commit(ctx); err != nil {
			return
		}

		drop += int64(len(keys))
		keys, err = db.scanGeneric(metaDataType, nil, 1024, false, "", false)
	}
	return
}

func (db *DB) stringFlush(ctx context.Context) (drop int64, err error) {
	t := db.string.batch
	t.Lock()
	defer t.Unlock()
	return db.flushType(ctx, t, StringType)
}

func (db *DB) listFlush(ctx context.Context) (drop int64, err error) {
	t := db.list.batch
	t.Lock()
	defer t.Unlock()
	return db.flushType(ctx, t, ListType)
}

func (db *DB) hashFlush(ctx context.Context) (drop int64, err error) {
	t := db.hash.batch
	t.Lock()
	defer t.Unlock()
	return db.flushType(ctx, t, HashType)
}

func (db *DB) setFlush(ctx context.Context) (drop int64, err error) {
	t := db.set.batch
	t.Lock()
	defer t.Unlock()

	return db.flushType(ctx, t, SetType)
}

func (db *DB) zsetFlush(ctx context.Context) (drop int64, err error) {
	t := db.zset.batch
	t.Lock()
	defer t.Unlock()
	return db.flushType(ctx, t, ZSetType)
}
