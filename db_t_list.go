package storager

import (
	"context"
	"encoding/binary"
	"log"
	"sync"
	"time"

	"github.com/weedge/xdis-storager/openkv"
)

func MinInt32(a int32, b int32) int32 {
	if a > b {
		return b
	} else {
		return a
	}
}

func MaxInt32(a int32, b int32) int32 {
	if a > b {
		return a
	} else {
		return b
	}
}

type DBList struct {
	*DB
	batch  *Batch
	lbKeys *lBlockKeys
}

func NewDBList(db *DB) *DBList {
	batch := NewBatch(db.store, db.IKVStoreDB.NewWriteBatch(),
		&dbBatchLocker{
			l:      &sync.Mutex{},
			wrLock: &db.store.wLock,
		})

	return &DBList{DB: db, batch: batch, lbKeys: newLBlockKeys()}
}

func (db *DBList) delete(t *Batch, key []byte) (num int64, err error) {
	it := db.IKVStoreDB.NewIterator()
	defer it.Close()

	mk := db.lEncodeMetaKey(key)
	headSeq, tailSeq, _, err := db.lGetMeta(it, mk)
	if err != nil {
		return
	}

	startKey := db.lEncodeListKey(key, headSeq)
	stopKey := db.lEncodeListKey(key, tailSeq)

	rit := openkv.NewRangeIterator(it, &openkv.Range{
		Min:  startKey,
		Max:  stopKey,
		Type: openkv.RangeClose})
	for ; rit.Valid(); rit.Next() {
		t.Delete(rit.RawKey())
		num++
	}

	t.Delete(mk)

	return num, nil
}

func (db *DBList) lSetMeta(ek []byte, headSeq int32, tailSeq int32) int32 {
	t := db.batch

	size := tailSeq - headSeq + 1
	if size < 0 {
		log.Panicf("invalid meta sequence range [%d, %d]", headSeq, tailSeq)
	} else if size == 0 {
		t.Delete(ek)
	} else {
		buf := make([]byte, 8)

		binary.LittleEndian.PutUint32(buf[0:4], uint32(headSeq))
		binary.LittleEndian.PutUint32(buf[4:8], uint32(tailSeq))

		t.Put(ek, buf)
	}

	return size
}

func (db *DBList) lGetMeta(it *openkv.Iterator, ek []byte) (headSeq int32, tailSeq int32, size int32, err error) {
	var v []byte
	if it != nil {
		v = it.Find(ek)
	} else {
		v, err = db.IKVStoreDB.Get(ek)
	}
	if err != nil {
		return
	} else if v == nil {
		headSeq = listInitialSeq
		tailSeq = listInitialSeq
		size = 0
		return
	} else {
		headSeq = int32(binary.LittleEndian.Uint32(v[0:4]))
		tailSeq = int32(binary.LittleEndian.Uint32(v[4:8]))
		size = tailSeq - headSeq + 1
	}
	return
}

func (db *DBList) lSignalAsReady(key []byte) {
	db.lbKeys.signal(key)
}

func (db *DBList) lpush(key []byte, whereSeq int32, args ...[]byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	var headSeq int32
	var tailSeq int32
	var size int32
	var err error

	t := db.batch
	t.Lock()
	defer t.Unlock()

	metaKey := db.lEncodeMetaKey(key)
	headSeq, tailSeq, size, err = db.lGetMeta(nil, metaKey)
	if err != nil {
		return 0, err
	}

	pushCnt := len(args)
	if pushCnt == 0 {
		return int64(size), nil
	}

	seq := headSeq
	var delta int32 = -1
	if whereSeq == listTailSeq {
		seq = tailSeq
		delta = 1
	}

	//	append elements
	if size > 0 {
		seq += delta
	}

	for i := 0; i < pushCnt; i++ {
		ek := db.lEncodeListKey(key, seq+int32(i)*delta)
		t.Put(ek, args[i])
	}

	seq += int32(pushCnt-1) * delta
	if seq <= listMinSeq || seq >= listMaxSeq {
		return 0, ErrListSeq
	}

	//	set meta info
	if whereSeq == listHeadSeq {
		headSeq = seq
	} else {
		tailSeq = seq
	}

	db.lSetMeta(metaKey, headSeq, tailSeq)

	err = t.Commit()

	if err == nil {
		db.lSignalAsReady(key)
	}

	return int64(size) + int64(pushCnt), err
}

func (db *DBList) lpop(key []byte, whereSeq int32) ([]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()

	var headSeq int32
	var tailSeq int32
	var size int32
	var err error

	metaKey := db.lEncodeMetaKey(key)
	headSeq, tailSeq, size, err = db.lGetMeta(nil, metaKey)
	if err != nil {
		return nil, err
	} else if size == 0 {
		return nil, nil
	}

	var value []byte

	seq := headSeq
	if whereSeq == listTailSeq {
		seq = tailSeq
	}

	itemKey := db.lEncodeListKey(key, seq)
	value, err = db.IKVStoreDB.Get(itemKey)
	if err != nil {
		return nil, err
	}

	if whereSeq == listHeadSeq {
		headSeq++
	} else {
		tailSeq--
	}

	t.Delete(itemKey)
	size = db.lSetMeta(metaKey, headSeq, tailSeq)
	if size == 0 {
		db.rmExpire(t, ListType, key)
	}

	err = t.Commit()
	return value, err
}

func (db *DBList) ltrim2(key []byte, startP, stopP int64) (err error) {
	if err := checkKeySize(key); err != nil {
		return err
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()

	var headSeq int32
	var llen int32
	start := int32(startP)
	stop := int32(stopP)

	ek := db.lEncodeMetaKey(key)
	if headSeq, _, llen, err = db.lGetMeta(nil, ek); err != nil {
		return err
	}

	if start < 0 {
		start = llen + start
	}
	if stop < 0 {
		stop = llen + stop
	}
	if start >= llen || start > stop {
		db.delete(t, key)
		db.rmExpire(t, ListType, key)
		return t.Commit()
	}

	if start < 0 {
		start = 0
	}
	if stop >= llen {
		stop = llen - 1
	}

	if start > 0 {
		for i := int32(0); i < start; i++ {
			t.Delete(db.lEncodeListKey(key, headSeq+i))
		}
	}
	if stop < int32(llen-1) {
		for i := int32(stop + 1); i < llen; i++ {
			t.Delete(db.lEncodeListKey(key, headSeq+i))
		}
	}

	db.lSetMeta(ek, headSeq+start, headSeq+stop)

	return t.Commit()
}

func (db *DBList) ltrim(key []byte, trimSize, whereSeq int32) (int32, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	if trimSize == 0 {
		return 0, nil
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()

	var headSeq int32
	var tailSeq int32
	var size int32
	var err error

	metaKey := db.lEncodeMetaKey(key)
	headSeq, tailSeq, size, err = db.lGetMeta(nil, metaKey)
	if err != nil {
		return 0, err
	} else if size == 0 {
		return 0, nil
	}

	var (
		trimStartSeq int32
		trimEndSeq   int32
	)

	if whereSeq == listHeadSeq {
		trimStartSeq = headSeq
		trimEndSeq = MinInt32(trimStartSeq+trimSize-1, tailSeq)
		headSeq = trimEndSeq + 1
	} else {
		trimEndSeq = tailSeq
		trimStartSeq = MaxInt32(trimEndSeq-trimSize+1, headSeq)
		tailSeq = trimStartSeq - 1
	}

	for trimSeq := trimStartSeq; trimSeq <= trimEndSeq; trimSeq++ {
		itemKey := db.lEncodeListKey(key, trimSeq)
		t.Delete(itemKey)
	}

	size = db.lSetMeta(metaKey, headSeq, tailSeq)
	if size == 0 {
		db.rmExpire(t, ListType, key)
	}

	err = t.Commit()
	return trimEndSeq - trimStartSeq + 1, err
}

// LIndex returns the value at index.
func (db *DBList) LIndex(key []byte, index int32) ([]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	var seq int32
	var headSeq int32
	var tailSeq int32
	var err error

	metaKey := db.lEncodeMetaKey(key)

	it := db.IKVStoreDB.NewIterator()
	defer it.Close()

	headSeq, tailSeq, _, err = db.lGetMeta(it, metaKey)
	if err != nil {
		return nil, err
	}

	if index >= 0 {
		seq = headSeq + index
	} else {
		seq = tailSeq + index + 1
	}

	sk := db.lEncodeListKey(key, seq)
	v := it.Find(sk)

	return v, nil
}

// LLen gets the length of the list.
func (db *DBList) LLen(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	ek := db.lEncodeMetaKey(key)
	_, _, size, err := db.lGetMeta(nil, ek)
	return int64(size), err
}

// LPop pops the value.
func (db *DBList) LPop(key []byte) ([]byte, error) {
	return db.lpop(key, listHeadSeq)
}

// LTrim trims the value from start to stop.
func (db *DBList) LTrim(key []byte, start, stop int64) error {
	return db.ltrim2(key, start, stop)
}

// LTrimFront trims the value from top.
func (db *DBList) LTrimFront(key []byte, trimSize int32) (int32, error) {
	return db.ltrim(key, trimSize, listHeadSeq)
}

// LTrimBack trims the value from back.
func (db *DBList) LTrimBack(key []byte, trimSize int32) (int32, error) {
	return db.ltrim(key, trimSize, listTailSeq)
}

// LPush push the value to the list.
func (db *DBList) LPush(key []byte, args ...[]byte) (int64, error) {
	return db.lpush(key, listHeadSeq, args...)
}

// LSet sets the value at index.
func (db *DBList) LSet(key []byte, index int32, value []byte) error {
	if err := checkKeySize(key); err != nil {
		return err
	}

	var seq int32
	var headSeq int32
	var tailSeq int32
	//var size int32
	var err error
	t := db.batch
	t.Lock()
	defer t.Unlock()
	metaKey := db.lEncodeMetaKey(key)

	headSeq, tailSeq, _, err = db.lGetMeta(nil, metaKey)
	if err != nil {
		return err
	}

	if index >= 0 {
		seq = headSeq + index
	} else {
		seq = tailSeq + index + 1
	}
	if seq < headSeq || seq > tailSeq {
		return ErrListIndex
	}
	sk := db.lEncodeListKey(key, seq)
	t.Put(sk, value)
	err = t.Commit()
	return err
}

// LRange gets the value of list at range.
func (db *DBList) LRange(key []byte, start int32, stop int32) ([][]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	var headSeq int32
	var llen int32
	var err error

	metaKey := db.lEncodeMetaKey(key)

	it := db.IKVStoreDB.NewIterator()
	defer it.Close()

	if headSeq, _, llen, err = db.lGetMeta(it, metaKey); err != nil {
		return nil, err
	}

	if start < 0 {
		start = llen + start
	}
	if stop < 0 {
		stop = llen + stop
	}
	if start < 0 {
		start = 0
	}

	if start > stop || start >= llen {
		return [][]byte{}, nil
	}

	if stop >= llen {
		stop = llen - 1
	}

	limit := (stop - start) + 1
	headSeq += start

	v := make([][]byte, 0, limit)

	startKey := db.lEncodeListKey(key, headSeq)
	rit := openkv.NewRangeLimitIterator(it,
		&openkv.Range{
			Min:  startKey,
			Max:  nil,
			Type: openkv.RangeClose},
		&openkv.Limit{
			Offset: 0,
			Count:  int(limit)})

	for ; rit.Valid(); rit.Next() {
		v = append(v, rit.Value())
	}

	return v, nil
}

// RPop rpops the value.
func (db *DBList) RPop(key []byte) ([]byte, error) {
	return db.lpop(key, listTailSeq)
}

// RPush rpushs the value .
func (db *DBList) RPush(key []byte, args ...[]byte) (int64, error) {
	return db.lpush(key, listTailSeq, args...)
}

// BLPop pops the list with block way.
func (db *DBList) BLPop(keys [][]byte, timeout time.Duration) ([]interface{}, error) {
	return db.lblockPop(keys, listHeadSeq, timeout)
}

// BRPop bpops the list with block way.
func (db *DBList) BRPop(keys [][]byte, timeout time.Duration) ([]interface{}, error) {
	return db.lblockPop(keys, listTailSeq, timeout)
}

func (db *DBList) lblockPop(keys [][]byte, whereSeq int32, timeout time.Duration) ([]interface{}, error) {
	for {
		var ctx context.Context
		var cancel context.CancelFunc
		if timeout > 0 {
			ctx, cancel = context.WithTimeout(context.Background(), timeout)
		} else {
			ctx, cancel = context.WithCancel(context.Background())
		}

		for _, key := range keys {
			v, err := db.lbKeys.popOrWait(db, key, whereSeq, cancel)

			if err != nil {
				cancel()
				return nil, err
			} else if v != nil {
				cancel()
				return []interface{}{key, v}, nil
			}
		}

		//blocking wait
		<-ctx.Done()
		cancel()

		//if ctx.Err() is a deadline exceeded (timeout) we return
		//otherwise we try to pop one of the keys again.
		if ctx.Err() == context.DeadlineExceeded {
			return nil, nil
		}
	}
}

// Del clears multi lists.
func (db *DBList) Del(keys ...[]byte) (int64, error) {
	t := db.batch
	t.Lock()
	defer t.Unlock()

	for _, key := range keys {
		if err := checkKeySize(key); err != nil {
			return 0, err
		}

		db.delete(t, key)
		db.rmExpire(t, ListType, key)

	}

	err := t.Commit()
	return int64(len(keys)), err
}

func (db *DBList) lExpireAt(key []byte, when int64) (int64, error) {

	t := db.batch
	t.Lock()
	defer t.Unlock()

	if llen, err := db.LLen(key); err != nil || llen == 0 {
		return 0, err
	}

	db.expireAt(t, ListType, key, when)
	if err := t.Commit(); err != nil {
		return 0, err
	}

	return 1, nil
}

// Expire expires the list.
func (db *DBList) Expire(key []byte, duration int64) (int64, error) {
	if duration <= 0 {
		return 0, ErrExpireValue
	}

	return db.lExpireAt(key, time.Now().Unix()+duration)
}

// ExpireAt expires the list at when.
func (db *DBList) ExpireAt(key []byte, when int64) (int64, error) {
	if when <= time.Now().Unix() {
		return 0, ErrExpireValue
	}

	return db.lExpireAt(key, when)
}

// TTL gets the TTL of list.
func (db *DBList) TTL(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return -1, err
	}

	return db.ttl(ListType, key)
}

// Persist removes the TTL of list.
func (db *DBList) Persist(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()

	n, err := db.rmExpire(t, ListType, key)
	if err != nil {
		return 0, err
	}

	err = t.Commit()
	return n, err
}

// Exists check list existed or not.
func (db *DBList) Exists(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	sk := db.lEncodeMetaKey(key)
	v, err := db.IKVStoreDB.Get(sk)
	if v != nil && err == nil {
		return 1, nil
	}
	return 0, err
}
