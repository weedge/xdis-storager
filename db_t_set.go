package storager

import (
	"context"
	"sync"
	"time"

	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/rdb"
	"github.com/weedge/pkg/utils"
)

type DBSet struct {
	*DB
	batch *Batch
}

func NewDBSet(db *DB) *DBSet {
	batch := NewBatch(db.store, db.IKV.NewWriteBatch(),
		&dbBatchLocker{
			l:      &sync.Mutex{},
			wrLock: &db.store.wLock,
		})
	return &DBSet{DB: db, batch: batch}
}

func checkSetKMSize(ctx context.Context, key []byte, member []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return ErrKeySize
	} else if len(member) > MaxSetMemberSize || len(member) == 0 {
		return ErrSetMemberSize
	}
	return nil
}

func (db *DBSet) delete(t *Batch, key []byte) (num int64, err error) {
	sk := db.sEncodeSizeKey(key)
	start := db.sEncodeStartKey(key)
	stop := db.sEncodeStopKey(key)

	it := db.IKV.RangeLimitIterator(start, stop, driver.RangeROpen, 0, -1)
	for ; it.Valid(); it.Next() {
		t.Delete(it.RawKey())
		num++
	}

	it.Close()
	t.Delete(sk)
	db.DelKeyMeta(t, key, SetType)

	return num, nil
}

func (db *DBSet) sIncrSize(ctx context.Context, t *Batch, key []byte, delta int64) (int64, error) {
	sk := db.sEncodeSizeKey(key)

	var err error
	var size int64
	if size, err = Int64(db.IKV.Get(sk)); err != nil {
		return 0, err
	}

	size += delta
	if size <= 0 {
		size = 0
		t.Delete(sk)
		db.rmExpire(t, SetType, key)
	} else {
		t.Put(sk, PutInt64(size))
	}

	return size, nil
}

func (db *DBSet) sExpireAt(ctx context.Context, key []byte, when int64) (int64, error) {
	t := db.batch
	t.Lock()
	defer t.Unlock()

	if scnt, err := db.SCard(ctx, key); err != nil || scnt == 0 {
		return 0, err
	}
	db.expireAt(t, SetType, key, when)
	if err := t.Commit(ctx); err != nil {
		return 0, err
	}

	return 1, nil
}

// SAdd adds the value to the set.
func (db *DBSet) SAdd(ctx context.Context, key []byte, args ...[]byte) (int64, error) {
	if len(args) == 0 {
		return 0, nil
	}
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()

	var err error
	var ek []byte
	var num int64
	for i := 0; i < len(args); i++ {
		if err := checkSetKMSize(ctx, key, args[i]); err != nil {
			return 0, err
		}

		ek = db.sEncodeSetKey(key, args[i])

		if v, err := db.IKV.Get(ek); err != nil {
			return 0, err
		} else if v == nil {
			num++
		}

		t.Put(ek, nil)
	}

	if _, err = db.sIncrSize(ctx, t, key, num); err != nil {
		return 0, err
	}

	db.SetKeyMeta(t, key, SetType)
	err = t.Commit(ctx)
	return num, err

}

// SCard gets the size of set.
func (db *DBSet) SCard(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	sk := db.sEncodeSizeKey(key)

	return Int64(db.IKV.Get(sk))
}

func (db *DBSet) sDiffGeneric(ctx context.Context, keys ...[]byte) ([][]byte, error) {
	destMap := make(map[string]bool)

	members, err := db.SMembers(ctx, keys[0])
	if err != nil {
		return nil, err
	}

	for _, m := range members {
		destMap[utils.Bytes2String(m)] = true
	}

	for _, k := range keys[1:] {
		members, err := db.SMembers(ctx, k)
		if err != nil {
			return nil, err
		}

		for _, m := range members {
			if _, ok := destMap[utils.Bytes2String(m)]; !ok {
				continue
			} else if ok {
				delete(destMap, utils.Bytes2String(m))
			}
		}
		// O - A = O, O is zero set.
		if len(destMap) == 0 {
			return nil, nil
		}
	}

	slice := make([][]byte, len(destMap))
	idx := 0
	for _, m := range members {
		if _, ok := destMap[utils.Bytes2String(m)]; ok {
			slice[idx] = m
			idx++
		}
	}

	return slice, nil
}

// SDiff gets the different of sets.
func (db *DBSet) SDiff(ctx context.Context, keys ...[]byte) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	return db.sDiffGeneric(ctx, keys...)
}

// SDiffStore gets the different of sets and stores to dest set.
func (db *DBSet) SDiffStore(ctx context.Context, dstKey []byte, keys ...[]byte) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	return db.sStoreGeneric(ctx, dstKey, DiffType, keys...)
}

// SKeyExists checks whether set existed or not.
func (db *DBSet) SKeyExists(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	sk := db.sEncodeSizeKey(key)
	v, err := db.IKV.Get(sk)
	if v != nil && err == nil {
		return 1, nil
	}
	return 0, err
}

func (db *DBSet) sInterGeneric(ctx context.Context, keys ...[]byte) ([][]byte, error) {
	destMap := make(map[string]bool)

	members, err := db.SMembers(ctx, keys[0])
	if err != nil {
		return nil, err
	}

	for _, m := range members {
		destMap[utils.Bytes2String(m)] = true
	}

	for _, key := range keys[1:] {
		if err := checkKeySize(key); err != nil {
			return nil, err
		}

		members, err := db.SMembers(ctx, key)
		if err != nil {
			return nil, err
		} else if len(members) == 0 {
			return nil, err
		}

		tempMap := make(map[string]bool)
		for _, member := range members {
			if err := checkKeySize(member); err != nil {
				return nil, err
			}
			if _, ok := destMap[utils.Bytes2String(member)]; ok {
				tempMap[utils.Bytes2String(member)] = true //mark this item as selected
			}
		}
		destMap = tempMap //reduce the size of the result set
		if len(destMap) == 0 {
			return nil, nil
		}
	}

	slice := make([][]byte, len(destMap))
	idx := 0
	for _, m := range members {
		if _, ok := destMap[utils.Bytes2String(m)]; ok {
			slice[idx] = m
			idx++
		}
	}

	return slice, nil

}

// SInter intersects the sets.
func (db *DBSet) SInter(ctx context.Context, keys ...[]byte) ([][]byte, error) {
	v, err := db.sInterGeneric(ctx, keys...)
	return v, err

}

// SInterStore intersects the sets and stores to dest set.
func (db *DBSet) SInterStore(ctx context.Context, dstKey []byte, keys ...[]byte) (int64, error) {
	n, err := db.sStoreGeneric(ctx, dstKey, InterType, keys...)
	return n, err
}

// SIsMember checks member in set.
func (db *DBSet) SIsMember(ctx context.Context, key []byte, member []byte) (int64, error) {
	ek := db.sEncodeSetKey(key, member)

	var n int64 = 1
	if v, err := db.IKV.Get(ek); err != nil {
		return 0, err
	} else if v == nil {
		n = 0
	}
	return n, nil
}

// SMembers gets members of set.
func (db *DBSet) SMembers(ctx context.Context, key []byte) ([][]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	start := db.sEncodeStartKey(key)
	stop := db.sEncodeStopKey(key)

	v := make([][]byte, 0, 16)

	it := db.IKV.RangeLimitIterator(start, stop, driver.RangeROpen, 0, -1)
	defer it.Close()

	for ; it.Valid(); it.Next() {
		_, m, err := db.sDecodeSetKey(it.Key())
		if err != nil {
			return nil, err
		}

		v = append(v, m)
	}

	return v, nil
}

// SRem removes the members of set.
func (db *DBSet) SRem(ctx context.Context, key []byte, args ...[]byte) (int64, error) {
	t := db.batch
	t.Lock()
	defer t.Unlock()

	var ek []byte
	var v []byte
	var err error

	it := db.IKV.NewIterator()
	defer it.Close()

	var num int64
	for i := 0; i < len(args); i++ {
		if err := checkSetKMSize(ctx, key, args[i]); err != nil {
			return 0, err
		}

		ek = db.sEncodeSetKey(key, args[i])

		v = it.RawFind(ek)
		if v == nil {
			continue
		} else {
			num++
			t.Delete(ek)
		}
	}

	if _, err = db.sIncrSize(ctx, t, key, -num); err != nil {
		return 0, err
	}

	err = t.Commit(ctx)
	return num, err

}

func (db *DBSet) sUnionGeneric(ctx context.Context, keys ...[]byte) ([][]byte, error) {
	dstMap := make(map[string]bool)
	slice := [][]byte{}

	for _, key := range keys {
		if err := checkKeySize(key); err != nil {
			return nil, err
		}

		members, err := db.SMembers(ctx, key)
		if err != nil {
			return nil, err
		}

		for _, member := range members {
			if _, ok := dstMap[utils.Bytes2String(member)]; !ok {
				slice = append(slice, member)
			}
			dstMap[utils.Bytes2String(member)] = true
		}
	}

	return slice, nil
}

// SUnion unions the sets.
func (db *DBSet) SUnion(ctx context.Context, keys ...[]byte) ([][]byte, error) {
	v, err := db.sUnionGeneric(ctx, keys...)
	return v, err
}

// SUnionStore unions the sets and stores to the dest set.
func (db *DBSet) SUnionStore(ctx context.Context, dstKey []byte, keys ...[]byte) (int64, error) {
	n, err := db.sStoreGeneric(ctx, dstKey, UnionType, keys...)
	return n, err
}

func (db *DBSet) sStoreGeneric(ctx context.Context, dstKey []byte, optType byte, keys ...[]byte) (int64, error) {
	if err := checkKeySize(dstKey); err != nil {
		return 0, err
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()

	var err error
	var ek []byte
	var v [][]byte

	switch optType {
	case UnionType:
		v, err = db.sUnionGeneric(ctx, keys...)
	case DiffType:
		v, err = db.sDiffGeneric(ctx, keys...)
	case InterType:
		v, err = db.sInterGeneric(ctx, keys...)
	}
	if err != nil {
		return 0, err
	}

	db.delete(t, dstKey)

	for _, m := range v {
		if err := checkSetKMSize(ctx, dstKey, m); err != nil {
			return 0, err
		}

		ek = db.sEncodeSetKey(dstKey, m)

		if _, err := db.IKV.Get(ek); err != nil {
			return 0, err
		}

		t.Put(ek, nil)
	}

	var n = int64(len(v))
	sk := db.sEncodeSizeKey(dstKey)
	t.Put(sk, PutInt64(n))
	db.SetKeyMeta(t, dstKey, SetType)

	if err = t.Commit(ctx); err != nil {
		return 0, err
	}
	return n, nil
}

// Del clears multi sets.
func (db *DBSet) Del(ctx context.Context, keys ...[]byte) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	for _, key := range keys {
		if err := checkKeySize(key); err != nil {
			return 0, err
		}
	}
	t := db.batch
	t.Lock()
	defer t.Unlock()

	nums := 0
	for _, key := range keys {
		if n, err := db.delete(t, key); err == nil && n > 0 {
			nums++
		}
		db.rmExpire(t, SetType, key)
	}

	err := t.Commit(ctx)
	return int64(nums), err
}

// Exists checks whether set existed or not.
func (db *DBSet) Exists(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	sk := db.sEncodeSizeKey(key)
	v, err := db.IKV.Get(sk)
	if v != nil && err == nil {
		return 1, nil
	}
	return 0, err
}

// Expire expires the set.
func (db *DBSet) Expire(ctx context.Context, key []byte, duration int64) (int64, error) {
	if duration <= 0 {
		return 0, ErrExpireValue
	}

	return db.sExpireAt(ctx, key, time.Now().Unix()+duration)

}

// ExpireAt expires the set at when.
func (db *DBSet) ExpireAt(ctx context.Context, key []byte, when int64) (int64, error) {
	if when <= time.Now().Unix() {
		return 0, ErrExpireValue
	}

	return db.sExpireAt(ctx, key, when)

}

// TTL gets the TTL of set.
func (db *DBSet) TTL(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return -1, err
	}

	sk := db.sEncodeSizeKey(key)
	v, err := db.IKV.Get(sk)
	if err != nil {
		return -1, err
	}
	if v == nil {
		return -2, nil
	}

	return db.ttl(SetType, key)
}

// Persist removes the TTL of set.
func (db *DBSet) Persist(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()

	n, err := db.rmExpire(t, SetType, key)
	if err != nil {
		return 0, err
	}
	err = t.Commit(ctx)
	return n, err
}

// Dump set rdb
func (db *DBSet) Dump(ctx context.Context, key []byte) (binVal []byte, err error) {
	v, err := db.SMembers(ctx, key)
	if err != nil {
		return
	} else if len(v) == 0 {
		return
	}

	return rdb.DumpSetValue(v), nil
}

// Restore set rdb
func (db *DBSet) Restore(ctx context.Context, t *Batch, key []byte, ttl int64, val rdb.Set) (err error) {
	if _, err = db.BatchDel(ctx, t, key); err != nil {
		return
	}

	if _, err = db.BatchSAdd(ctx, t, key, val...); err != nil {
		return
	}

	if ttl > 0 {
		if _, err = db.BatchExpire(ctx, t, key, ttl); err != nil {
			return
		}
	}
	return
}

// BatchDel clears multi sets.
func (db *DBSet) BatchDel(ctx context.Context, t *Batch, keys ...[]byte) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	for _, key := range keys {
		if err := checkKeySize(key); err != nil {
			return 0, err
		}
	}

	nums := 0
	for _, key := range keys {
		if n, err := db.delete(t, key); err == nil && n > 0 {
			nums++
		}
		db.rmExpire(t, SetType, key)
	}

	return int64(nums), nil
}

// BatchSAdd adds the value to the set.
func (db *DBSet) BatchSAdd(ctx context.Context, t *Batch, key []byte, args ...[]byte) (int64, error) {
	if len(args) == 0 {
		return 0, nil
	}
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	var err error
	var ek []byte
	var num int64
	for i := 0; i < len(args); i++ {
		if err := checkSetKMSize(ctx, key, args[i]); err != nil {
			return 0, err
		}

		ek = db.sEncodeSetKey(key, args[i])

		if v, err := db.IKV.Get(ek); err != nil {
			return 0, err
		} else if v == nil {
			num++
		}

		t.Put(ek, nil)
	}

	if _, err = db.sIncrSize(ctx, t, key, num); err != nil {
		return 0, err
	}

	db.SetKeyMeta(t, key, SetType)

	return num, nil
}

// BatchExpire expires the set.
func (db *DBSet) BatchExpire(ctx context.Context, t *Batch, key []byte, duration int64) (int64, error) {
	if duration <= 0 {
		return 0, ErrExpireValue
	}

	when := time.Now().Unix() + duration
	db.expireAt(t, SetType, key, when)
	return 1, nil
}
