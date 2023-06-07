package storager

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/utils"
)

type DBHash struct {
	*DB
	batch *Batch
}

func NewDBHash(db *DB) *DBHash {
	batch := NewBatch(db.store, db.IKV.NewWriteBatch(),
		&dbBatchLocker{
			l:      &sync.Mutex{},
			wrLock: &db.store.wLock,
		})
	return &DBHash{DB: db, batch: batch}
}

func checkHashKFSize(ctx context.Context, key []byte, field []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return ErrKeySize
	} else if len(field) > MaxHashFieldSize || len(field) == 0 {
		return ErrHashFieldSize
	}
	return nil
}

func (db *DBHash) delete(t *Batch, key []byte) (num int64, err error) {
	sk := db.hEncodeSizeKey(key)
	start := db.hEncodeStartKey(key)
	stop := db.hEncodeStopKey(key)

	it := db.IKV.RangeLimitIterator(start, stop, driver.RangeROpen, 0, -1)
	for ; it.Valid(); it.Next() {
		t.Delete(it.Key())
		num++
	}
	it.Close()
	t.Delete(sk)

	return num, nil
}

// Del cleans multi hash data.
func (db *DBHash) Del(ctx context.Context, keys ...[]byte) (int64, error) {
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

	for _, key := range keys {
		db.delete(t, key)
		db.rmExpire(t, HashType, key)
	}

	err := t.Commit()
	return int64(len(keys)), err
}

func (db *DBHash) hSetItem(ctx context.Context, key []byte, field []byte, value []byte) (int64, error) {
	t := db.batch

	ek := db.hEncodeHashKey(key, field)

	var n int64 = 1
	if v, _ := db.IKV.Get(ek); v != nil {
		n = 0
	} else {
		if _, err := db.hIncrSize(ctx, key, 1); err != nil {
			return 0, err
		}
	}

	t.Put(ek, value)
	return n, nil
}

func (db *DBHash) hIncrSize(ctx context.Context, key []byte, delta int64) (int64, error) {
	t := db.batch
	sk := db.hEncodeSizeKey(key)

	var err error
	var size int64
	if size, err = Int64(db.IKV.Get(sk)); err != nil {
		return 0, err
	}

	size += delta
	if size <= 0 {
		size = 0
		t.Delete(sk)
		db.rmExpire(t, HashType, key)
	} else {
		t.Put(sk, PutInt64(size))
	}

	return size, nil
}

// HLen returns the lengh of hash.
func (db *DBHash) HLen(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	return Int64(db.IKV.Get(db.hEncodeSizeKey(key)))
}

// uHSet sets the field with value of key.
func (db *DBHash) HSet(ctx context.Context, key []byte, field []byte, value []byte) (int64, error) {
	if err := checkHashKFSize(ctx, key, field); err != nil {
		return 0, err
	} else if err := checkValueSize(value); err != nil {
		return 0, err
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()

	n, err := db.hSetItem(ctx, key, field, value)
	if err != nil {
		return 0, err
	}

	err = t.Commit()
	return n, err
}

// HGet gets the value of the field.
func (db *DBHash) HGet(ctx context.Context, key []byte, field []byte) ([]byte, error) {
	if err := checkHashKFSize(ctx, key, field); err != nil {
		return nil, err
	}

	return db.IKV.Get(db.hEncodeHashKey(key, field))
}

// HMset sets multi field-values.
func (db *DBHash) HMset(ctx context.Context, key []byte, args ...driver.FVPair) error {
	t := db.batch
	t.Lock()
	defer t.Unlock()

	var num int64
	for i := 0; i < len(args); i++ {
		if err := checkHashKFSize(ctx, key, args[i].Field); err != nil {
			return err
		} else if err := checkValueSize(args[i].Value); err != nil {
			return err
		}

		ek := db.hEncodeHashKey(key, args[i].Field)

		if v, err := db.IKV.Get(ek); err != nil {
			return err
		} else if v == nil {
			num++
		}

		t.Put(ek, args[i].Value)
	}

	if _, err := db.hIncrSize(ctx, key, num); err != nil {
		return err
	}

	err := t.Commit()
	return err
}

// HMget gets multi values of fields
func (db *DBHash) HMget(ctx context.Context, key []byte, args ...[]byte) ([][]byte, error) {
	var ek []byte

	it := db.IKV.NewIterator()
	defer it.Close()

	r := make([][]byte, len(args))
	for i := 0; i < len(args); i++ {
		if err := checkHashKFSize(ctx, key, args[i]); err != nil {
			return nil, err
		}

		ek = db.hEncodeHashKey(key, args[i])

		r[i] = it.Find(ek)
	}

	return r, nil
}

// HDel deletes the fields.
func (db *DBHash) HDel(ctx context.Context, key []byte, args ...[]byte) (int64, error) {
	t := db.batch

	var ek []byte
	var v []byte
	var err error

	t.Lock()
	defer t.Unlock()

	it := db.IKV.NewIterator()
	defer it.Close()

	var num int64
	for i := 0; i < len(args); i++ {
		if err := checkHashKFSize(ctx, key, args[i]); err != nil {
			return 0, err
		}

		ek = db.hEncodeHashKey(key, args[i])

		v = it.RawFind(ek)
		if v == nil {
			continue
		} else {
			num++
			t.Delete(ek)
		}
	}

	if _, err = db.hIncrSize(ctx, key, -num); err != nil {
		return 0, err
	}

	err = t.Commit()

	return num, err
}

// HIncrBy increases the value of field by delta.
func (db *DBHash) HIncrBy(ctx context.Context, key []byte, field []byte, delta int64) (int64, error) {
	if err := checkHashKFSize(ctx, key, field); err != nil {
		return 0, err
	}

	t := db.batch
	var ek []byte
	var err error

	t.Lock()
	defer t.Unlock()

	ek = db.hEncodeHashKey(key, field)

	var n int64
	if n, err = utils.StrInt64(db.IKV.Get(ek)); err != nil {
		return 0, err
	}

	n += delta

	_, err = db.hSetItem(ctx, key, field, strconv.AppendInt(nil, n, 10))
	if err != nil {
		return 0, err
	}

	err = t.Commit()

	return n, err
}

// HGetAll returns all field-values.
func (db *DBHash) HGetAll(ctx context.Context, key []byte) ([]driver.FVPair, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	start := db.hEncodeStartKey(key)
	stop := db.hEncodeStopKey(key)

	v := make([]driver.FVPair, 0, 16)

	it := db.IKV.RangeLimitIterator(start, stop, driver.RangeROpen, 0, -1)
	defer it.Close()

	for ; it.Valid(); it.Next() {
		_, f, err := db.hDecodeHashKey(it.Key())
		if err != nil {
			return nil, err
		}

		v = append(v, driver.FVPair{Field: f, Value: it.Value()})
	}

	return v, nil
}

// HKeys returns the all fields.
func (db *DBHash) HKeys(ctx context.Context, key []byte) ([][]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	start := db.hEncodeStartKey(key)
	stop := db.hEncodeStopKey(key)

	v := make([][]byte, 0, 16)

	it := db.IKV.RangeLimitIterator(start, stop, driver.RangeROpen, 0, -1)
	defer it.Close()

	for ; it.Valid(); it.Next() {
		_, f, err := db.hDecodeHashKey(it.Key())
		if err != nil {
			return nil, err
		}
		v = append(v, f)
	}

	return v, nil
}

// HValues returns all values
func (db *DBHash) HValues(ctx context.Context, key []byte) ([][]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	start := db.hEncodeStartKey(key)
	stop := db.hEncodeStopKey(key)

	v := make([][]byte, 0, 16)

	it := db.IKV.RangeLimitIterator(start, stop, driver.RangeROpen, 0, -1)
	defer it.Close()

	for ; it.Valid(); it.Next() {
		_, _, err := db.hDecodeHashKey(it.Key())
		if err != nil {
			return nil, err
		}

		v = append(v, it.Value())
	}

	return v, nil
}

func (db *DBHash) hExpireAt(ctx context.Context, key []byte, when int64) (int64, error) {
	t := db.batch
	t.Lock()
	defer t.Unlock()

	if hlen, err := db.HLen(ctx, key); err != nil || hlen == 0 {
		return 0, err
	}

	db.expireAt(t, HashType, key, when)
	if err := t.Commit(); err != nil {
		return 0, err
	}

	return 1, nil
}

// Expire expires the data with duration.
func (db *DBHash) Expire(ctx context.Context, key []byte, duration int64) (int64, error) {
	if duration <= 0 {
		return 0, ErrExpireValue
	}

	return db.hExpireAt(ctx, key, time.Now().Unix()+duration)
}

// ExpireAt expires the data at time when.
func (db *DBHash) ExpireAt(ctx context.Context, key []byte, when int64) (int64, error) {
	if when <= time.Now().Unix() {
		return 0, ErrExpireValue
	}

	return db.hExpireAt(ctx, key, when)
}

// TTL gets the TTL of data.
func (db *DBHash) TTL(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return -1, err
	}

	return db.ttl(HashType, key)
}

// Persist removes the TTL of data.
func (db *DBHash) Persist(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()

	n, err := db.rmExpire(t, HashType, key)
	if err != nil {
		return 0, err
	}

	err = t.Commit()
	return n, err
}

// HKeyExists checks whether data exists or not.
func (db *DBHash) Exists(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	sk := db.hEncodeSizeKey(key)
	v, err := db.IKV.Get(sk)
	if v != nil && err == nil {
		return 1, nil
	}
	return 0, err
}

// Clear clears the data.
func (db *DBHash) Clear(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()

	num, err := db.delete(t, key)
	if err != nil {
		return 0, nil
	}
	db.rmExpire(t, HashType, key)

	err = t.Commit()
	return num, err
}
