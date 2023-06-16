package storager

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/weedge/pkg/driver"
	openkvDriver "github.com/weedge/pkg/driver/openkv"
	"github.com/weedge/pkg/utils"
)

type DBString struct {
	*DB
	batch *Batch
}

func NewDBString(db *DB) *DBString {
	batch := NewBatch(db.store, db.IKV.NewWriteBatch(),
		&dbBatchLocker{
			l:      &sync.Mutex{},
			wrLock: &db.store.wLock,
		})
	return &DBString{DB: db, batch: batch}
}

func (db *DBString) delete(t *Batch, key []byte) (int64, error) {
	key = db.encodeStringKey(key)
	t.Delete(key)
	return 1, nil
}

func checkKeySize(key []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return ErrKeySize
	}
	return nil
}

func checkValueSize(value []byte) error {
	if len(value) > MaxValueSize {
		return ErrValueSize
	}
	return nil
}

// Set sets the data.
func (db *DBString) Set(ctx context.Context, key []byte, value []byte) error {
	if err := checkKeySize(key); err != nil {
		return err
	}
	if err := checkValueSize(value); err != nil {
		return err
	}

	var err error
	key = db.encodeStringKey(key)

	db.batch.Lock()
	defer db.batch.Unlock()

	db.batch.Put(key, value)

	err = db.batch.Commit()

	return err
}

// Get gets the value.
func (db *DBString) Get(ctx context.Context, key []byte) ([]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	key = db.encodeStringKey(key)

	return db.IKV.Get(key)
}

// GetSlice gets the slice of the data to adapt leveldb slice
func (db *DBString) GetSlice(ctx context.Context, key []byte) (openkvDriver.ISlice, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}

	key = db.encodeStringKey(key)

	return db.IKV.GetSlice(key)
}

// GetSet gets the value and sets new value.
func (db *DBString) GetSet(ctx context.Context, key []byte, value []byte) ([]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}
	if err := checkValueSize(value); err != nil {
		return nil, err
	}

	key = db.encodeStringKey(key)

	t := db.batch

	t.Lock()
	defer t.Unlock()

	oldValue, err := db.IKV.Get(key)
	if err != nil {
		return nil, err
	}

	t.Put(key, value)

	err = t.Commit()

	return oldValue, err
}

// Incr increases the data.
func (db *DBString) Incr(ctx context.Context, key []byte) (int64, error) {
	return db.incr(ctx, key, 1)
}

// IncrBy increases the data by increment.
func (db *DBString) IncrBy(ctx context.Context, key []byte, increment int64) (int64, error) {
	return db.incr(ctx, key, increment)
}

// Decr decreases the data.
func (db *DBString) Decr(ctx context.Context, key []byte) (int64, error) {
	return db.incr(ctx, key, -1)
}

// DecrBy decreases the data by decrement.
func (db *DBString) DecrBy(ctx context.Context, key []byte, decrement int64) (int64, error) {
	return db.incr(ctx, key, -decrement)
}

func (db *DBString) incr(ctx context.Context, key []byte, delta int64) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	var err error
	key = db.encodeStringKey(key)

	t := db.batch

	t.Lock()
	defer t.Unlock()

	var n int64
	n, err = utils.StrInt64(db.IKV.Get(key))
	if err != nil {
		return 0, ErrValueIntOutOfRange
	}

	n += delta

	t.Put(key, strconv.AppendInt(nil, n, 10))

	err = t.Commit()
	return n, err
}

// MGet gets multi data.
func (db *DBString) MGet(ctx context.Context, keys ...[]byte) ([][]byte, error) {
	values := make([][]byte, len(keys))

	it := db.IKV.NewIterator()
	defer it.Close()

	for i := range keys {
		if err := checkKeySize(keys[i]); err != nil {
			return nil, err
		}

		values[i] = it.Find(db.encodeStringKey(keys[i]))
	}

	return values, nil
}

// MSet sets multi data.
func (db *DBString) MSet(ctx context.Context, args ...driver.KVPair) error {
	if len(args) == 0 {
		return nil
	}

	t := db.batch

	var err error
	var key []byte
	var value []byte

	t.Lock()
	defer t.Unlock()

	for i := 0; i < len(args); i++ {
		if err := checkKeySize(args[i].Key); err != nil {
			return err
		} else if err := checkValueSize(args[i].Value); err != nil {
			return err
		}

		key = db.encodeStringKey(args[i].Key)

		value = args[i].Value

		t.Put(key, value)

	}

	err = t.Commit()
	return err
}

// SetNX sets the data if not existed.
func (db *DBString) SetNX(ctx context.Context, key []byte, value []byte) (n int64, err error) {
	if err = checkKeySize(key); err != nil {
		return
	}
	if err = checkValueSize(value); err != nil {
		return
	}

	key = db.encodeStringKey(key)
	n = 1

	t := db.batch
	t.Lock()
	defer t.Unlock()

	if v, err := db.IKV.Get(key); err != nil {
		return 0, err
	} else if v != nil {
		return 0, nil
	}

	t.Put(key, value)
	err = t.Commit()
	if err != nil {
		return
	}

	return
}

// SetEX sets the data with a TTL.
func (db *DBString) SetEX(ctx context.Context, key []byte, duration int64, value []byte) error {
	if err := checkKeySize(key); err != nil {
		return err
	} else if err := checkValueSize(value); err != nil {
		return err
	} else if duration <= 0 {
		return ErrExpireValue
	}

	ek := db.encodeStringKey(key)

	t := db.batch

	t.Lock()
	defer t.Unlock()

	t.Put(ek, value)
	db.expireAt(t, StringType, key, time.Now().Unix()+duration)

	return t.Commit()
}

// SetNXEX set k v nx ex seconds
// NX -- Only set the key if it does not already exist.
// EX seconds -- Set the specified expire time, in seconds.
func (db *DBString) SetNXEX(ctx context.Context, key []byte, duration int64, value []byte) (n int64, err error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	} else if err := checkValueSize(value); err != nil {
		return 0, err
	} else if duration <= 0 {
		return 0, ErrExpireValue
	}

	ek := db.encodeStringKey(key)

	t := db.batch

	t.Lock()
	defer t.Unlock()

	if v, err := db.IKV.Get(ek); err != nil {
		return 0, err
	} else if v != nil {
		return 0, nil
	}

	t.Put(ek, value)
	db.expireAt(t, StringType, key, time.Now().Unix()+duration)
	err = t.Commit()
	if err != nil {
		return
	}

	return 1, nil
}

// SetXXEX set k v xx ex seconds
// XX -- Only set the key if it already exists.
// EX seconds -- Set the specified expire time, in seconds.
func (db *DBString) SetXXEX(ctx context.Context, key []byte, duration int64, value []byte) (n int64, err error) {
	if err = checkKeySize(key); err != nil {
		return
	} else if err = checkValueSize(value); err != nil {
		return
	} else if duration <= 0 {
		return 0, ErrExpireValue
	}

	ek := db.encodeStringKey(key)

	t := db.batch

	t.Lock()
	defer t.Unlock()

	if v, err := db.IKV.Get(ek); err != nil {
		return 0, err
	} else if v == nil {
		return 0, nil
	}

	t.Put(ek, value)
	db.expireAt(t, StringType, key, time.Now().Unix()+duration)
	err = t.Commit()
	if err != nil {
		return
	}

	return 1, nil
}

// Del deletes the data.
func (db *DBString) Del(ctx context.Context, keys ...[]byte) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	codedKeys := make([][]byte, len(keys))
	for i, k := range keys {
		if err := checkKeySize(k); err != nil {
			return 0, err
		}
		codedKeys[i] = db.encodeStringKey(k)
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()

	nums := 0
	for i, k := range keys {
		v, err := db.IKV.Get(codedKeys[i])
		if err == nil && v != nil {
			nums++
		}
		t.Delete(codedKeys[i])
		db.rmExpire(t, StringType, k)
	}

	err := t.Commit()
	return int64(nums), err
}

// Exists check data exists or not.
func (db *DBString) Exists(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	var err error
	key = db.encodeStringKey(key)

	var v []byte
	v, err = db.IKV.Get(key)
	if v != nil && err == nil {
		return 1, nil
	}

	return 0, err
}

// Expire expires the data.
func (db *DBString) Expire(ctx context.Context, key []byte, duration int64) (int64, error) {
	if duration <= 0 {
		return 0, ErrExpireValue
	}

	return db.setExpireAt(ctx, key, time.Now().Unix()+duration)
}

// ExpireAt expires the data at when.
func (db *DBString) ExpireAt(ctx context.Context, key []byte, when int64) (int64, error) {
	if when <= time.Now().Unix() {
		return 0, ErrExpireValue
	}

	return db.setExpireAt(ctx, key, when)
}

func (db *DBString) setExpireAt(ctx context.Context, key []byte, when int64) (int64, error) {
	t := db.batch
	t.Lock()
	defer t.Unlock()

	if exist, err := db.Exists(ctx, key); err != nil || exist == 0 {
		return 0, err
	}

	db.expireAt(t, StringType, key, when)
	if err := t.Commit(); err != nil {
		return 0, err
	}

	return 1, nil
}

// TTL returns the TTL of the data.
func (db *DBString) TTL(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return -1, err
	}

	sk := db.encodeStringKey(key)
	v, err := db.IKV.Get(sk)
	if err != nil {
		return -1, err
	}
	if v == nil {
		return -2, nil
	}

	return db.ttl(StringType, key)
}

// Persist removes the TTL of the data.
func (db *DBString) Persist(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()
	n, err := db.rmExpire(t, StringType, key)
	if err != nil {
		return 0, err
	}

	err = t.Commit()
	return n, err
}

// SetRange sets the data with new value from offset.
func (db *DBString) SetRange(ctx context.Context, key []byte, offset int, value []byte) (int64, error) {
	if len(value) == 0 {
		return 0, nil
	}

	if err := checkKeySize(key); err != nil {
		return 0, err
	} else if len(value)+offset > MaxValueSize {
		return 0, ErrValueSize
	}

	key = db.encodeStringKey(key)

	t := db.batch

	t.Lock()
	defer t.Unlock()

	oldValue, err := db.IKV.Get(key)
	if err != nil {
		return 0, err
	}

	extra := offset + len(value) - len(oldValue)
	if extra > 0 {
		oldValue = append(oldValue, make([]byte, extra)...)
	}

	copy(oldValue[offset:], value)

	t.Put(key, oldValue)

	if err := t.Commit(); err != nil {
		return 0, err
	}

	return int64(len(oldValue)), nil
}

func getRange(start int, end int, valLen int) (int, int) {
	if start < 0 {
		start = valLen + start
	}

	if end < 0 {
		end = valLen + end
	}

	if start < 0 {
		start = 0
	}

	if end < 0 {
		end = 0
	}

	if end >= valLen {
		end = valLen - 1
	}
	return start, end
}

// GetRange gets the range of the data.
func (db *DBString) GetRange(ctx context.Context, key []byte, start int, end int) ([]byte, error) {
	if err := checkKeySize(key); err != nil {
		return nil, err
	}
	key = db.encodeStringKey(key)

	value, err := db.IKV.Get(key)
	if err != nil {
		return nil, err
	}

	valLen := len(value)

	start, end = getRange(start, end, valLen)

	if start > end {
		return nil, nil
	}

	return value[start : end+1], nil
}

// StrLen returns the length of the data.
func (db *DBString) StrLen(ctx context.Context, key []byte) (int64, error) {
	s, err := db.GetSlice(ctx, key)
	if err != nil {
		return 0, err
	}
	if s == nil {
		return 0, nil
	}

	n := s.Size()
	s.Free()
	return int64(n), nil
}

// Append appends the value to the data.
func (db *DBString) Append(ctx context.Context, key []byte, value []byte) (int64, error) {
	if len(value) == 0 {
		return 0, nil
	}

	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	key = db.encodeStringKey(key)

	t := db.batch

	t.Lock()
	defer t.Unlock()

	oldValue, err := db.IKV.Get(key)
	if err != nil {
		return 0, err
	}

	if len(oldValue)+len(value) > MaxValueSize {
		return 0, ErrValueSize
	}

	oldValue = append(oldValue, value...)

	t.Put(key, oldValue)

	if err := t.Commit(); err != nil {
		return 0, nil
	}

	return int64(len(oldValue)), nil
}
