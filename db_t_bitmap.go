package storager

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"time"

	"context"
)

type DBBitmap struct {
	*DB
	batch *Batch
}

func NewDBBitmap(db *DB) *DBBitmap {
	batch := NewBatch(db.store, db.IKV.NewWriteBatch(),
		&dbBatchLocker{
			l:      &sync.Mutex{},
			wrLock: &db.store.wLock,
		})
	return &DBBitmap{DB: db, batch: batch}
}

func (db *DBBitmap) delete(t *Batch, key []byte) (int64, error) {
	key = db.encodeBitmapKey(key)
	t.Delete(key)
	return 1, nil
}

// BitOP does the bit operations in data.
func (db *DBBitmap) BitOP(ctx context.Context, op string, destKey []byte, srcKeys ...[]byte) (int64, error) {
	if err := checkKeySize(destKey); err != nil {
		return 0, err
	}

	op = strings.ToLower(op)
	if len(srcKeys) == 0 {
		return 0, nil
	} else if op == BitNot && len(srcKeys) > 1 {
		return 0, fmt.Errorf("BITOP NOT has only one srckey")
	} else if len(srcKeys) < 2 {
		return 0, nil
	}

	key := db.encodeBitmapKey(srcKeys[0])

	value, err := db.IKV.Get(key)
	if err != nil {
		return 0, err
	}

	if op == BitNot {
		for i := 0; i < len(value); i++ {
			value[i] = ^value[i]
		}
	} else {
		for j := 1; j < len(srcKeys); j++ {
			if err := checkKeySize(srcKeys[j]); err != nil {
				return 0, err
			}

			key = db.encodeBitmapKey(srcKeys[j])
			ovalue, err := db.IKV.Get(key)
			if err != nil {
				return 0, err
			}

			if len(value) < len(ovalue) {
				value, ovalue = ovalue, value
			}

			for i := 0; i < len(ovalue); i++ {
				switch op {
				case BitAND:
					value[i] &= ovalue[i]
				case BitOR:
					value[i] |= ovalue[i]
				case BitXOR:
					value[i] ^= ovalue[i]
				default:
					return 0, fmt.Errorf("invalid op type: %s", op)
				}
			}

			for i := len(ovalue); i < len(value); i++ {
				switch op {
				case BitAND:
					value[i] &= 0
				case BitOR:
					value[i] |= 0
				case BitXOR:
					value[i] ^= 0
				}
			}
		}
	}

	key = db.encodeBitmapKey(destKey)

	t := db.batch

	t.Lock()
	defer t.Unlock()

	t.Put(key, value)

	if err := t.Commit(); err != nil {
		return 0, err
	}

	return int64(len(value)), nil
}

var bitsInByte = [256]int32{0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3,
	4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3,
	3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4,
	5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4,
	3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4,
	5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2,
	2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3,
	4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4,
	5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6,
	6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5,
	6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8}

func numberBitCount(i uint32) uint32 {
	i = i - ((i >> 1) & 0x55555555)
	i = (i & 0x33333333) + ((i >> 2) & 0x33333333)
	return (((i + (i >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24
}

// BitCount returns the bit count of data.
func (db *DBBitmap) BitCount(ctx context.Context, key []byte, start int, end int) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	key = db.encodeBitmapKey(key)
	value, err := db.IKV.Get(key)
	if err != nil {
		return 0, err
	}

	start, end = getRange(start, end, len(value))
	value = value[start : end+1]

	var n int64

	pos := 0
	for ; pos+4 <= len(value); pos = pos + 4 {
		n += int64(numberBitCount(binary.BigEndian.Uint32(value[pos : pos+4])))
	}

	for ; pos < len(value); pos++ {
		n += int64(bitsInByte[value[pos]])
	}

	return n, nil
}

// BitPos returns the pos of the data.
func (db *DBBitmap) BitPos(ctx context.Context, key []byte, on int, start int, end int) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	if (on & ^1) != 0 {
		return 0, fmt.Errorf("bit must be 0 or 1, not %d", on)
	}

	var skipValue uint8
	if on == 0 {
		skipValue = 0xFF
	}

	key = db.encodeBitmapKey(key)
	value, err := db.IKV.Get(key)
	if err != nil {
		return 0, err
	}

	start, end = getRange(start, end, len(value))
	value = value[start : end+1]

	for i, v := range value {
		if uint8(v) != skipValue {
			for j := 0; j < 8; j++ {
				isNull := uint8(v)&(1<<uint8(7-j)) == 0

				if (on == 1 && !isNull) || (on == 0 && isNull) {
					return int64((start+i)*8 + j), nil
				}
			}
		}
	}

	return -1, nil
}

// SetBit sets the bit to the data.
func (db *DBBitmap) SetBit(ctx context.Context, key []byte, offset int, on int) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	if (on & ^1) != 0 {
		return 0, fmt.Errorf("bit must be 0 or 1, not %d", on)
	}

	t := db.batch

	t.Lock()
	defer t.Unlock()

	key = db.encodeBitmapKey(key)
	value, err := db.IKV.Get(key)
	if err != nil {
		return 0, err
	}

	byteOffset := int(uint32(offset) >> 3)
	extra := byteOffset + 1 - len(value)
	if extra > 0 {
		value = append(value, make([]byte, extra)...)
	}

	byteVal := value[byteOffset]
	bit := 7 - uint8(uint32(offset)&0x7)
	bitVal := byteVal & (1 << bit)

	byteVal &= ^(1 << bit)
	byteVal |= (uint8(on&0x1) << bit)

	value[byteOffset] = byteVal

	t.Put(key, value)
	if err := t.Commit(); err != nil {
		return 0, err
	}

	if bitVal > 0 {
		return 1, nil
	}

	return 0, nil
}

// GetBit gets the bit of data at offset.
func (db *DBBitmap) GetBit(ctx context.Context, key []byte, offset int) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	key = db.encodeBitmapKey(key)

	value, err := db.IKV.Get(key)
	if err != nil {
		return 0, err
	}

	byteOffset := uint32(offset) >> 3
	bit := 7 - uint8(uint32(offset)&0x7)

	if byteOffset >= uint32(len(value)) {
		return 0, nil
	}

	bitVal := value[byteOffset] & (1 << bit)
	if bitVal > 0 {
		return 1, nil
	}

	return 0, nil
}

// Del deletes the data.
func (db *DBBitmap) Del(ctx context.Context, keys ...[]byte) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	codedKeys := make([][]byte, len(keys))
	for i, k := range keys {
		if err := checkKeySize(k); err != nil {
			return 0, err
		}
		codedKeys[i] = db.encodeBitmapKey(k)
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()

	for i, k := range keys {
		t.Delete(codedKeys[i])
		db.rmExpire(t, BitmapType, k)
	}

	err := t.Commit()
	return int64(len(keys)), err
}

// Exists check data exists or not.
func (db *DBBitmap) Exists(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	var err error
	key = db.encodeBitmapKey(key)

	var v []byte
	v, err = db.IKV.Get(key)
	if v != nil && err == nil {
		return 1, nil
	}

	return 0, err
}

// Expire expires the data.
func (db *DBBitmap) Expire(ctx context.Context, key []byte, duration int64) (int64, error) {
	if duration <= 0 {
		return 0, ErrExpireValue
	}

	return db.setExpireAt(ctx, key, time.Now().Unix()+duration)
}

// ExpireAt expires the data at when.
func (db *DBBitmap) ExpireAt(ctx context.Context, key []byte, when int64) (int64, error) {
	if when <= time.Now().Unix() {
		return 0, ErrExpireValue
	}

	return db.setExpireAt(ctx, key, when)
}

func (db *DBBitmap) setExpireAt(ctx context.Context, key []byte, when int64) (int64, error) {
	t := db.batch
	t.Lock()
	defer t.Unlock()

	if exist, err := db.Exists(ctx, key); err != nil || exist == 0 {
		return 0, err
	}

	db.expireAt(t, BitmapType, key, when)
	if err := t.Commit(); err != nil {
		return 0, err
	}

	return 1, nil
}

// TTL returns the TTL of the data.
func (db *DBBitmap) TTL(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return -1, err
	}

	return db.ttl(BitmapType, key)
}

// Persist removes the TTL of the data.
func (db *DBBitmap) Persist(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()
	n, err := db.rmExpire(t, BitmapType, key)
	if err != nil {
		return 0, err
	}

	err = t.Commit()
	return n, err
}
