package storager

import (
	"bytes"
	"strings"
	"sync"
	"time"

	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/utils"
	"github.com/weedge/xdis-storager/openkv"
)

type DBZSet struct {
	*DB
	batch *Batch
}

func NewDBZSet(db *DB) *DBZSet {
	batch := NewBatch(db.store, db.IKVStoreDB.NewWriteBatch(),
		&dbBatchLocker{
			l:      &sync.Mutex{},
			wrLock: &db.store.wLock,
		})
	return &DBZSet{DB: db, batch: batch}
}

func checkZSetKMSize(key []byte, member []byte) error {
	if len(key) > MaxKeySize || len(key) == 0 {
		return ErrKeySize
	} else if len(member) > MaxZSetMemberSize || len(member) == 0 {
		return ErrZSetMemberSize
	}
	return nil
}

func (db *DBZSet) delete(t *Batch, key []byte) (num int64, err error) {
	num, err = db.zRemRange(t, key, MinScore, MaxScore, 0, -1)
	return
}

func (db *DBZSet) zRemRange(t *Batch, key []byte, min int64, max int64, offset int, count int) (int64, error) {
	if len(key) > MaxKeySize {
		return 0, ErrKeySize
	}

	it := db.zIterator(key, min, max, offset, count, false)
	var num int64
	for ; it.Valid(); it.Next() {
		sk := it.RawKey()
		_, m, _, err := db.zDecodeScoreKey(sk)
		if err != nil {
			continue
		}

		if n, err := db.zDelItem(t, key, m, true); err != nil {
			return 0, err
		} else if n == 1 {
			num++
		}

		t.Delete(sk)
	}
	it.Close()

	if _, err := db.zIncrSize(t, key, -num); err != nil {
		return 0, err
	}

	return num, nil
}

// Del clears multi zsets.
func (db *DBZSet) Del(keys ...[]byte) (int64, error) {
	t := db.batch
	t.Lock()
	defer t.Unlock()

	for _, key := range keys {
		if _, err := db.zRemRange(t, key, MinScore, MaxScore, 0, -1); err != nil {
			return 0, err
		}
	}

	err := t.Commit()

	return int64(len(keys)), err
}

func (db *DBZSet) zDelItem(t *Batch, key []byte, member []byte, skipDelScore bool) (int64, error) {
	ek := db.zEncodeSetKey(key, member)
	if v, err := db.IKVStoreDB.Get(ek); err != nil {
		return 0, err
	} else if v == nil {
		//not exists
		return 0, nil
	} else {
		//exists
		if !skipDelScore {
			//we must del score
			s, err := Int64(v, err)
			if err != nil {
				return 0, err
			}
			sk := db.zEncodeScoreKey(key, member, s)
			t.Delete(sk)
		}
	}

	t.Delete(ek)

	return 1, nil
}

func (db *DBZSet) zIncrSize(t *Batch, key []byte, delta int64) (int64, error) {
	sk := db.zEncodeSizeKey(key)

	size, err := Int64(db.IKVStoreDB.Get(sk))
	if err != nil {
		return 0, err
	}
	size += delta
	if size <= 0 {
		size = 0
		t.Delete(sk)
		db.rmExpire(t, ZSetType, key)
	} else {
		t.Put(sk, PutInt64(size))
	}

	return size, nil
}

func (db *DBZSet) zSetItem(t *Batch, key []byte, score int64, member []byte) (int64, error) {
	if score <= MinScore || score >= MaxScore {
		return 0, ErrScoreOverflow
	}

	var exists int64
	ek := db.zEncodeSetKey(key, member)

	if v, err := db.IKVStoreDB.Get(ek); err != nil {
		return 0, err
	} else if v != nil {
		exists = 1

		s, err := Int64(v, err)
		if err != nil {
			return 0, err
		}

		sk := db.zEncodeScoreKey(key, member, s)
		t.Delete(sk)
	}

	t.Put(ek, PutInt64(score))

	sk := db.zEncodeScoreKey(key, member, score)
	t.Put(sk, []byte{})

	return exists, nil
}

// ZAdd add the members.
func (db *DBZSet) ZAdd(key []byte, args ...driver.ScorePair) (int64, error) {
	if len(args) == 0 {
		return 0, nil
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()

	var num int64
	for i := 0; i < len(args); i++ {
		score := args[i].Score
		member := args[i].Member

		if err := checkZSetKMSize(key, member); err != nil {
			return 0, err
		}

		if n, err := db.zSetItem(t, key, score, member); err != nil {
			return 0, err
		} else if n == 0 {
			//add new
			num++
		}
	}

	if _, err := db.zIncrSize(t, key, num); err != nil {
		return 0, err
	}

	err := t.Commit()
	return num, err
}

// ZCard gets the size of the zset.
func (db *DBZSet) ZCard(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	sk := db.zEncodeSizeKey(key)
	return Int64(db.IKVStoreDB.Get(sk))
}

// ZScore gets the score of member.
func (db *DBZSet) ZScore(key []byte, member []byte) (int64, error) {
	if err := checkZSetKMSize(key, member); err != nil {
		return InvalidScore, err
	}

	score := InvalidScore

	k := db.zEncodeSetKey(key, member)
	if v, err := db.IKVStoreDB.Get(k); err != nil {
		return InvalidScore, err
	} else if v == nil {
		return InvalidScore, ErrScoreMiss
	} else {
		if score, err = Int64(v, nil); err != nil {
			return InvalidScore, err
		}
	}

	return score, nil
}

// ZRem removes members
func (db *DBZSet) ZRem(key []byte, members ...[]byte) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()

	var num int64
	for i := 0; i < len(members); i++ {
		if err := checkZSetKMSize(key, members[i]); err != nil {
			return 0, err
		}

		if n, err := db.zDelItem(t, key, members[i], false); err != nil {
			return 0, err
		} else if n == 1 {
			num++
		}
	}

	if _, err := db.zIncrSize(t, key, -num); err != nil {
		return 0, err
	}

	err := t.Commit()
	return num, err
}

// ZIncrBy increases the score of member with delta.
func (db *DBZSet) ZIncrBy(key []byte, delta int64, member []byte) (int64, error) {
	if err := checkZSetKMSize(key, member); err != nil {
		return InvalidScore, err
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()

	ek := db.zEncodeSetKey(key, member)

	var oldScore int64
	v, err := db.IKVStoreDB.Get(ek)
	if err != nil {
		return InvalidScore, err
	} else if v == nil {
		db.zIncrSize(t, key, 1)
	} else {
		if oldScore, err = Int64(v, err); err != nil {
			return InvalidScore, err
		}
	}

	newScore := oldScore + delta
	if newScore >= MaxScore || newScore <= MinScore {
		return InvalidScore, ErrScoreOverflow
	}

	sk := db.zEncodeScoreKey(key, member, newScore)
	t.Put(sk, []byte{})
	t.Put(ek, PutInt64(newScore))

	if v != nil {
		// so as to update score, we must delete the old one
		oldSk := db.zEncodeScoreKey(key, member, oldScore)
		t.Delete(oldSk)
	}

	err = t.Commit()
	return newScore, err
}

// ZCount gets the number of score in [min, max]
func (db *DBZSet) ZCount(key []byte, min int64, max int64) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	minKey := db.zEncodeStartScoreKey(key, min)
	maxKey := db.zEncodeStopScoreKey(key, max)

	rangeType := driver.RangeROpen

	it := db.IKVStoreDB.RangeLimitIterator(minKey, maxKey, rangeType, 0, -1)
	var n int64
	for ; it.Valid(); it.Next() {
		n++
	}
	it.Close()

	return n, nil
}

func (db *DBZSet) zrank(key []byte, member []byte, reverse bool) (int64, error) {
	if err := checkZSetKMSize(key, member); err != nil {
		return 0, err
	}

	k := db.zEncodeSetKey(key, member)

	it := db.IKVStoreDB.NewIterator()
	defer it.Close()

	v := it.Find(k)
	if v == nil {
		return -1, nil
	}

	s, err := Int64(v, nil)
	if err != nil {
		return 0, err
	}
	var rit *openkv.RangeLimitIterator

	sk := db.zEncodeScoreKey(key, member, s)

	if !reverse {
		minKey := db.zEncodeStartScoreKey(key, MinScore)

		rit = openkv.NewRangeIterator(it, &openkv.Range{Min: minKey, Max: sk, Type: driver.RangeClose})
	} else {
		maxKey := db.zEncodeStopScoreKey(key, MaxScore)
		rit = openkv.NewRevRangeIterator(it, &openkv.Range{Min: sk, Max: maxKey, Type: driver.RangeClose})
	}

	var lastKey []byte
	var n int64

	for ; rit.Valid(); rit.Next() {
		n++

		lastKey = rit.BufKey(lastKey)
	}

	if _, m, _, err := db.zDecodeScoreKey(lastKey); err == nil && bytes.Equal(m, member) {
		n--
		return n, nil
	}

	return -1, nil
}

func (db *DBZSet) zIterator(key []byte, min int64, max int64, offset int, count int, reverse bool) *openkv.RangeLimitIterator {
	minKey := db.zEncodeStartScoreKey(key, min)
	maxKey := db.zEncodeStopScoreKey(key, max)

	if !reverse {
		return db.IKVStoreDB.RangeLimitIterator(minKey, maxKey, driver.RangeClose, offset, count)
	}
	return db.IKVStoreDB.RevRangeLimitIterator(minKey, maxKey, driver.RangeClose, offset, count)
}

func (db *DBZSet) zRange(key []byte, min int64, max int64, offset int, count int, reverse bool) ([]driver.ScorePair, error) {
	if len(key) > MaxKeySize {
		return nil, ErrKeySize
	}

	if offset < 0 {
		return []driver.ScorePair{}, nil
	}

	nv := count
	// count may be very large, so we must limit it for below mem make.
	if nv <= 0 || nv > 1024 {
		nv = 64
	}

	v := make([]driver.ScorePair, 0, nv)

	var it *openkv.RangeLimitIterator

	//if reverse and offset is 0, count < 0, we may use forward iterator then reverse
	//because store iterator prev is slower than next
	if !reverse || (offset == 0 && count < 0) {
		it = db.zIterator(key, min, max, offset, count, false)
	} else {
		it = db.zIterator(key, min, max, offset, count, true)
	}

	for ; it.Valid(); it.Next() {
		_, m, s, err := db.zDecodeScoreKey(it.Key())
		//may be we will check key equal?
		if err != nil {
			continue
		}

		v = append(v, driver.ScorePair{Member: m, Score: s})
	}
	it.Close()

	if reverse && (offset == 0 && count < 0) {
		for i, j := 0, len(v)-1; i < j; i, j = i+1, j-1 {
			v[i], v[j] = v[j], v[i]
		}
	}

	return v, nil
}

func (db *DBZSet) zParseLimit(key []byte, start int, stop int) (offset int, count int, err error) {
	if start < 0 || stop < 0 {
		//refer redis implementation
		var size int64
		size, err = db.ZCard(key)
		if err != nil {
			return
		}

		llen := int(size)

		if start < 0 {
			start = llen + start
		}
		if stop < 0 {
			stop = llen + stop
		}

		if start < 0 {
			start = 0
		}

		if start >= llen {
			offset = -1
			return
		}
	}

	if start > stop {
		offset = -1
		return
	}

	offset = start
	count = (stop - start) + 1
	return
}

// ZClear clears the zset.
func (db *DBZSet) ZClear(key []byte) (int64, error) {
	t := db.batch
	t.Lock()
	defer t.Unlock()

	rmCnt, err := db.zRemRange(t, key, MinScore, MaxScore, 0, -1)
	if err == nil {
		err = t.Commit()
	}

	return rmCnt, err
}

// ZMclear clears multi zsets.
func (db *DBZSet) ZMclear(keys ...[]byte) (int64, error) {
	t := db.batch
	t.Lock()
	defer t.Unlock()

	for _, key := range keys {
		if _, err := db.zRemRange(t, key, MinScore, MaxScore, 0, -1); err != nil {
			return 0, err
		}
	}

	err := t.Commit()

	return int64(len(keys)), err
}

// ZRange gets the members from start to stop.
func (db *DBZSet) ZRange(key []byte, start int, stop int) ([]driver.ScorePair, error) {
	return db.ZRangeGeneric(key, start, stop, false)
}

// ZRangeByScore gets the data with score in min and max.
// min and max must be inclusive
// if no limit, set offset = 0 and count = -1
func (db *DBZSet) ZRangeByScore(key []byte, min int64, max int64,
	offset int, count int) ([]driver.ScorePair, error) {
	return db.ZRangeByScoreGeneric(key, min, max, offset, count, false)
}

// ZRank gets the rank of member.
func (db *DBZSet) ZRank(key []byte, member []byte) (int64, error) {
	return db.zrank(key, member, false)
}

// ZRemRangeByRank removes the member at range from start to stop.
func (db *DBZSet) ZRemRangeByRank(key []byte, start int, stop int) (int64, error) {
	offset, count, err := db.zParseLimit(key, start, stop)
	if err != nil {
		return 0, err
	}

	var rmCnt int64

	t := db.batch
	t.Lock()
	defer t.Unlock()

	rmCnt, err = db.zRemRange(t, key, MinScore, MaxScore, offset, count)
	if err == nil {
		err = t.Commit()
	}

	return rmCnt, err
}

// ZRemRangeByScore removes the data with score at [min, max]
func (db *DBZSet) ZRemRangeByScore(key []byte, min int64, max int64) (int64, error) {
	t := db.batch
	t.Lock()
	defer t.Unlock()

	rmCnt, err := db.zRemRange(t, key, min, max, 0, -1)
	if err == nil {
		err = t.Commit()
	}

	return rmCnt, err
}

// ZRevRange gets the data reversed.
func (db *DBZSet) ZRevRange(key []byte, start int, stop int) ([]driver.ScorePair, error) {
	return db.ZRangeGeneric(key, start, stop, true)
}

// ZRevRank gets the rank of member reversed.
func (db *DBZSet) ZRevRank(key []byte, member []byte) (int64, error) {
	return db.zrank(key, member, true)
}

// ZRevRangeByScore gets the data with score at [min, max]
// min and max must be inclusive
// if no limit, set offset = 0 and count = -1
func (db *DBZSet) ZRevRangeByScore(key []byte, min int64, max int64, offset int, count int) ([]driver.ScorePair, error) {
	return db.ZRangeByScoreGeneric(key, min, max, offset, count, true)
}

// ZRangeGeneric is a generic function for scan zset.
func (db *DBZSet) ZRangeGeneric(key []byte, start int, stop int, reverse bool) ([]driver.ScorePair, error) {
	offset, count, err := db.zParseLimit(key, start, stop)
	if err != nil {
		return nil, err
	}

	return db.zRange(key, MinScore, MaxScore, offset, count, reverse)
}

// ZRangeByScoreGeneric is a generic function to scan zset with score.
// min and max must be inclusive
// if no limit, set offset = 0 and count = -1
func (db *DBZSet) ZRangeByScoreGeneric(key []byte, min int64, max int64,
	offset int, count int, reverse bool) ([]driver.ScorePair, error) {

	return db.zRange(key, min, max, offset, count, reverse)
}

// Expire expires the zset.
func (db *DBZSet) Expire(key []byte, duration int64) (int64, error) {
	if duration <= 0 {
		return 0, ErrExpireValue
	}

	return db.zExpireAt(key, time.Now().Unix()+duration)
}

// ExpireAt expires the zset at when.
func (db *DBZSet) ExpireAt(key []byte, when int64) (int64, error) {
	if when <= time.Now().Unix() {
		return 0, ErrExpireValue
	}

	return db.zExpireAt(key, when)
}

// TTL gets the TTL of zset.
func (db *DBZSet) TTL(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return -1, err
	}

	return db.ttl(ZSetType, key)
}

// Persist removes the TTL of zset.
func (db *DBZSet) Persist(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()

	n, err := db.rmExpire(t, ZSetType, key)
	if err != nil {
		return 0, err
	}

	err = t.Commit()
	return n, err
}

func getAggregateFunc(aggregate []byte) func(int64, int64) int64 {
	aggr := strings.ToLower(utils.Bytes2String(aggregate))
	switch aggr {
	case AggregateSum:
		return func(a int64, b int64) int64 {
			return a + b
		}
	case AggregateMax:
		return func(a int64, b int64) int64 {
			if a > b {
				return a
			}
			return b
		}
	case AggregateMin:
		return func(a int64, b int64) int64 {
			if a > b {
				return b
			}
			return a
		}
	}
	return nil
}

// ZUnionStore unions the zsets and stores to dest zset.
func (db *DBZSet) ZUnionStore(destKey []byte, srcKeys [][]byte, weights []int64, aggregate []byte) (int64, error) {

	var destMap = map[string]int64{}
	aggregateFunc := getAggregateFunc(aggregate)
	if aggregateFunc == nil {
		return 0, ErrInvalidAggregate
	}
	if len(srcKeys) < 1 {
		return 0, ErrInvalidSrcKeyNum
	}
	if weights != nil {
		if len(srcKeys) != len(weights) {
			return 0, ErrInvalidWeightNum
		}
	} else {
		weights = make([]int64, len(srcKeys))
		for i := 0; i < len(weights); i++ {
			weights[i] = 1
		}
	}

	for i, key := range srcKeys {
		scorePairs, err := db.ZRange(key, 0, -1)
		if err != nil {
			return 0, err
		}
		for _, pair := range scorePairs {
			if score, ok := destMap[Bytes2String(pair.Member)]; !ok {
				destMap[Bytes2String(pair.Member)] = pair.Score * weights[i]
			} else {
				destMap[Bytes2String(pair.Member)] = aggregateFunc(score, pair.Score*weights[i])
			}
		}
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()

	db.delete(t, destKey)

	for member, score := range destMap {
		if err := checkZSetKMSize(destKey, []byte(member)); err != nil {
			return 0, err
		}

		if _, err := db.zSetItem(t, destKey, score, []byte(member)); err != nil {
			return 0, err
		}
	}

	var n = int64(len(destMap))
	sk := db.zEncodeSizeKey(destKey)
	t.Put(sk, PutInt64(n))

	if err := t.Commit(); err != nil {
		return 0, err
	}
	return n, nil
}

// ZInterStore intersects the zsets and stores to dest zset.
func (db *DBZSet) ZInterStore(destKey []byte, srcKeys [][]byte, weights []int64, aggregate []byte) (int64, error) {
	aggregateFunc := getAggregateFunc(aggregate)
	if aggregateFunc == nil {
		return 0, ErrInvalidAggregate
	}
	if len(srcKeys) < 1 {
		return 0, ErrInvalidSrcKeyNum
	}
	if weights != nil {
		if len(srcKeys) != len(weights) {
			return 0, ErrInvalidWeightNum
		}
	} else {
		weights = make([]int64, len(srcKeys))
		for i := 0; i < len(weights); i++ {
			weights[i] = 1
		}
	}

	var destMap = map[string]int64{}
	scorePairs, err := db.ZRange(srcKeys[0], 0, -1)
	if err != nil {
		return 0, err
	}
	for _, pair := range scorePairs {
		destMap[Bytes2String(pair.Member)] = pair.Score * weights[0]
	}

	for i, key := range srcKeys[1:] {
		scorePairs, err := db.ZRange(key, 0, -1)
		if err != nil {
			return 0, err
		}
		tmpMap := map[string]int64{}
		for _, pair := range scorePairs {
			if score, ok := destMap[Bytes2String(pair.Member)]; ok {
				tmpMap[Bytes2String(pair.Member)] = aggregateFunc(score, pair.Score*weights[i+1])
			}
		}
		destMap = tmpMap
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()

	db.delete(t, destKey)

	for member, score := range destMap {
		if err := checkZSetKMSize(destKey, []byte(member)); err != nil {
			return 0, err
		}
		if _, err := db.zSetItem(t, destKey, score, []byte(member)); err != nil {
			return 0, err
		}
	}

	n := int64(len(destMap))
	sk := db.zEncodeSizeKey(destKey)
	t.Put(sk, PutInt64(n))

	if err := t.Commit(); err != nil {
		return 0, err
	}
	return n, nil
}

// ZRangeByLex scans the zset lexicographically
func (db *DBZSet) ZRangeByLex(key []byte, min []byte, max []byte, rangeType driver.RangeType, offset int, count int) ([][]byte, error) {
	if min == nil {
		min = db.zEncodeStartSetKey(key)
	} else {
		min = db.zEncodeSetKey(key, min)
	}
	if max == nil {
		max = db.zEncodeStopSetKey(key)
	} else {
		max = db.zEncodeSetKey(key, max)
	}

	it := db.IKVStoreDB.RangeLimitIterator(min, max, rangeType, offset, count)
	defer it.Close()

	ay := make([][]byte, 0, 16)
	for ; it.Valid(); it.Next() {
		if _, m, err := db.zDecodeSetKey(it.Key()); err == nil {
			ay = append(ay, m)
		}
	}

	return ay, nil
}

// ZRemRangeByLex remvoes members in [min, max] lexicographically
func (db *DBZSet) ZRemRangeByLex(key []byte, min []byte, max []byte, rangeType driver.RangeType) (int64, error) {
	if min == nil {
		min = db.zEncodeStartSetKey(key)
	} else {
		min = db.zEncodeSetKey(key, min)
	}
	if max == nil {
		max = db.zEncodeStopSetKey(key)
	} else {
		max = db.zEncodeSetKey(key, max)
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()

	it := db.IKVStoreDB.RangeIterator(min, max, rangeType)
	defer it.Close()

	var n int64
	for ; it.Valid(); it.Next() {
		t.Delete(it.RawKey())
		n++
	}

	if err := t.Commit(); err != nil {
		return 0, err
	}

	return n, nil
}

// ZLexCount gets the count of zset lexicographically.
func (db *DBZSet) ZLexCount(key []byte, min []byte, max []byte, rangeType driver.RangeType) (int64, error) {
	if min == nil {
		min = db.zEncodeStartSetKey(key)
	} else {
		min = db.zEncodeSetKey(key, min)
	}
	if max == nil {
		max = db.zEncodeStopSetKey(key)
	} else {
		max = db.zEncodeSetKey(key, max)
	}

	it := db.IKVStoreDB.RangeIterator(min, max, rangeType)
	defer it.Close()

	var n int64
	for ; it.Valid(); it.Next() {
		n++
	}

	return n, nil
}

// Exists checks zset existed or not.
func (db *DBZSet) Exists(key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	sk := db.zEncodeSizeKey(key)
	v, err := db.IKVStoreDB.Get(sk)
	if v != nil && err == nil {
		return 1, nil
	}
	return 0, err
}

func (db *DBZSet) zExpireAt(key []byte, when int64) (int64, error) {
	t := db.batch
	t.Lock()
	defer t.Unlock()

	if zcnt, err := db.ZCard(key); err != nil || zcnt == 0 {
		return 0, err
	}

	db.expireAt(t, ZSetType, key, when)
	if err := t.Commit(); err != nil {
		return 0, err
	}

	return 1, nil
}
