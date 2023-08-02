package storager

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"time"

	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/rdb"
	"github.com/weedge/pkg/utils"
	"github.com/weedge/xdis-storager/openkv"
)

type DBZSet struct {
	*DB
	batch *Batch
}

func NewDBZSet(db *DB) *DBZSet {
	batch := NewBatch(db.store, db.IKV.NewWriteBatch(),
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
	sizeKey := db.zEncodeSizeKey(key)
	t.Delete(sizeKey)
	db.DelKeyMeta(t, key, ZSetType)
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

		if n, err := db.zDelItem(t, key, m, false); err != nil {
			return 0, err
		} else if n == 1 {
			num++
		}
	}
	it.Close()

	return num, nil
}

// Del clears multi zsets.
func (db *DBZSet) Del(ctx context.Context, keys ...[]byte) (int64, error) {
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
		n, err := db.delete(t, key)
		if err != nil {
			return 0, err
		}
		if n > 0 {
			nums++
		}
	}

	err := t.Commit(ctx)
	return int64(nums), err
}

func (db *DBZSet) zDelItem(t *Batch, key []byte, member []byte, skipDelScore bool) (int64, error) {
	ek := db.zEncodeSetKey(key, member)
	v, err := db.IKV.Get(ek)
	if err != nil {
		return 0, err
	}
	//not exists return
	if v == nil {
		return 0, nil
	}

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

	t.Delete(ek)

	return 1, nil
}

func (db *DBZSet) zIncrSize(t *Batch, key []byte, delta int64) (int64, error) {
	sk := db.zEncodeSizeKey(key)

	size, err := Int64(db.IKV.Get(sk))
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

	if v, err := db.IKV.Get(ek); err != nil {
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

	db.SetKeyMeta(t, key, ZSetType)

	return exists, nil
}

// ZAdd add the members.
func (db *DBZSet) ZAdd(ctx context.Context, key []byte, args ...driver.ScorePair) (int64, error) {
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

	err := t.Commit(ctx)
	return num, err
}

// ZCard gets the size of the zset.
func (db *DBZSet) ZCard(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}

	sk := db.zEncodeSizeKey(key)
	return Int64(db.IKV.Get(sk))
}

// ZScore gets the score of member.
func (db *DBZSet) ZScore(ctx context.Context, key []byte, member []byte) (int64, error) {
	if err := checkZSetKMSize(key, member); err != nil {
		return InvalidScore, err
	}

	score := InvalidScore

	k := db.zEncodeSetKey(key, member)
	if v, err := db.IKV.Get(k); err != nil {
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
func (db *DBZSet) ZRem(ctx context.Context, key []byte, members ...[]byte) (int64, error) {
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

	err := t.Commit(ctx)
	return num, err
}

// ZIncrBy increases the score of member with delta.
func (db *DBZSet) ZIncrBy(ctx context.Context, key []byte, delta int64, member []byte) (int64, error) {
	if err := checkZSetKMSize(key, member); err != nil {
		return InvalidScore, err
	}

	t := db.batch
	t.Lock()
	defer t.Unlock()

	ek := db.zEncodeSetKey(key, member)

	var oldScore int64
	v, err := db.IKV.Get(ek)
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

	db.SetKeyMeta(t, key, ZSetType)

	err = t.Commit(ctx)
	return newScore, err
}

// ZCount gets the number of score in [min, max]
func (db *DBZSet) ZCount(ctx context.Context, key []byte, min int64, max int64) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	minKey := db.zEncodeStartScoreKey(key, min)
	maxKey := db.zEncodeStopScoreKey(key, max)

	rangeType := driver.RangeROpen

	it := db.IKV.RangeLimitIterator(minKey, maxKey, rangeType, 0, -1)
	var n int64
	for ; it.Valid(); it.Next() {
		n++
	}
	it.Close()

	return n, nil
}

func (db *DBZSet) zrank(ctx context.Context, key []byte, member []byte, reverse bool) (int64, error) {
	if err := checkZSetKMSize(key, member); err != nil {
		return 0, err
	}

	k := db.zEncodeSetKey(key, member)

	it := db.IKV.NewIterator()
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
		return db.IKV.RangeLimitIterator(minKey, maxKey, driver.RangeClose, offset, count)
	}
	return db.IKV.RevRangeLimitIterator(minKey, maxKey, driver.RangeClose, offset, count)
}

func (db *DBZSet) zRange(ctx context.Context, key []byte, min int64, max int64, offset int, count int, reverse bool) ([]driver.ScorePair, error) {
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

// zParseLimit parse index pos to limit offset count
func (db *DBZSet) zParseLimit(ctx context.Context, key []byte, start int, stop int) (offset int, count int, err error) {
	if start < 0 || stop < 0 {
		//refer redis implementation
		var size int64
		size, err = db.ZCard(ctx, key)
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

// ZRange gets the members from start to stop.
func (db *DBZSet) ZRange(ctx context.Context, key []byte, start int, stop int) ([]driver.ScorePair, error) {
	return db.ZRangeGeneric(ctx, key, start, stop, false)
}

// ZRangeByScore gets the data with score in min and max.
// min and max must be inclusive
// if no limit, set offset = 0 and count = -1
func (db *DBZSet) ZRangeByScore(ctx context.Context, key []byte, min int64, max int64,
	offset int, count int) ([]driver.ScorePair, error) {
	return db.ZRangeByScoreGeneric(ctx, key, min, max, offset, count, false)
}

// ZRank gets the rank of member.
func (db *DBZSet) ZRank(ctx context.Context, key []byte, member []byte) (int64, error) {
	return db.zrank(ctx, key, member, false)
}

// ZRemRangeByRank removes the member at range from start to stop.
func (db *DBZSet) ZRemRangeByRank(ctx context.Context, key []byte, start int, stop int) (int64, error) {
	offset, count, err := db.zParseLimit(ctx, key, start, stop)
	if err != nil {
		return 0, err
	}

	var rmCnt int64

	t := db.batch
	t.Lock()
	defer t.Unlock()

	if rmCnt, err = db.zRemRange(t, key, MinScore, MaxScore, offset, count); err != nil {
		return 0, err
	}
	if _, err = db.zIncrSize(t, key, -rmCnt); err != nil {
		return 0, err
	}
	if err = t.Commit(ctx); err != nil {
		return 0, err
	}

	return rmCnt, nil
}

// ZRemRangeByScore removes the data with score at [min, max]
func (db *DBZSet) ZRemRangeByScore(ctx context.Context, key []byte, min int64, max int64) (int64, error) {
	t := db.batch
	t.Lock()
	defer t.Unlock()

	rmCnt, err := db.zRemRange(t, key, min, max, 0, -1)
	if err != nil {
		return 0, nil
	}
	if _, err := db.zIncrSize(t, key, -rmCnt); err != nil {
		return 0, err
	}
	if err = t.Commit(ctx); err != nil {
		return 0, err
	}

	return rmCnt, nil
}

// ZRevRange gets the data reversed.
func (db *DBZSet) ZRevRange(ctx context.Context, key []byte, start int, stop int) ([]driver.ScorePair, error) {
	return db.ZRangeGeneric(ctx, key, start, stop, true)
}

// ZRevRank gets the rank of member reversed.
func (db *DBZSet) ZRevRank(ctx context.Context, key []byte, member []byte) (int64, error) {
	return db.zrank(ctx, key, member, true)
}

// ZRevRangeByScore gets the data with score at [min, max]
// min and max must be inclusive
// if no limit, set offset = 0 and count = -1
func (db *DBZSet) ZRevRangeByScore(ctx context.Context, key []byte, min int64, max int64, offset int, count int) ([]driver.ScorePair, error) {
	return db.ZRangeByScoreGeneric(ctx, key, min, max, offset, count, true)
}

// ZRangeGeneric is a generic function for scan zset.
// zrange/zrevrange index pos start,stop
func (db *DBZSet) ZRangeGeneric(ctx context.Context, key []byte, start int, stop int, reverse bool) ([]driver.ScorePair, error) {
	offset, count, err := db.zParseLimit(ctx, key, start, stop)
	if err != nil {
		return nil, err
	}

	return db.zRange(ctx, key, MinScore, MaxScore, offset, count, reverse)
}

// ZRangeByScoreGeneric is a generic function to scan zset with score.
// min and max must be inclusive
// if no limit, set offset = 0 and count<0
func (db *DBZSet) ZRangeByScoreGeneric(ctx context.Context, key []byte, min int64, max int64,
	offset int, count int, reverse bool) ([]driver.ScorePair, error) {

	return db.zRange(ctx, key, min, max, offset, count, reverse)
}

// Expire expires the zset.
func (db *DBZSet) Expire(ctx context.Context, key []byte, duration int64) (int64, error) {
	if duration <= 0 {
		return 0, ErrExpireValue
	}

	return db.zExpireAt(ctx, key, time.Now().Unix()+duration)
}

// ExpireAt expires the zset at when.
func (db *DBZSet) ExpireAt(ctx context.Context, key []byte, when int64) (int64, error) {
	if when <= time.Now().Unix() {
		return 0, ErrExpireValue
	}

	return db.zExpireAt(ctx, key, when)
}

// TTL gets the TTL of zset.
func (db *DBZSet) TTL(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return -1, err
	}

	sk := db.zEncodeSizeKey(key)
	v, err := db.IKV.Get(sk)
	if err != nil {
		return -1, err
	}
	if v == nil {
		return -2, nil
	}

	return db.ttl(ZSetType, key)
}

// Persist removes the TTL of zset.
func (db *DBZSet) Persist(ctx context.Context, key []byte) (int64, error) {
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

	err = t.Commit(ctx)
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
func (db *DBZSet) ZUnionStore(ctx context.Context, destKey []byte, srcKeys [][]byte, weights []int64, aggregate []byte) (int64, error) {

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
		scorePairs, err := db.ZRange(ctx, key, 0, -1)
		if err != nil {
			return 0, err
		}
		for _, pair := range scorePairs {
			if score, ok := destMap[utils.Bytes2String(pair.Member)]; !ok {
				destMap[utils.Bytes2String(pair.Member)] = pair.Score * weights[i]
			} else {
				destMap[utils.Bytes2String(pair.Member)] = aggregateFunc(score, pair.Score*weights[i])
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

	if err := t.Commit(ctx); err != nil {
		return 0, err
	}
	return n, nil
}

// ZInterStore intersects the zsets and stores to dest zset.
func (db *DBZSet) ZInterStore(ctx context.Context, destKey []byte, srcKeys [][]byte, weights []int64, aggregate []byte) (int64, error) {
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
	scorePairs, err := db.ZRange(ctx, srcKeys[0], 0, -1)
	if err != nil {
		return 0, err
	}
	for _, pair := range scorePairs {
		destMap[utils.Bytes2String(pair.Member)] = pair.Score * weights[0]
	}

	for i, key := range srcKeys[1:] {
		scorePairs, err := db.ZRange(ctx, key, 0, -1)
		if err != nil {
			return 0, err
		}
		tmpMap := map[string]int64{}
		for _, pair := range scorePairs {
			if score, ok := destMap[utils.Bytes2String(pair.Member)]; ok {
				tmpMap[utils.Bytes2String(pair.Member)] = aggregateFunc(score, pair.Score*weights[i+1])
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

	if err := t.Commit(ctx); err != nil {
		return 0, err
	}
	return n, nil
}

// ZRangeByLex scans the zset lexicographically
func (db *DBZSet) ZRangeByLex(ctx context.Context, key []byte, min []byte, max []byte, rangeType driver.RangeType, offset int, count int) ([][]byte, error) {
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

	it := db.IKV.RangeLimitIterator(min, max, rangeType, offset, count)
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
func (db *DBZSet) ZRemRangeByLex(ctx context.Context, key []byte, min []byte, max []byte, rangeType driver.RangeType) (int64, error) {
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

	it := db.IKV.RangeIterator(min, max, rangeType)
	defer it.Close()

	var num int64
	for ; it.Valid(); it.Next() {
		ek := it.RawKey()
		_, m, err := db.zDecodeSetKey(ek)
		if err != nil {
			continue
		}

		if n, err := db.zDelItem(t, key, m, false); err != nil {
			return 0, err
		} else if n == 1 {
			num++
		}

		//t.Delete(ek)
	}

	if _, err := db.zIncrSize(t, key, -num); err != nil {
		return 0, err
	}

	if err := t.Commit(ctx); err != nil {
		return 0, err
	}

	return num, nil
}

// ZLexCount gets the count of zset lexicographically.
func (db *DBZSet) ZLexCount(ctx context.Context, key []byte, min []byte, max []byte, rangeType driver.RangeType) (int64, error) {
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

	it := db.IKV.RangeIterator(min, max, rangeType)
	defer it.Close()

	var n int64
	for ; it.Valid(); it.Next() {
		n++
	}

	return n, nil
}

// Exists checks zset existed or not.
func (db *DBZSet) Exists(ctx context.Context, key []byte) (int64, error) {
	if err := checkKeySize(key); err != nil {
		return 0, err
	}
	sk := db.zEncodeSizeKey(key)
	v, err := db.IKV.Get(sk)
	if v != nil && err == nil {
		return 1, nil
	}
	return 0, err
}

func (db *DBZSet) zExpireAt(ctx context.Context, key []byte, when int64) (int64, error) {
	t := db.batch
	t.Lock()
	defer t.Unlock()

	if zcnt, err := db.ZCard(ctx, key); err != nil || zcnt == 0 {
		return 0, err
	}

	db.expireAt(t, ZSetType, key, when)
	if err := t.Commit(ctx); err != nil {
		return 0, err
	}

	return 1, nil
}

// Dump zset rdb
func (db *DBZSet) Dump(ctx context.Context, key []byte) (binVal []byte, err error) {
	v, err := db.ZRangeByScore(ctx, key, MinScore, MaxScore, 0, -1)
	if err != nil {
		return
	} else if len(v) == 0 {
		return
	}

	zsVal := make(rdb.ZSet, len(v))
	for i := 0; i < len(v); i++ {
		zsVal[i].Member = v[i].Member
		zsVal[i].Score = float64(v[i].Score)
	}

	return rdb.DumpZSetValue(zsVal), nil
}

// Restore zset rdb
// use int64 for zset score, not float
func (db *DBZSet) Restore(ctx context.Context, t *Batch, key []byte, ttl int64, val rdb.ZSet) (err error) {
	if _, err = db.BatchDel(ctx, t, key); err != nil {
		return
	}

	sp := make([]driver.ScorePair, len(val))
	for i := 0; i < len(val); i++ {
		sp[i] = driver.ScorePair{Score: int64(val[i].Score), Member: val[i].Member}
	}

	if _, err = db.BatchZAdd(ctx, t, key, sp...); err != nil {
		return
	}

	if ttl > 0 {
		if _, err = db.BatchExpire(ctx, t, key, ttl); err != nil {
			return
		}
	}
	return
}

// BatchDel clears multi zsets.
func (db *DBZSet) BatchDel(ctx context.Context, t *Batch, keys ...[]byte) (int64, error) {
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
		n, err := db.delete(t, key)
		if err != nil {
			return 0, err
		}
		if n > 0 {
			nums++
		}
	}

	return int64(nums), nil
}

// BatchZAdd add the members.
func (db *DBZSet) BatchZAdd(ctx context.Context, t *Batch, key []byte, args ...driver.ScorePair) (int64, error) {
	if len(args) == 0 {
		return 0, nil
	}

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

	return num, nil
}

// BatchExpire expires the zset.
func (db *DBZSet) BatchExpire(ctx context.Context, t *Batch, key []byte, duration int64) (int64, error) {
	if duration <= 0 {
		return 0, ErrExpireValue
	}

	when := time.Now().Unix() + duration
	db.expireAt(t, ZSetType, key, when)
	return 1, nil
}
