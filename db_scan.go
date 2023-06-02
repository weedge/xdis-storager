package storager

import (
	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/utils"
	"github.com/weedge/xdis-storager/openkv"
)

func (db *DB) scanGeneric(storeDataType byte, key []byte, count int,
	inclusive bool, match string, reverse bool) ([][]byte, error) {

	r, err := utils.BuildMatchRegexp(match)
	if err != nil {
		return nil, err
	}

	minKey, maxKey, err := db.buildScanKeyRange(storeDataType, key, reverse)
	if err != nil {
		return nil, err
	}

	count = checkScanCount(count)

	it := db.buildScanIterator(minKey, maxKey, inclusive, reverse)

	v := make([][]byte, 0, count)

	for i := 0; it.Valid() && i < count; it.Next() {
		if k, err := db.decodeScanKey(storeDataType, it.Key()); err != nil {
			continue
		} else if r != nil && !r.Match(k) {
			continue
		} else {
			v = append(v, k)
			i++
		}
	}
	it.Close()
	return v, nil
}

func (db *DB) buildScanKeyRange(storeDataType byte, key []byte, reverse bool) (minKey []byte, maxKey []byte, err error) {
	if !reverse {
		if minKey, err = db.encodeScanMinKey(storeDataType, key); err != nil {
			return
		}
		if maxKey, err = db.encodeScanMaxKey(storeDataType, nil); err != nil {
			return
		}
	} else {
		if minKey, err = db.encodeScanMinKey(storeDataType, nil); err != nil {
			return
		}
		if maxKey, err = db.encodeScanMaxKey(storeDataType, key); err != nil {
			return
		}
	}
	return
}

func checkScanCount(count int) int {
	if count <= 0 {
		count = DefaultScanCount
	}

	return count
}

func (db *DB) buildScanIterator(minKey []byte, maxKey []byte, inclusive bool, reverse bool) *openkv.RangeLimitIterator {
	tp := driver.RangeOpen

	if !reverse {
		if inclusive {
			tp = driver.RangeROpen
		}
	} else {
		if inclusive {
			tp = driver.RangeLOpen
		}
	}

	var it *openkv.RangeLimitIterator
	if !reverse {
		it = db.IKV.RangeIterator(minKey, maxKey, tp)
	} else {
		it = db.IKV.RevRangeIterator(minKey, maxKey, tp)
	}

	return it
}
