package storager

import (
	"sync"
	"time"

	"github.com/weedge/xdis-storager/openkv"
)

type onExpired func(*Batch, []byte) (int64, error)

type TTLChecker struct {
	db *DB

	//use slice datatype for ttl check
	txs []*Batch
	cbs []onExpired

	//next check time
	nc int64
	sync.RWMutex
}

func NewTTLChecker(db *DB) *TTLChecker {
	c := &TTLChecker{
		db:  db,
		txs: make([]*Batch, maxDataType),
		cbs: make([]onExpired, maxDataType),
		nc:  0,
	}

	// register data type with batch and delete op
	c.register(StringType, db.string.batch, db.string.delete)
	c.register(ListType, db.list.batch, db.list.delete)
	c.register(HashType, db.hash.batch, db.hash.delete)
	c.register(SetType, db.set.batch, db.set.delete)
	c.register(ZSetType, db.zset.batch, db.zset.delete)
	c.register(BitmapType, db.bitmap.batch, db.bitmap.delete)

	return c
}

func (c *TTLChecker) register(dataType byte, batch *Batch, f onExpired) {
	c.txs[dataType] = batch
	c.cbs[dataType] = f
}

func (c *TTLChecker) setNextCheckTime(when int64, force bool) {
	c.Lock()
	if force {
		c.nc = when
	} else if c.nc > when {
		c.nc = when
	}
	c.Unlock()
}

func (c *TTLChecker) check() {
	now := time.Now().Unix()

	c.Lock()
	nc := c.nc
	c.Unlock()

	if now < nc {
		return
	}

	nc = now + 3600

	minKey := c.db.expEncodeTimeKey(NoneType, nil, 0)
	maxKey := c.db.expEncodeTimeKey(maxDataType, nil, nc)

	it := c.db.IKVStoreDB.RangeLimitIterator(minKey, maxKey, openkv.RangeROpen, 0, -1)
	for ; it.Valid(); it.Next() {
		tk := it.RawKey()
		mk := it.RawValue()

		dt, k, nt, err := c.db.expDecodeTimeKey(tk)
		if err != nil {
			continue
		}

		if nt > now {
			//the next ttl check time is nt!
			nc = nt
			break
		}

		t := c.txs[dt]
		cb := c.cbs[dt]
		if tk == nil || cb == nil {
			continue
		}

		t.Lock()
		if exp, err := Int64(c.db.IKVStoreDB.Get(mk)); err == nil {
			// check expire again
			if exp <= now {
				cb(t, k)
				t.Delete(tk)
				t.Delete(mk)
				t.Commit()
			}

		}
		t.Unlock()
	}
	it.Close()

	c.setNextCheckTime(nc, true)
}
