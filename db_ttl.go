package storager

import "time"

func (db *DB) rmExpire(t *Batch, dataType byte, key []byte) (int64, error) {
	mk := db.expEncodeMetaKey(dataType, key)
	v, err := db.IKVStoreDB.Get(mk)
	if err != nil {
		return 0, err
	} else if v == nil {
		return 0, nil
	}

	when, err2 := Int64(v, nil)
	if err2 != nil {
		return 0, err2
	}

	tk := db.expEncodeTimeKey(dataType, key, when)
	t.Delete(mk)
	t.Delete(tk)
	return 1, nil
}

func (db *DB) ttl(dataType byte, key []byte) (t int64, err error) {
	mk := db.expEncodeMetaKey(dataType, key)

	if t, err = Int64(db.IKVStoreDB.Get(mk)); err != nil || t == 0 {
		t = -1
	} else {
		t -= time.Now().Unix()
		if t <= 0 {
			t = -1
		}
		// if t == -1 : to remove ????
	}

	return t, err
}

func (db *DB) expireAt(t *Batch, dataType byte, key []byte, when int64) {
	mk := db.expEncodeMetaKey(dataType, key)
	tk := db.expEncodeTimeKey(dataType, key, when)

	t.Put(tk, mk)
	t.Put(mk, PutInt64(when))

	db.ttlChecker.setNextCheckTime(when, false)
}
