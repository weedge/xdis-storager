package storager

import (
	"container/list"
	"context"
	"sync"
)

type lBlockKeys struct {
	sync.Mutex

	keys map[string]*list.List
}

func newLBlockKeys() *lBlockKeys {
	l := &lBlockKeys{
		keys: make(map[string]*list.List),
	}

	return l
}

func (l *lBlockKeys) signal(key []byte) {
	l.Lock()
	defer l.Unlock()

	s := Bytes2String(key)
	fns, ok := l.keys[s]
	if !ok {
		return
	}
	for e := fns.Front(); e != nil; e = e.Next() {
		fn := e.Value.(context.CancelFunc)
		fn()
	}

	delete(l.keys, s)
}

func (l *lBlockKeys) popOrWait(db *DBList, key []byte, whereSeq int32, fn context.CancelFunc) ([]interface{}, error) {
	v, err := db.lpop(key, whereSeq)
	if err != nil {
		return nil, err
	} else if v != nil {
		return []interface{}{key, v}, nil
	}

	l.Lock()

	s := Bytes2String(key)
	chs, ok := l.keys[s]
	if !ok {
		chs = list.New()
		l.keys[s] = chs
	}

	chs.PushBack(fn)
	l.Unlock()
	return nil, nil
}
