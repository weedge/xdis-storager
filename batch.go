package storager

import (
	"context"
	"sync"

	"github.com/weedge/xdis-storager/openkv"
)

// Batch write batch commit
type Batch struct {
	store *Storager
	*openkv.WriteBatch

	sync.Locker
}

func NewBatch(store *Storager, wb *openkv.WriteBatch, locker sync.Locker) *Batch {
	return &Batch{store: store, WriteBatch: wb, Locker: locker}
}

// todo impl buffer commiter to commit (use list push/pop)
func (b *Batch) Commit(ctx context.Context) error {
	if b.store != nil && b.store.committer != nil {
		return b.store.committer.Commit(ctx, b.WriteBatch)
	}

	b.store.commitLock.Lock()
	defer b.store.commitLock.Unlock()
	return b.WriteBatch.Commit()
}

func (b *Batch) Lock() {
	b.Locker.Lock()
}

func (b *Batch) Unlock() {
	b.WriteBatch.Rollback()
	b.Locker.Unlock()
}

func (b *Batch) Put(key []byte, value []byte) {
	b.WriteBatch.Put(key, value)
}

func (b *Batch) Delete(key []byte) {
	b.WriteBatch.Delete(key)
}

type dbBatchLocker struct {
	l      *sync.Mutex
	wrLock *sync.RWMutex
}

func (l *dbBatchLocker) Lock() {
	l.wrLock.RLock()
	l.l.Lock()
}

func (l *dbBatchLocker) Unlock() {
	l.l.Unlock()
	l.wrLock.RUnlock()
}
