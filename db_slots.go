package storager

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/redis/go-redis/v9"
	"github.com/weedge/pkg/driver"
	goredisinjectors "github.com/weedge/pkg/injectors/goredis-injectors"
	"github.com/weedge/pkg/rdb"
	"github.com/weedge/pkg/utils/asynctask"
)

// HashTag like redis cluster hash tag
func HashTag(key []byte) []byte {
	part := key
	if i := bytes.IndexByte(part, '{'); i != -1 {
		part = part[i+1:]
	} else {
		return key
	}
	if i := bytes.IndexByte(part, '}'); i != -1 {
		return part[:i]
	} else {
		return key
	}
}

type DBSlot struct {
	*DB
	batch *Batch
}

func NewDBSlot(db *DB) *DBSlot {
	batch := NewBatch(db.store, db.IKV.NewWriteBatch(),
		&dbBatchLocker{
			l:      &sync.Mutex{},
			wrLock: &db.store.wLock,
		})
	return &DBSlot{DB: db, batch: batch}
}

func (m *DBSlot) HashTagToSlot(tag []byte) uint32 {
	return crc32.ChecksumIEEE(tag) % uint32(m.store.opts.Slots)
}

func (m *DBSlot) HashKeyToSlot(key []byte) ([]byte, uint32) {
	tag := HashTag(key)
	return tag, m.HashTagToSlot(tag)
}

// SlotsHashKey hash keys to slots, return slot slice
func (m *DBSlot) SlotsHashKey(ctx context.Context, keys ...[]byte) ([]uint64, error) {
	if m.store.opts.Slots <= 0 {
		return nil, ErrUnsupportSlots
	}

	slots := make([]uint64, 0, len(keys))
	for _, key := range keys {
		_, slot := m.HashKeyToSlot(key)
		slots = append(slots, uint64(slot))
	}

	return slots, nil
}

// MigrateSlotOneKey migrate slot one key/val to addr with timeout (ms)
// return 1, success, 0 slot is empty
func (m *DBSlot) MigrateSlotOneKey(ctx context.Context, addr string, timeout time.Duration, slot uint64) (migrateCn int64, err error) {
	key, err := m.findSlotFirstMetaObjKey(ctx, slot)
	if err == ErrKeyNotFound {
		err = nil
		return
	}
	if err != nil {
		return
	}

	migrateCn, err = m.migrate(ctx, addr, timeout, key)
	return
}

// MigrateSlotKeyWithSameTag migrate slot keys/vals  which have the same tag with one key to addr with timeout (ms)
// return n, success, 0 slot is empty
func (m *DBSlot) MigrateSlotKeyWithSameTag(ctx context.Context, addr string, timeout time.Duration, slot uint64) (migrateCn int64, err error) {
	key, err := m.findSlotFirstMetaObjKey(ctx, slot)
	if err == ErrKeyNotFound {
		err = nil
		return
	}
	if err != nil {
		return
	}

	metaKeys, err := m.findSlotAllTagMetaObjKey(ctx, key.Tag)
	if err != nil {
		return
	}
	migrateCn, err = m.migrate(ctx, addr, timeout, metaKeys...)
	return
}

// MigrateOneKey migrate one key/val (no hash tag  tag=key) to addr with timeout (ms)
// return n (same key, diff dataType), success, 0 slot is empty
func (m *DBSlot) MigrateOneKey(ctx context.Context, addr string, timeout time.Duration, key []byte) (migrateCn int64, err error) {
	tag := key
	metaKeys, err := m.findSlotAllTagMetaObjKey(ctx, tag)
	if err == ErrKeyNotFound {
		err = nil
		return
	}
	if err != nil {
		return
	}
	migrateCn, err = m.migrate(ctx, addr, timeout, metaKeys...)
	return
}

// MigrateKeyWithSameTag migrate keys/vals which have the same tag with one key to addr with timeout (ms)
// return n, n migrate success, 0 slot is empty
func (m *DBSlot) MigrateKeyWithSameTag(ctx context.Context, addr string, timeout time.Duration, key []byte) (migrateCn int64, err error) {
	tag, _ := m.HashKeyToSlot(key)
	metaKeys, err := m.findSlotAllTagMetaObjKey(ctx, tag)
	if err == ErrKeyNotFound {
		err = nil
		return
	}
	if err != nil {
		return
	}

	migrateCn, err = m.migrate(ctx, addr, timeout, metaKeys...)
	return
}

// SlotsRestore dest migrate addr restore slot obj [key ttlms serialized-value(rdb) ...]
func (m *DBSlot) SlotsRestore(ctx context.Context, objs ...*driver.SlotsRestoreObj) (err error) {
	m.batch.Lock()
	defer m.batch.Unlock()

	for _, obj := range objs {
		key := obj.Key
		val, err := rdb.DecodeDump(obj.Val)
		if err != nil {
			klog.CtxDebugf(ctx, "binVal %b decode err:%s", obj.Val, err.Error())
			return err
		}

		// current support seconds
		ttl := obj.TTLms / 1e3
		if ttl <= 0 {
			ttl = 0
		}

		// del -> store -> ttl(>0)
		switch value := val.(type) {
		case rdb.String:
			m.string.Restore(ctx, m.batch, key, ttl, value)
		case rdb.Hash:
			m.hash.Restore(ctx, m.batch, key, ttl, value)
		case rdb.List:
			m.list.Restore(ctx, m.batch, key, ttl, value)
		case rdb.Set:
			m.set.Restore(ctx, m.batch, key, ttl, value)
		case rdb.ZSet:
			m.zset.Restore(ctx, m.batch, key, ttl, value)
		default:
			return fmt.Errorf("invalid data type %T", value)
		}
	}

	err = m.batch.Commit(ctx)
	return
}

// SlotsInfo show slot info with slots range [start,start+count]
// return slotInfo slice
// if withSize is true, slotInfo.Size is slot's keys cn; size>0,show it;
// else exits key is 1, show it
func (m *DBSlot) SlotsInfo(ctx context.Context, startSlot, count uint64, withSize bool) (slotInfos []*driver.SlotInfo, err error) {
	if m.store.opts.Slots <= 0 {
		return nil, ErrUnsupportSlots
	}

	end := startSlot + count
	for slot := startSlot; slot <= end && slot < uint64(m.store.opts.Slots); slot++ {
		info := &driver.SlotInfo{}
		if !withSize {
			if key, err := m.findSlotFirstMetaObjKey(ctx, slot); err != nil && err != ErrKeyNotFound {
				return nil, err
			} else if key != nil {
				info.Num = slot
				info.Size = 1
				slotInfos = append(slotInfos, info)
			}
		} else {
			if size, err := m.getSlotMetaObjKeyCn(ctx, slot); err != nil {
				return nil, err
			} else if size > 0 {
				info.Num = slot
				info.Size = size
				slotInfos = append(slotInfos, info)
			}
		}
	}

	return
}

// SlotsDel del slots, return after del slot info
// just del slot one key
// if slot have key to del, slotInfo.Size is 1, else is 0
func (m *DBSlot) SlotsDel(ctx context.Context, slots ...uint64) (slotInfos []*driver.SlotInfo, err error) {
	if m.store.opts.Slots <= 0 {
		return nil, ErrUnsupportSlots
	}
	for _, slot := range slots {
		info := &driver.SlotInfo{}
		if key, err := m.findSlotFirstMetaObjKey(ctx, slot); err != nil && err != ErrKeyNotFound {
			return nil, err
		} else if key != nil {
			if _, err = m.BatchDel(ctx, key); err != nil {
				return nil, err
			}
			info.Num = slot
			info.Size = 1
		} else {
			info.Num = slot
			info.Size = 0
		}
		slotInfos = append(slotInfos, info)
	}

	return
}

// SlotsCheck slots  must check below case
// - The key stored in each slot can find the corresponding val in the db
// - Keys in each db can be found in the corresponding slot
// WARNING: just used debug/test, don't use in product,
func (m *DBSlot) SlotsCheck(ctx context.Context) (err error) {
	if m.store.opts.Slots <= 0 {
		return ErrUnsupportSlots
	}
	return
}

func (m *DBSlot) findSlotFirstMetaObjKey(ctx context.Context, slot uint64) (meta *MetaObjKey, err error) {
	slotEk := m.encodeDbIndexSlot(slot)
	it := m.NewIterator()
	defer it.Close()
	if it.Seek(slotEk); it.Valid() {
		ek := it.Key()
		// prefix iter end
		if !bytes.HasPrefix(ek, slotEk) {
			return nil, ErrKeyNotFound
		}
		meta, err = m.decodeDbIndexSlotTagMetaKey(ek)
		return
	}

	return nil, ErrKeyNotFound
}

func (m *DBSlot) getSlotMetaObjKeyCn(ctx context.Context, slot uint64) (cn uint64, err error) {
	slotEk := m.encodeDbIndexSlot(slot)
	it := m.NewIterator()
	defer it.Close()
	for it.Seek(slotEk); it.Valid(); it.Next() {
		ek := it.RawKey()
		// prefix iter end
		if !bytes.HasPrefix(ek, slotEk) {
			break
		}
		cn++
	}

	return
}

func (m *DBSlot) findSlotAllTagMetaObjKey(ctx context.Context, tag []byte) (objs []*MetaObjKey, err error) {
	tagEk := m.encodeDbIndexSlotTag(tag)
	it := m.NewIterator()
	defer it.Close()
	for it.Seek(tagEk); it.Valid(); it.Next() {
		ek := it.Key()
		// prefix iter end
		if !bytes.HasPrefix(ek, tagEk) {
			break
		}

		MetaObjKey, kerr := m.decodeDbIndexSlotTagMetaKey(ek)
		if kerr != nil {
			return nil, kerr
		}
		objs = append(objs, MetaObjKey)
	}

	if len(objs) == 0 {
		return nil, ErrKeyNotFound
	}
	return
}

// migrate key to addr with timeout(r/w), but migrate have some w ops,
// 1. send restore cmd to addr save rdb,
// 2. del cmd local migrated key
// NOTICE: migrate don't atomic, like network timeout(don't know key migrated),
// maybe migrated key in src,dest two slots
// if migrate failed, please try exec cmd again~
// so current migrate just adapter AP system
// online case:
// when slot keys is migrating, send r/w cmd to the src node slot,
// need proxy + pd or config srv (keep meta info, eg: key -> slot <> node) to transmit request
// if in migrating stat, key not in src node, try request dest node, (key migrate cmd (restore, del) and w/r cmd must mutex)
// offline case:
// use this as migrate tools
// To support migrate <-> resp kv store
// the dump value format is the same as redis.
// https://github.com/sripathikrishnan/redis-rdb-tools/blob/master/docs/RDB_Version_History.textile
// more think:
// use lsm tree as kvstore engine, need do sm compaction sstables when so many migrated keys, so need dump sstables then load it
// batch op no stream
func (m *DBSlot) migrate(ctx context.Context, addr string, timeout time.Duration, keys ...*MetaObjKey) (cn int64, err error) {
	if len(keys) == 0 {
		return
	}

	commonOpts := goredisinjectors.DefaultRedisClientCommonOptions()
	// smartClient/proxy to retry
	commonOpts.MaxRetries = -1
	cli := goredisinjectors.InitRedisClient(
		goredisinjectors.WithRedisAddr(addr),
		goredisinjectors.WithRedisDB(m.index),
		goredisinjectors.WithRedisClientCommonOptions(*commonOpts),
	).(*redis.Client).WithTimeout(timeout)
	startTime := time.Now()
	defer func() {
		klog.CtxInfof(ctx, "migrate len(keys) %d cost %d ms", len(keys), time.Since(startTime).Milliseconds())
		if cerr := cli.Close(); cerr != nil {
			err = cerr
			return
		}
	}()

	if m.store.opts.MigrateAsyncTask.ChSize > 0 {
		return m.asyncMigrateKeys(ctx, cli, keys...)
	}

	return m.syncMigrateKeys(ctx, cli, keys...)
}

func (m *DBSlot) asyncMigrateKeys(ctx context.Context, cli *redis.Client, keys ...*MetaObjKey) (cn int64, err error) {
	chSize := m.store.opts.MigrateAsyncTask.ChSize
	workerCn := m.store.opts.MigrateAsyncTask.WorkerCn
	if len(keys) < m.store.opts.MigrateAsyncTask.WorkerCn {
		workerCn = len(keys)
	}

	var mu sync.RWMutex
	var wgErrs []error
	asyncTask, err := asynctask.GetNamedAsyncTask(
		m.store.opts.MigrateAsyncTask.Name, chSize, workerCn,
		func(err error) {
			if err != nil {
				mu.Lock()
				wgErrs = append(wgErrs, err)
				mu.Unlock()
				return
			}
		})
	if err != nil {
		return
	}

	for i := range keys {
		if i > 0 && i%m.store.opts.MigrateBatchKeyCn == 0 {
			asyncTask.Post(&MigrateAsyncTask{Ctx: ctx, Cli: cli, Keys: keys[cn:i], DBSlot: m})
			cn += int64(m.store.opts.MigrateBatchKeyCn)
		}
	}
	asyncTask.Post(&MigrateAsyncTask{Ctx: ctx, Cli: cli, Keys: keys[cn:], DBSlot: m})
	asyncTask.Close()

	if len(wgErrs) > 0 {
		cn = int64(len(keys) - len(wgErrs))
		errStr := ""
		for _, e := range wgErrs {
			errStr += e.Error()
		}
		err = fmt.Errorf("wgErrs:%s", errStr)
		return
	}
	cn = int64(len(keys))

	return
}

func (m *DBSlot) syncMigrateKeys(ctx context.Context, cli *redis.Client, keys ...*MetaObjKey) (cn int64, err error) {
	for i := range keys {
		if i > 0 && i%m.store.opts.MigrateBatchKeyCn == 0 {
			err = m.migrateBatchKey(ctx, cli, keys[cn:i]...)
			if err != nil {
				return
			}
			cn += int64(m.store.opts.MigrateBatchKeyCn)
		}
	}

	err = m.migrateBatchKey(ctx, cli, keys[cn:]...)
	if err != nil {
		return
	}

	return int64(len(keys)), nil
}

func (m *DBSlot) migrateBatchKey(ctx context.Context, cli *redis.Client, keys ...*MetaObjKey) (err error) {
	objs := make([]*driver.SlotsRestoreObj, len(keys))
	for i, key := range keys {
		binVal, err := m.Dump(ctx, key)
		if err != nil {
			return err
		}

		ttl, err := m.TTL(ctx, key)
		if err != nil {
			return err
		}
		if ttl == -2 {
			return ErrKeyNotFound
		}
		if ttl < 0 {
			ttl = 0
		}
		objs[i] = &driver.SlotsRestoreObj{
			DB:    int32(m.index),
			Key:   key.DataKey,
			Val:   binVal,
			TTLms: ttl * 1e3,
		}
	}

	err = m.migrateSlotsRestoreObj(ctx, cli, objs...)
	if err != nil {
		return
	}

	_, err = m.BatchDel(ctx, keys...)
	if err != nil {
		return
	}

	return
}

func (m *DBSlot) migrateSlotsRestoreObj(ctx context.Context, cli *redis.Client, objs ...*driver.SlotsRestoreObj) (err error) {
	args := make([]any, len(objs)*3+1)
	args[0] = "slotsrestore"
	for i := range objs {
		args[3*i+1] = objs[i].Key
		args[3*i+2] = objs[i].TTLms
		args[3*i+3] = objs[i].Val
	}
	err = cli.Do(ctx, args...).Err()
	if err != nil {
		return
	}

	return
}

// Dump metaObj key
func (m *DBSlot) Dump(ctx context.Context, key *MetaObjKey) (binVal []byte, err error) {
	switch key.DataType {
	case StringType:
		binVal, err = m.string.Dump(ctx, key.DataKey)
	case HashType:
		binVal, err = m.hash.Dump(ctx, key.DataKey)
	case ListType:
		binVal, err = m.list.Dump(ctx, key.DataKey)
	case SetType:
		binVal, err = m.set.Dump(ctx, key.DataKey)
	case ZSetType:
		binVal, err = m.zset.Dump(ctx, key.DataKey)
	default:
		return nil, fmt.Errorf("un support metaKey dataType: %d", key.DataType)
	}

	return
}

// TTL metaObj key return ttl second
func (m *DBSlot) TTL(ctx context.Context, key *MetaObjKey) (int64, error) {
	switch key.DataType {
	case StringType:
		return m.string.TTL(ctx, key.DataKey)
	case HashType:
		return m.hash.TTL(ctx, key.DataKey)
	case ListType:
		return m.list.TTL(ctx, key.DataKey)
	case SetType:
		return m.set.TTL(ctx, key.DataKey)
	case ZSetType:
		return m.zset.TTL(ctx, key.DataKey)
	default:
		return 0, fmt.Errorf("un support metaKey dataType: %d", key.DataType)
	}
}

func (m *DBSlot) BatchDel(ctx context.Context, keys ...*MetaObjKey) (num int64, err error) {
	m.batch.Lock()
	defer m.batch.Unlock()

	for _, key := range keys {
		n := int64(0)
		switch key.DataType {
		case StringType:
			n, err = m.string.BatchDel(ctx, m.batch, key.DataKey)
		case HashType:
			n, err = m.hash.BatchDel(ctx, m.batch, key.DataKey)
		case ListType:
			n, err = m.list.BatchDel(ctx, m.batch, key.DataKey)
		case SetType:
			n, err = m.set.BatchDel(ctx, m.batch, key.DataKey)
		case ZSetType:
			n, err = m.zset.BatchDel(ctx, m.batch, key.DataKey)
		default:
			return 0, fmt.Errorf("un support metaKey dataType: %d", key.DataType)
		}
		if err != nil {
			return
		}
		num += n
	}
	err = m.batch.Commit(ctx)

	return
}
