package storager

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/gofrs/flock"
	"github.com/twmb/murmur3"
	boom "github.com/tylertreat/BoomFilters"
	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/safer"
	"github.com/weedge/xdis-storager/config"
	storagerDriver "github.com/weedge/xdis-storager/driver"
	"github.com/weedge/xdis-storager/openkv"
)

var gOpts *config.StorgerOptions

// Storager core store struct for server use like redis
type Storager struct {
	opts *config.StorgerOptions
	// file lock
	fLock *flock.Flock

	// open kv store engine
	odb *openkv.DB
	//odb storagerDriver.IKV

	// multi storager db instances on one kv store engine
	dbs map[int]*DB
	// dbs map lock for get and set map[int]*DB
	dbLock sync.Mutex

	// allow one write at same time
	wLock sync.RWMutex
	// allow one write commit at same time
	commitLock sync.Mutex

	// when set committer, use committer.Commit instead of writebatch commit for write op
	committer storagerDriver.ICommitter

	// counting bloom filter for db stats
	cbf *boom.CountingBloomFilter
	// need lock to op CBF bitset
	cbfLock sync.Mutex

	// ttl check
	ttlCheckers  []*TTLChecker
	ttlCheckerCh chan *TTLChecker
	wg           sync.WaitGroup
	quit         chan struct{}
}

func New(opts *config.StorgerOptions) (store *Storager) {
	store = &Storager{}
	store.InitOpts(opts)
	return
}

func (store *Storager) Open(ctx context.Context) (err error) {
	opts := store.opts

	defer func() {
		if err != nil {
			if e := store.Close(); e != nil {
				klog.CtxErrorf(ctx, "close store err: %s", e.Error())
			}
		}
	}()

	os.MkdirAll(opts.DataDir, 0755)
	store.fLock = flock.New(path.Join(opts.DataDir, "LOCK"))
	ok, err := store.fLock.TryLock()
	if err != nil {
		return err
	}
	if !ok {
		err = fmt.Errorf("store file had locked")
		return err
	}

	if store.odb, err = openkv.Open(&opts.OpenkvOptions); err != nil {
		return
	}

	store.cbf = boom.NewCountingBloomFilter(opts.CBFItmeCn, opts.CBFBucketSize, opts.CBFFpRate)
	store.cbf.SetHash(murmur3.New64())
	InitAllDbStats(store)

	store.dbs = make(map[int]*DB, opts.Databases)
	store.quit = make(chan struct{})

	store.checkTTL(ctx)

	return
}

func (m *Storager) Name() string {
	return RegisterStoragerName
}

func (m *Storager) InitOpts(opts *config.StorgerOptions) {
	if len(opts.DataDir) == 0 {
		opts.DataDir = config.DefaultDataDir
	}

	if opts.Databases == 0 {
		opts.Databases = config.DefaultDatabases
	} else if opts.Databases > MaxDatabases {
		opts.Databases = MaxDatabases
	}

	if opts.TTLCheckInterval < 0 {
		opts.TTLCheckInterval = config.DefaultTTLCheckInterval
	}
	if opts.TTLCheckInterval > config.MaxTTLCheckInterval-config.DefaultTTLCheckInterval {
		opts.TTLCheckInterval = config.MaxTTLCheckInterval - config.DefaultTTLCheckInterval
	}

	m.opts = opts
	gOpts = opts
}

// Close close ttl check, kv store close, flock close
func (m *Storager) Close() (err error) {
	if m.quit != nil {
		close(m.quit)
	}
	m.wg.Wait()

	errs := []error{}
	if m.odb != nil {
		errs = append(errs, m.odb.Close())
		m.odb = nil
	}

	for _, db := range m.dbs {
		errs = append(errs, db.Close())
	}
	errs = append(errs, m.fLock.Close())

	errStrs := []string{}
	for _, er := range errs {
		if er != nil {
			errStrs = append(errStrs, er.Error())
		}
	}
	if len(errStrs) > 0 {
		err = fmt.Errorf("errs: %s", strings.Join(errStrs, " | "))
	}
	return
}

// Select chooses a database.
func (m *Storager) Select(ctx context.Context, index int) (idb driver.IDB, err error) {
	if index < 0 || index >= m.opts.Databases {
		return nil, fmt.Errorf("invalid db index %d, must in [0, %d]", index, m.opts.Databases-1)
	}

	m.dbLock.Lock()
	db, ok := m.dbs[index]
	if ok {
		idb = db
		m.dbLock.Unlock()
		return
	}
	db = NewDB(m, index)
	m.dbs[index] = db
	db.stats = LoadDbStats(index)

	// async send checker,
	// if recv checkTTL tick to check,ch full, maybe block
	go func() {
		m.ttlCheckerCh <- db.ttlChecker
	}()
	idb = db
	m.dbLock.Unlock()

	return
}

func (m *Storager) checkTTL(ctx context.Context) {
	m.ttlCheckers = make([]*TTLChecker, 0, config.DefaultDatabases)
	m.ttlCheckerCh = make(chan *TTLChecker, config.DefaultDatabases)

	safer.GoSafely(&m.wg, false, func() {
		tick := time.NewTicker(time.Duration(m.opts.TTLCheckInterval) * time.Second)
		defer tick.Stop()

		for {
			select {
			case <-tick.C:
				for _, c := range m.ttlCheckers {
					c.check(ctx)
				}
			case c := <-m.ttlCheckerCh:
				m.ttlCheckers = append(m.ttlCheckers, c)
				c.check(ctx)
			case <-m.quit:
				return
			}
		}
	}, nil, os.Stderr)
}

// FlushAll will clear all data
func (m *Storager) FlushAll(ctx context.Context) error {
	m.wLock.Lock()
	defer m.wLock.Unlock()

	return m.flushAll(ctx)
}

func (m *Storager) flushAll(ctx context.Context) (err error) {
	it := m.odb.NewIterator()
	defer it.Close()

	w := m.odb.NewWriteBatch()
	defer func() {
		if err != nil {
			w.Rollback()
		}
	}()

	n := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		n++
		if n == 10000 {
			if err = w.Commit(); err != nil {
				klog.CtxErrorf(ctx, "flush all commit error: %s", err.Error())
				return
			}
			n = 0
		}
		w.Delete(it.RawKey())
	}

	if err = w.Commit(); err != nil {
		klog.CtxErrorf(ctx, "flush all commit error: %s", err.Error())
		return
	}

	return nil
}

// GetKVStore get kvstore engine which save data
func (s *Storager) GetKVStore() storagerDriver.IKV {
	return s.odb
}

// SetCommitter
func (s *Storager) SetCommitter(committer storagerDriver.ICommitter) {
	s.committer = committer
}

// GetRWlock noCopy return rw muxtex lock for read only
func (s *Storager) GetRWLock() *sync.RWMutex {
	return &s.wLock
}

// GetCommitLock noCopy return mutex lock for commit log/data
func (s *Storager) GetCommitLock() *sync.Mutex {
	return &s.commitLock
}
