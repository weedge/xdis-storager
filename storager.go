package storager

import (
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/gofrs/flock"
	"github.com/weedge/pkg/driver"
	"github.com/weedge/pkg/safer"
	"github.com/weedge/xdis-storager/config"
	"github.com/weedge/xdis-storager/openkv"
)

// Storager core store struct for server use like redis
type Storager struct {
	opts *config.StorgerOptions
	// file lock
	fLock *flock.Flock

	// open kv store engine
	odb *openkv.DB

	// multi storager db instances on one kv store engine
	dbs map[int]*DB
	// dbs map lock for get and set map[int]*DB
	dbLock sync.Mutex

	// allow one write at same time
	wLock sync.RWMutex
	// allow one write commit at same time
	commitLock sync.Mutex

	// ttl check
	ttlCheckers  []*TTLChecker
	ttlCheckerCh chan *TTLChecker
	wg           sync.WaitGroup
	quit         chan struct{}
}

func Open(opts *config.StorgerOptions) (store *Storager, err error) {
	store = &Storager{}
	store.InitOpts(opts)

	os.MkdirAll(opts.DataDir, 0755)

	defer func(s *Storager) {
		if err != nil {
			if e := s.Close(); e != nil {
				klog.Errorf("close store err: %s", e.Error())
			}
		}
	}(store)

	store.fLock = flock.New(path.Join(opts.DataDir, "LOCK"))
	ok, err := store.fLock.TryLock()
	if err != nil {
		return
	}
	if !ok {
		err = fmt.Errorf("store file had locked")
		return
	}

	if store.odb, err = openkv.Open(opts); err != nil {
		return nil, err
	}

	store.dbs = make(map[int]*DB, opts.Databases)
	store.quit = make(chan struct{})

	store.checkTTL()

	return
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
func (m *Storager) Select(index int) (idb driver.IDB, err error) {
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

	// async send checker,
	// if recv checkTTL tick to check,ch full, maybe block
	go func(db *DB) {
		m.ttlCheckerCh <- db.ttlChecker
	}(db)
	idb = db
	m.dbLock.Unlock()

	return
}

func (m *Storager) checkTTL() {
	m.ttlCheckers = make([]*TTLChecker, 0, config.DefaultDatabases)
	m.ttlCheckerCh = make(chan *TTLChecker, config.DefaultDatabases)

	safer.GoSafely(&m.wg, false, func() {
		tick := time.NewTicker(time.Duration(m.opts.TTLCheckInterval) * time.Second)
		defer tick.Stop()

		for {
			select {
			case <-tick.C:
				for _, c := range m.ttlCheckers {
					c.check()
				}
			case c := <-m.ttlCheckerCh:
				m.ttlCheckers = append(m.ttlCheckers, c)
				c.check()
			case <-m.quit:
				return
			}
		}
	}, nil, os.Stderr)
}

// FlushAll will clear all data
func (m *Storager) FlushAll() error {
	m.wLock.Lock()
	defer m.wLock.Unlock()

	return m.flushAll()
}

func (m *Storager) flushAll() (err error) {
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
				klog.Errorf("flush all commit error: %s", err.Error())
				return
			}
			n = 0
		}
		w.Delete(it.RawKey())
	}

	if err = w.Commit(); err != nil {
		klog.Errorf("flush all commit error: %s", err.Error())
		return
	}

	return nil
}
