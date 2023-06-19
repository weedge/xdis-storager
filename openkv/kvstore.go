package openkv

import (
	"fmt"
	"os"
	"path"

	driver "github.com/weedge/pkg/driver/openkv"
	"github.com/weedge/xdis-storager/config"
)

func initStorePath(opts *config.StorgerOptions) (p string, err error) {
	p = opts.DBPath
	if len(opts.DBPath) == 0 {
		p = path.Join(opts.DataDir, fmt.Sprintf("%s_data", opts.KVStoreName))
	}

	if err = os.MkdirAll(p, 0755); err != nil {
		return
	}

	return
}

func Open(opts *config.StorgerOptions) (db *DB, err error) {
	store, err := driver.GetStore(opts.KVStoreName)
	if err != nil {
		return
	}

	p, err := initStorePath(opts)
	if err != nil {
		return
	}

	idb, err := store.Open(p)
	if err != nil {
		return
	}

	db = &DB{
		opts: opts,
		IDB:  idb,
		name: store.Name(),
	}

	return
}

func Repair(opts *config.StorgerOptions) (err error) {
	s, err := driver.GetStore(opts.KVStoreName)
	if err != nil {
		return
	}

	p, err := initStorePath(opts)
	if err != nil {
		return
	}

	return s.Repair(p)
}
