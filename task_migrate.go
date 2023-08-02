package storager

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type MigrateAsyncTask struct {
	Ctx    context.Context
	DBSlot *DBSlot
	Cli    *redis.Client
	Keys   []*MetaObjKey
}

func (m *MigrateAsyncTask) Run() (err error) {
	err = m.DBSlot.migrateBatchKey(m.Ctx, m.Cli, m.Keys...)
	if err != nil {
		return fmt.Errorf("migrate len(keys) %d %w", len(m.Keys), err)
	}

	return
}
