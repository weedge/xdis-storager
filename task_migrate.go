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
	Key    *MetaObjKey
}

func (m *MigrateAsyncTask) Run() (err error) {
	err = m.DBSlot.migrateKey(m.Ctx, m.Cli, m.Key)
	if err != nil {
		return fmt.Errorf("migrate key %+v %w", m.Key, err)
	}

	return
}
