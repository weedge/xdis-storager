package storager

import (
	"fmt"

	"github.com/weedge/pkg/driver"
)

// StatsInfo get stroage stats info by sections
func (s *Storager) StatsInfo(sections ...string) (info map[string][]driver.InfoPair) {
	info = map[string][]driver.InfoPair{}
	for _, section := range sections {
		switch section {
		case "keyspace":
			info[section] = s.KeySpaceStatsInfo()
		case "existkeydb":
			info[section] = s.ExistKeyDB()
		default:
		}
	}

	return
}

func (s *Storager) KeySpaceStatsInfo() (info []driver.InfoPair) {
	info = make([]driver.InfoPair, 0, s.opts.Databases)
	for index := 0; index < s.opts.Databases; index++ {
		dbStats := LoadDbStats(index)
		if dbStats == nil || dbStats.KeyCn.Load() == 0 {
			continue
		}
		info = append(info, driver.InfoPair{Key: fmt.Sprintf("db%d", index), Value: dbStats.String()})
	}

	return
}

func (s *Storager) ExistKeyDB() (info []driver.InfoPair) {
	info = make([]driver.InfoPair, 0, s.opts.Databases)
	for index := 0; index < s.opts.Databases; index++ {
		if DBHasKey(s, index) {
			info = append(info, driver.InfoPair{Key: fmt.Sprintf("db%d", index), Value: ""})
		}
	}

	return
}
