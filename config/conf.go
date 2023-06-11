package config

type StorgerOptions struct {
	DataDir          string `mapstructure:"dataDir"`
	Databases        int    `mapstructure:"databases"`
	KVStoreName      string `mapstructure:"kvStoreName"`
	DBPath           string `mapstructure:"dbPath"`
	DBSyncCommit     int    `mapstructure:"dbSyncCommit"`
	TTLCheckInterval int    `mapstructure:"ttlCheckInterval"`
}

func DefaultStoragerOptions() *StorgerOptions {
	return &StorgerOptions{
		DataDir:          DefaultDataDir,
		Databases:        DefaultDatabases,
		KVStoreName:      DefaultKVStoreName,
		TTLCheckInterval: DefaultTTLCheckInterval,
	}
}
