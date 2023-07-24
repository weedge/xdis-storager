package config

type StorgerOptions struct {
	OpenkvOptions    `mapstructure:",squash"`
	Databases        int `mapstructure:"databases"`
	Slots            int `mapstructure:"slots"`
	TTLCheckInterval int `mapstructure:"ttlCheckInterval"`
}
type OpenkvOptions struct {
	DataDir      string `mapstructure:"dataDir"`
	KVStoreName  string `mapstructure:"kvStoreName"`
	DBPath       string `mapstructure:"dbPath"`
	DBSyncCommit int    `mapstructure:"dbSyncCommit"`
}

func DefaultStoragerOptions() *StorgerOptions {
	return &StorgerOptions{
		OpenkvOptions:    *DefaultOpenkvOptions(),
		Databases:        DefaultDatabases,
		Slots:            DefaulSlots,
		TTLCheckInterval: DefaultTTLCheckInterval,
	}
}
func DefaultOpenkvOptions() *OpenkvOptions {
	return &OpenkvOptions{
		DataDir:     DefaultDataDir,
		KVStoreName: DefaultKVStoreName,
	}
}
