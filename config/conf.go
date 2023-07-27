package config

type StorgerOptions struct {
	OpenkvOptions    `mapstructure:",squash"`
	Databases        int     `mapstructure:"databases"`
	Slots            int     `mapstructure:"slots"`
	TTLCheckInterval int     `mapstructure:"ttlCheckInterval"`
	CBFItmeCn        uint    `mapstructure:"cbfItmeCn"`
	CBFBucketSize    uint8   `mapstructure:"cbfBucketSize"`
	CBFFpRate        float64 `mapstructure:"cbfFpRate"`
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
		CBFItmeCn:        DefaultCBFItmeCn,
		CBFBucketSize:    DefaultCBFBucketSize,
		CBFFpRate:        DefaultCBFfpRate,
	}
}
func DefaultOpenkvOptions() *OpenkvOptions {
	return &OpenkvOptions{
		DataDir:     DefaultDataDir,
		KVStoreName: DefaultKVStoreName,
	}
}

