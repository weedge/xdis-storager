package config

type StorgerOptions struct {
	OpenkvOptions    `mapstructure:",squash"`
	Databases        int       `mapstructure:"databases"`
	Slots            int       `mapstructure:"slots"`
	TTLCheckInterval int       `mapstructure:"ttlCheckInterval"`
	CBFItmeCn        uint      `mapstructure:"cbfItmeCn"`
	CBFBucketSize    uint8     `mapstructure:"cbfBucketSize"`
	CBFFpRate        float64   `mapstructure:"cbfFpRate"`
	MigrateAsyncTask AsyncTask `mapstructure:"migrateAsyncTask"`
}
type OpenkvOptions struct {
	DataDir        string `mapstructure:"dataDir"`
	KVStoreName    string `mapstructure:"kvStoreName"`
	DBPath         string `mapstructure:"dbPath"`
	DBSyncCommit   int    `mapstructure:"dbSyncCommit"`
	BufferOpCommit bool   `mapstructure:"bufferOpCommit"`
}

type AsyncTask struct {
	Name     string `mapstructure:"name"`
	ChSize   int64  `mapstructure:"chSize"`
	WorkerCn int    `mapstructure:"workerCn"`
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
func DefaultMigrateAsyncTask() *AsyncTask {
	return &AsyncTask{
		Name:     "migrate",
		ChSize:   DefaultChSize,
		WorkerCn: DefaultWorkerCn,
	}
}
