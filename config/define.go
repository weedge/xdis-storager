package config

const (
	DefaultKVStoreName = "goleveldb"
	DefaultDataDir     = "./data"
	DefaultDatabases   = 16
	DefaulSlots        = 1024

	DefaultTTLCheckInterval = 1
	MaxTTLCheckInterval     = 3600
)

const (
	// https://en.wikipedia.org/wiki/Bloom_filter
	// use github.com/bits-and-blooms/bloom/v3 EstimateParameters to check
	// 85M (BFBitSize/8) ~ 50 million keys and 0.01 False Positive Rate (bit size: 718879379)
	DefaultBFBitSize = 718879379
	// 10 hash 0.01 False Positive Rate
	DefaultBFHashCn = 10

	// http://pages.cs.wisc.edu/~jussara/papers/00ton.pdf
	// use github.com/tylertreat/BoomFilters
	// 502533109 bit size ~ 64M
	DefaultCBFItmeCn = 50 * 1024 * 1024
	// counter bucket size, counter Max 2^4 - 1
	// in this db key stats case, use DefaultCBFBucketSize is ok
	DefaultCBFBucketSize = 4
	// false-positive rate
	DefaultCBFfpRate = 0.01

	// Count-min Sketch There are more false-positive that maybe do not fit this case
)
