module github.com/weedge/xdis-storager

go 1.19

require (
	github.com/cloudwego/kitex v0.5.2
	github.com/gofrs/flock v0.8.1
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db
	github.com/redis/go-redis/v9 v9.0.5
	github.com/stretchr/testify v1.8.3
	github.com/syndtr/goleveldb v1.0.0
	github.com/twmb/murmur3 v1.1.8
	github.com/tylertreat/BoomFilters v0.0.0-20210315201527-1a82519a3e43
	github.com/weedge/pkg v0.0.0-20230801170230-40c01f9e9a38
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/cupcake/rdb v0.0.0-20161107195141-43ba34106c76 // indirect
	github.com/d4l3k/messagediff v1.2.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	golang.org/x/sys v0.10.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

//replace github.com/weedge/pkg => ../pkg
