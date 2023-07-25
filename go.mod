module github.com/weedge/xdis-storager

go 1.19

require (
	github.com/cloudwego/kitex v0.5.2
	github.com/gofrs/flock v0.8.1
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db
	github.com/stretchr/testify v1.8.3
	github.com/syndtr/goleveldb v1.0.0
	github.com/weedge/pkg v0.0.0-20230725030031-e699e06784f3
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	golang.org/x/sys v0.8.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

//replace github.com/weedge/pkg => ../pkg
