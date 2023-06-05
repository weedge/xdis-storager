module github.com/weedge/xdis-storager

go 1.19

require (
	github.com/cloudwego/kitex v0.5.2
	github.com/gofrs/flock v0.8.1
	github.com/weedge/pkg v0.0.0-20230602074650-53ef9798dd1f
)

require golang.org/x/sys v0.8.0 // indirect

replace github.com/weedge/pkg => ../pkg
