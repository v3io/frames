module github.com/v3io/frames

require (
	github.com/ghodss/yaml v1.0.0
	github.com/golang/groupcache v0.0.0-00000000000000-611e8accdfc92c4187d399e95ce826046d4c8d73
	github.com/golang/protobuf v1.2.0
	github.com/nuclio/errors v0.0.1
	github.com/nuclio/logger v0.0.1
	github.com/nuclio/zap v0.0.2
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pkg/errors v0.8.1
	github.com/stretchr/testify v1.4.0
	github.com/v3io/v3io-go v0.0.5-0.20191205125653-9003ae83f0b6
	github.com/v3io/v3io-tsdb v0.9.12-1
	github.com/valyala/fasthttp v1.2.0
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
	golang.org/x/net v0.0.0-20181114220301-adae6a3d119a
	golang.org/x/sync v0.0.0-20181108010431-42b317875d0f // indirect
	google.golang.org/grpc v1.17.0
)

replace github.com/xwb1989/sqlparser => github.com/v3io/sqlparser v0.0.0-20190306105200-4d7273501871
