module github.com/v3io/frames

go 1.14

require (
	github.com/ghodss/yaml v1.0.0
	github.com/golang/groupcache v0.0.0-20191027212112-611e8accdfc9
	github.com/golang/protobuf v1.2.0
	github.com/nuclio/errors v0.0.1
	github.com/nuclio/logger v0.0.1
	github.com/nuclio/zap v0.0.2
	github.com/pkg/errors v0.8.1
	github.com/stretchr/testify v1.4.0
	github.com/v3io/v3io-go v0.1.5-0.20200416113214-f1b82b9a8e82
	github.com/v3io/v3io-tsdb v0.10.9
	github.com/valyala/fasthttp v1.2.0
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
	golang.org/x/net v0.0.0-20190311183353-d8887717615a
	google.golang.org/grpc v1.20.0
)

replace (
	github.com/v3io/frames => ./
	github.com/v3io/v3io-go => github.com/v3io/v3io-go v0.0.0-20200602081255-9a94144f811c
	github.com/xwb1989/sqlparser => github.com/v3io/sqlparser v0.0.0-20190306105200-4d7273501871
)
