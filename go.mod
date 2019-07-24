module github.com/v3io/frames

require (
	github.com/ghodss/yaml v1.0.0
	github.com/golang/protobuf v1.2.0
	github.com/nuclio/logger v0.0.1
	github.com/nuclio/zap v0.0.2
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pkg/errors v0.8.1
	github.com/tinylib/msgp v1.1.0 // indirect
	github.com/v3io/v3io-go v0.0.0-20190717000000-e266c3b72d73fc193c4ecbc64f63bef9f8245621
	github.com/v3io/v3io-tsdb v0.9.6
	github.com/valyala/fasthttp v1.2.0
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
	golang.org/x/net v0.0.0-20181114220301-adae6a3d119a
	google.golang.org/grpc v1.17.0
)

replace github.com/xwb1989/sqlparser => github.com/v3io/sqlparser v0.0.0-20190306105200-4d7273501871
