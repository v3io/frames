module github.com/v3io/frames

require (
	github.com/ghodss/yaml v1.0.0
	github.com/golang/protobuf v1.2.0
	github.com/nuclio/logger v0.0.1
	github.com/nuclio/zap v0.0.2
	github.com/pkg/errors v0.8.1
	github.com/v3io/v3io-go v0.0.0-20190606000000-9441f7c028db0b9642b541b07f30e35a1b812c58
	github.com/v3io/v3io-tsdb v0.9.3
	github.com/valyala/fasthttp v1.2.0
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
	golang.org/x/net v0.0.0-20181114220301-adae6a3d119a
	google.golang.org/grpc v1.17.0
)

replace github.com/xwb1989/sqlparser => github.com/v3io/sqlparser v0.0.0-20190306105200-4d7273501871
