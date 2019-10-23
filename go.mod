module github.com/v3io/frames

require (
	github.com/ghodss/yaml v1.0.0
	github.com/golang/protobuf v1.2.0
	github.com/nuclio/logger v0.0.1
	github.com/nuclio/zap v0.0.2
	github.com/pkg/errors v0.8.1
	github.com/v3io/v3io-go v0.0.5-0.20190826150152-1f2c9a9a61cb
	github.com/v3io/v3io-tsdb v0.9.10
	github.com/valyala/fasthttp v1.2.0
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
	golang.org/x/net v0.0.0-20181114220301-adae6a3d119a
	google.golang.org/grpc v1.17.0
)

replace (
	github.com/v3io/v3io-go => github.com/gtopper/v3io-go v0.0.0-20191023140729-153160c5bb1004aa73b710cfc51dce08e5f20799
	github.com/v3io/v3io-tsdb => github.com/gtopper/v3io-tsdb v0.0.0-20191023145112-76fcabfb3650adaca88b70eb109b1b28dac4d410
	github.com/xwb1989/sqlparser => github.com/v3io/sqlparser v0.0.0-20190306105200-4d7273501871
)
