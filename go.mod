module github.com/v3io/frames

go 1.19

require (
	github.com/ghodss/yaml v1.0.0
	github.com/golang/groupcache v0.0.0-20191027212112-611e8accdfc9
	github.com/golang/protobuf v1.2.0
	github.com/nuclio/errors v0.0.4
	github.com/nuclio/logger v0.0.1
	github.com/nuclio/zap v0.1.2
	github.com/pkg/errors v0.8.1
	github.com/stretchr/testify v1.8.1
	github.com/v3io/v3io-go v0.3.0
	github.com/v3io/v3io-tsdb v0.14.1
	github.com/valyala/fasthttp v1.44.0
	github.com/xwb1989/sqlparser v0.0.0-20180606152119-120387863bf2
	golang.org/x/net v0.17.0
	golang.org/x/text v0.13.0
	google.golang.org/grpc v1.20.0
)

require (
	github.com/andybalholm/brotli v1.0.4 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/imdario/mergo v0.3.7 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/liranbg/uberzap v1.20.0-nuclio.1 // indirect
	github.com/logrusorgru/aurora/v3 v3.0.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20181016184325-3113b8401b8a // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/sys v0.13.0 // indirect
	google.golang.org/genproto v0.0.0-20180817151627-c66870c02cf8 // indirect
	gopkg.in/yaml.v2 v2.2.8 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	zombiezen.com/go/capnproto2 v2.17.0+incompatible // indirect
)

replace (
	github.com/v3io/frames => ./
	github.com/v3io/v3io-go => github.com/v3io/v3io-go v0.3.0
	github.com/xwb1989/sqlparser => github.com/v3io/sqlparser v0.0.0-20190306105200-4d7273501871
)
