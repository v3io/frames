# frames

Server and client library of streaming data from v3io

## Components

- Go server ([msgpack][msgpack] over chunked HTTP)
- Go client
- Python client

[msgpack]: https://msgpack.org/

## Server API

### POST /read

Read from database. POST body is a JSON encoded `ReadRequest`
Return value is chunked HTTP where each chunk is msgpack encoded `Frame`

### POST /write?backend=BACKEND&table=TABLE

Write to database, `backend` and `table` are fields in `WriteRequest`
Body should be chunked msgpack encoded frames
Response is JSON in the format

    {
	"num_frames": 10,
	"num_rows": 1000
    }

### Development

Core is written in [Go](https://golang.org/).

To run the tests run `go test -v ./...`

## LICENSE

[Apache 2](LICENSE)
