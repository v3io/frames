# carrow - Go bindings to Apache Arrow via C++-API

Access to [Arrow C++](https://arrow.apache.org/docs/cpp/) from Go.

## FAQ

#### Why Not [Apache Arrow for Go](https://github.com/apache/arrow/tree/master/go)?

We'd like to share memory between Go & Python and the current arrow bindings
don't have that option. Since `pyarrow` uses the `C++` Arrow under the hood, we
can just pass a s a pointer.

Also, the C++ Arrow library is more maintained than the Go one and have more
features.

## Development

- The C++ glue layer is in `carrow.cc`, we try to keep it simple and unaware of Go.
- See `Dockerfile` & `build-docker` target in the `Makefile` on how to setup an environment
- See `Dockerfile.test` for running tests (used in CircleCI)

### Debugging

We have Go, C++ & Python code working together. See the `Dockerfile` on how we
get dependencies and set environment for development. 

#### Example using gdb

    $ PKG_CONFIG_PATH=/opt/miniconda/lib/pkgconfig LD_LIBRARY_PATH=/opt/miniconda/lib  go build ./_misc/wtr.go
    $ LD_LIBRARY_PATH=/opt/miniconda/lib gdb wtr
    (gdb) break carrow.cc:write_table
    (gdb) run -db /tmp/plasma.db -id 800
