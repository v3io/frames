.PHONY: clean

ARROW_SRC_DIR=/src/arrow/cpp/src -I/opt/miniconda/include
PLASMA_DB=/tmp/plasma.db
CXXOPT := -O2

all: libcarrow.a
	go build ./...

libcarrow.a: carrow.o
	ar r $@ $^

%.o: %.cc
	g++ -Wall -g $(CXXOPT) -std=c++11 -I$(ARROW_SRC_DIR) -o $@ -c $^

clean:
	rm -f *.o *.a

get-arrow:
		git clone git://github.com/apache/arrow.git ../arrow
		(cd ../arrow && git checkout apache-arrow-0.13.0)

build-docker:
	docker build . -t carrow:builder
	docker run \
		-v $(PWD):/src/carrow \
		-it --workdir=/src/carrow/ \
		carrow:builder

test:
	go test -v ./...

circleci:
	docker build -f Dockerfile.test .

benchmark:
	go test  -run  Example -count 10000

fresh: clean all

# Playground

plasma-client:
		g++ _misc/plasma.cc \
			-g \
			$(shell pkg-config --cflags --libs plasma) \
			$(shell pkg-config --cflags --libs arrow) \
			-I$(ARROW_SRC_DIR) \
			--std=c++11 \
			-o plasmac

plasma-client-local:
		g++ _misc/plasma.cc \
			-g \
			-larrow -lplasma \
			-L/opt/miniconda/lib \
			-I/opt/miniconda/include \
			--std=c++11 \
			-o plasmac

plasma-server:
		rm -f $(PLASMA_DB)
		plasma_store -m 1000000 -s $(PLASMA_DB)

run-wtr:
		PKG_CONFIG_PATH=/opt/miniconda/lib/pkgconfig make
		PKG_CONFIG_PATH=/opt/miniconda/lib/pkgconfig \
			LD_LIBRARY_PATH=/opt/miniconda/lib \
			go run ./_misc/wtr.go -db /tmp/plasma.db -id $(ID)

wtr:
	PKG_CONFIG_PATH=/opt/miniconda/lib/pkgconfig \
		LD_LIBRARY_PATH=/opt/miniconda/lib  \
		go build ./_misc/wtr.go

gdb-wtr: wtr
	LD_LIBRARY_PATH=/opt/miniconda/lib gdb wtr