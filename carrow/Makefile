.PHONY: clean

ARROW_INC = $(shell pkg-config --cflags arrow)
PLASMA_DB = /tmp/plasma.db
CXXOPT := -O2
LD_LIBRARY_PATH := /miniconda/lib
PKG_CONFIG_PATH := /miniconda/lib/pkgconfig

all: libcarrow.a
	go build ./...

libcarrow.a: carrow.o
	ar r $@ $^

%.o: %.cc
	g++ -Wall -g $(CXXOPT) -std=c++11 -I$(ARROW_INC) -o $@ -c $^

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
		make
		go run ./_misc/wtr.go -db /tmp/plasma.db -id $(ID)

wtr:
		go build ./_misc/wtr.go

gdb-wtr: wtr
	gdb wtr
