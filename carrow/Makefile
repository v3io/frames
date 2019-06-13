.PHONY: clean

CXXOPT := -O2
ARROW_INC := $(shell pkg-config --cflags arrow)

all: libcarrow.a

libcarrow.a: carrow.o
	ar r $@ $^

%.o: %.cc
	g++ -Wall -g $(CXXOPT) -std=c++11 -I$(ARROW_INC) -o $@ -c $^

clean:
	rm -f *.o *.a

fresh: clean all
