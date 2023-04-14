
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall

all: correctness persistence

correctness:  bloomfilter.o skiplist.o kvstore.o correctness.o 

persistence: bloomfilter.o skiplist.o kvstore.o persistence.o

debug: bloomfilter.o skiplist.o debug.o

clean:
	-rm -f correctness persistence debug *.o
