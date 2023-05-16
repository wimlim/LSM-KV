
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall

all: correctness persistence

correctness:  sstable.o skiplist.o kvstore.o correctness.o 

persistence: sstable.o skiplist.o kvstore.o persistence.o

debug: sstable.o skiplist.o kvstore.o debug.o

clean:
	-rm -f correctness persistence debug *.o
