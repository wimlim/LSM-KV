
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall

all: correctness persistence

correctness:  sstable.o memtable.o kvstore.o correctness.o 

persistence: sstable.o memtable.o kvstore.o persistence.o

debug: sstable.o memtable.o kvstore.o debug.o

clean:
	-rm -f correctness persistence debug *.o
