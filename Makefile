
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall

all: correctness persistence

correctness: skiplist.o kvstore.o correctness.o 

persistence: skiplist.o kvstore.o persistence.o

debug: skiplist.o debug.o

clean:
	-rm -f correctness persistence *.o
