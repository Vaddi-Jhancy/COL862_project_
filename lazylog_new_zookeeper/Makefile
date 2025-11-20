CXX = g++
CXXFLAGS = -std=c++17 -O2 -pthread
ZK_FLAGS = -DTHREADED
ZK_LIBS  = -lzookeeper_mt

all: lazylog

lazylog: main.o sequencer.o shard.o client.o
	$(CXX) $(CXXFLAGS) $(ZK_FLAGS) -o lazylog main.o sequencer.o shard.o client.o $(ZK_LIBS)

main.o: main.cpp
	$(CXX) $(CXXFLAGS) -c main.cpp

sequencer.o: sequencer.cpp common.h
	$(CXX) $(CXXFLAGS) $(ZK_FLAGS) -c sequencer.cpp

shard.o: shard.cpp common.h
	$(CXX) $(CXXFLAGS) -c shard.cpp

client.o: client.cpp common.h
	$(CXX) $(CXXFLAGS) -c client.cpp

clean:
	rm -f *.o lazylog
