CXX = g++
CXXFLAGS = -std=c++17 -O2 -pthread -static

all: raft_sim.exe

raft_sim.exe: main.cpp RaftNode.cpp *.h
	$(CXX) $(CXXFLAGS) main.cpp RaftNode.cpp -lws2_32 -o raft_sim.exe

clean:
	rm -f raft_sim.exe
