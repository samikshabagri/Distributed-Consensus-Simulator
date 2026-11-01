CXX = g++
CXXFLAGS = -std=c++17 -O2 -Wall -Wextra -pthread

all: raft_sim raft_tests

raft_sim: main.cpp RaftNode.cpp *.h
	$(CXX) $(CXXFLAGS) main.cpp RaftNode.cpp -o raft_sim

raft_tests: tests.cpp RaftNode.cpp *.h
	$(CXX) $(CXXFLAGS) tests.cpp RaftNode.cpp -o raft_tests

test: raft_tests
	./raft_tests

clean:
	rm -f raft_sim raft_tests raft_sim.exe raft_tests.exe results.csv
