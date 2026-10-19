#ifndef RAFTNODE_H
#define RAFTNODE_H

#include <thread>
#include <atomic>
#include <memory>
#include <vector>
#include <string>
#include "NetworkSimulator.h"

using namespace std;

enum class NodeState {
    Follower,
    Candidate,
    Leader
};

class RaftNode {
private:
    int id;
    atomic<NodeState> state;
    atomic<int> currentTerm;
    int votedFor;
    int votesReceived;
    vector<LogEntry> log;

    thread nodeThread;
    atomic<bool> running;

    shared_ptr<ThreadSafeQueue<shared_ptr<Message>>> messageQueue;

    // Main event loop
    void run();
    
    // State transitions
    void becomeFollower(int term);
    void becomeCandidate();
    void becomeLeader();

    // Message handlers
    void processMessage(shared_ptr<Message> msg);
    void handleRequestVoteArgs(shared_ptr<RequestVoteArgs> args);
    void handleRequestVoteReply(shared_ptr<RequestVoteReply> reply);
    void handleAppendEntriesArgs(shared_ptr<AppendEntriesArgs> args);
    
    // Utilities
    void sendHeartbeats();
    int getRandomTimeout();

public:
    RaftNode(int id);
    ~RaftNode();
    
    void start();
    void stop();

    bool isLeader() const { return state == NodeState::Leader; }
    int getId() const { return id; }
    int getTerm() const { return currentTerm; }
    
    // Propose transaction to cluster
    bool proposeTransaction(const string& command);
    void simulateStepDown();
};

#endif // RAFTNODE_H
