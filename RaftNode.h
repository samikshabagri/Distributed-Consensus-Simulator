#ifndef RAFTNODE_H
#define RAFTNODE_H

#include <thread>
#include <atomic>
#include <memory>
#include <vector>
#include <string>
#include "NetworkSimulator.h"

enum class NodeState {
    Follower,
    Candidate,
    Leader
};

class RaftNode {
private:
    int id;
    std::atomic<NodeState> state;
    std::atomic<int> currentTerm;
    int votedFor;
    int votesReceived;
    std::vector<LogEntry> log;

    std::thread nodeThread;
    std::atomic<bool> running;

    std::shared_ptr<ThreadSafeQueue<std::shared_ptr<Message>>> messageQueue;

    // Main event loop
    void run();
    
    // State transitions
    void becomeFollower(int term);
    void becomeCandidate();
    void becomeLeader();

    // Message handlers
    void processMessage(std::shared_ptr<Message> msg);
    void handleRequestVoteArgs(std::shared_ptr<RequestVoteArgs> args);
    void handleRequestVoteReply(std::shared_ptr<RequestVoteReply> reply);
    void handleAppendEntriesArgs(std::shared_ptr<AppendEntriesArgs> args);
    
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
    bool proposeTransaction(const std::string& command);
    void simulateStepDown();
};

#endif // RAFTNODE_H
