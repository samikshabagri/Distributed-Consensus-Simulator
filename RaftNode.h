#pragma once
#include <thread>
#include <atomic>
#include <random>
#include <memory>
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

    std::thread nodeThread;
    std::atomic<bool> running;

    std::shared_ptr<ThreadSafeQueue<std::shared_ptr<Message>>> messageQueue;

    // Main loop
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
    
    // Utility
    void sendHeartbeats();
    int getRandomTimeout();

public:
    RaftNode(int id);
    ~RaftNode();
    
    void start();
    void stop();
};
