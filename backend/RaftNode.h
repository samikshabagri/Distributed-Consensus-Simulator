#ifndef RAFTNODE_H
#define RAFTNODE_H

#include <thread>
#include <atomic>
#include <memory>
#include <vector>
#include <string>
#include <mutex>
#include <chrono>
#include <random>
#include "NetworkSimulator.h"
#include "BankLedger.h"

using namespace std;

enum class NodeState { Follower, Candidate, Leader };

string stateName(NodeState s);

struct NodeStatus {
    int id;
    NodeState state;
    int term;
    int lastLogIndex;
    int commitIndex;
    int lastApplied;
    bool crashed;
    bool autoScaled;
    long long readsHandled;
    long long writesHandled;
    BankLedger ledger;
};

struct ProposeResult {
    bool accepted = false;
    int index = 0;
    int term = 0;
};

class RaftNode {
private:
    static constexpr int HEARTBEAT_MS = 250;
    static constexpr int ELECTION_MIN_MS = 1000;
    static constexpr int ELECTION_MAX_MS = 2200;

    int id;
    int quorumSize;
    bool autoScaled;

    mutable mutex mtx;

    // Persistent Raft state
    int currentTerm;
    int votedFor;
    vector<LogEntry> log;

    // Volatile state
    NodeState state;
    int votesReceived;
    int commitIndex;
    int lastApplied;
    BankLedger ledger;

    // Leader replication state
    vector<int> nextIndex;
    vector<int> matchIndex;

    // Traffic counters
    atomic<long long> readsHandled{0};
    atomic<long long> writesHandled{0};

    // Timers & thread
    mt19937 rng;
    chrono::steady_clock::time_point electionDeadline;
    chrono::steady_clock::time_point nextHeartbeat;
    thread nodeThread;
    atomic<bool> running;
    bool crashed;
    shared_ptr<ThreadSafeQueue<shared_ptr<Message>>> messageQueue;

    void run();
    void onTimer();

    void becomeFollower(int term);
    void becomeCandidate();
    void becomeLeader();

    void processMessage(shared_ptr<Message> msg);
    void handleRequestVoteArgs(shared_ptr<RequestVoteArgs> args);
    void handleRequestVoteReply(shared_ptr<RequestVoteReply> reply);
    void handleAppendEntriesArgs(shared_ptr<AppendEntriesArgs> args);
    void handleAppendEntriesReply(shared_ptr<AppendEntriesReply> reply);

    void ensurePeer(int peerId);
    void sendAppendEntries(int peerId);
    void broadcastAppendEntries();
    void advanceCommitIndex();
    void applyCommittedEntries();

    int lastLogIndex() const { return (int)log.size() - 1; }
    int lastLogTerm() const { return log.back().term; }
    int majority() const { return quorumSize / 2 + 1; }
    void resetElectionTimer();
    string tag() const;

public:
    RaftNode(int id, int quorumSize, bool autoScaled = false);
    ~RaftNode();

    void start();
    void signalStop() { running = false; }
    void stop();

    bool isLeader() const;
    int getId() const { return id; }
    int getTerm() const;
    NodeStatus getStatus() const;

    ProposeResult proposeTransaction(const Transaction& txn);
    bool isCommitted(int index, int term, bool& lost) const;
    double readBalance(const string& account);

    void crash();
    void recover();
    bool isCrashed() const;
};

#endif
