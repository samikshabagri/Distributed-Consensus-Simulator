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

enum class NodeState {
    Follower,
    Candidate,
    Leader
};

string stateName(NodeState s);

// A copy of a node's state, taken under its lock (used by main/tests to print tables)
struct NodeStatus {
    int id;
    NodeState state;
    int term;
    int lastLogIndex;
    int commitIndex;
    int lastApplied;
    bool crashed;
    BankLedger ledger;
};

// Result of a client write
struct ProposeResult {
    bool accepted = false;   // false if this node is not leader (or is crashed)
    int index = 0;           // log index the entry was placed at
    int term = 0;            // term it was placed in
};

class RaftNode {
private:
    // --- Timing constants (milliseconds) ---
    static constexpr int HEARTBEAT_MS = 250;
    static constexpr int ELECTION_MIN_MS = 1000;
    static constexpr int ELECTION_MAX_MS = 2200;

    int id;
    int clusterSize;

    // Phase III: ONE mutex protects all the Raft state below.
    // In Phase II the main thread and the node thread both changed votedFor /
    // votesReceived without a lock (ThreadSanitizer reported data races).
    mutable mutex mtx;

    // --- Persistent state (a real server would keep this on disk; it survives a crash) ---
    int currentTerm;
    int votedFor;
    vector<LogEntry> log;        // log[0] is a dummy entry (term 0); real entries start at 1

    // --- Volatile state (lost on crash) ---
    NodeState state;
    int votesReceived;
    int commitIndex;             // highest index known to be committed (stored on a majority)
    int lastApplied;             // highest index applied to the ledger
    int knownLeader;
    BankLedger ledger;           // the replicated state machine

    // --- Leader-only state ---
    vector<int> nextIndex;       // per follower: next log index to send
    vector<int> matchIndex;      // per follower: highest index known to be replicated there

    // --- Timers ---
    mt19937 rng;                 // per-node random generator for election timeouts
    chrono::steady_clock::time_point electionDeadline;
    chrono::steady_clock::time_point nextHeartbeat;

    thread nodeThread;
    atomic<bool> running;
    bool crashed;

    shared_ptr<ThreadSafeQueue<shared_ptr<Message>>> messageQueue;

    // Main event loop
    void run();
    void onTimer();

    // State transitions (all called with mtx held)
    void becomeFollower(int term);
    void becomeCandidate();
    void becomeLeader();

    // Message handlers (all called with mtx held)
    void processMessage(shared_ptr<Message> msg);
    void handleRequestVoteArgs(shared_ptr<RequestVoteArgs> args);
    void handleRequestVoteReply(shared_ptr<RequestVoteReply> reply);
    void handleAppendEntriesArgs(shared_ptr<AppendEntriesArgs> args);
    void handleAppendEntriesReply(shared_ptr<AppendEntriesReply> reply);

    // Log replication helpers (called with mtx held)
    void sendAppendEntries(int peerId);
    void broadcastAppendEntries();
    void advanceCommitIndex();
    void applyCommittedEntries();

    // Utilities
    int lastLogIndex() const { return (int)log.size() - 1; }
    int lastLogTerm() const { return log.back().term; }
    int majority() const { return clusterSize / 2 + 1; }
    void resetElectionTimer();
    int getRandomTimeout();
    string tag() const;

public:
    RaftNode(int id, int clusterSize);
    ~RaftNode();

    void start();
    void signalStop() { running = false; }
    void stop();

    bool isLeader() const;
    int getId() const { return id; }
    int getTerm() const;
    NodeStatus getStatus() const;
    vector<LogEntry> getCommittedLog() const;

    // --- Client API ---
    // Write: only the leader accepts. Returns where the entry was placed.
    ProposeResult proposeTransaction(const Transaction& txn);
    // True once the entry at (index, term) is committed on this node.
    // False if it was overwritten by a newer leader (it will never commit).
    bool isCommitted(int index, int term, bool& lost) const;
    // Read (CQRS): any node can answer from its own ledger. May be stale.
    double readBalance(const string& account, int& appliedIndex) const;

    // --- Fault injection ---
    void crash();      // stop responding, lose volatile state
    void recover();    // come back as a follower and catch up from the leader
    bool isCrashed() const;
};

#endif // RAFTNODE_H
