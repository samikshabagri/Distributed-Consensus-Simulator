#include "RaftNode.h"
#include "Logger.h"
#include "Metrics.h"
#include <random>
#include <algorithm>

using namespace std;

string stateName(NodeState s) {
    switch (s) {
        case NodeState::Follower:  return "Follower";
        case NodeState::Candidate: return "Candidate";
        case NodeState::Leader:    return "LEADER";
    }
    return "?";
}

static Logger& L() { return Logger::get(); }

// ============================================================================
// Construction / lifecycle
// ============================================================================

RaftNode::RaftNode(int id, int clusterSize)
    : id(id), clusterSize(clusterSize), currentTerm(0), votedFor(-1),
      state(NodeState::Follower), votesReceived(0), commitIndex(0), lastApplied(0),
      knownLeader(-1), nextIndex(clusterSize, 1), matchIndex(clusterSize, 0),
      rng((unsigned int)chrono::high_resolution_clock::now().time_since_epoch().count() + id * 7919),
      running(false), crashed(false) {
    log.push_back(LogEntry{});   // dummy entry at index 0 (term 0)
    NetworkSimulator::getInstance().registerNode(id);
    messageQueue = NetworkSimulator::getInstance().getQueue(id);
    resetElectionTimer();
}

RaftNode::~RaftNode() {
    stop();
}

void RaftNode::start() {
    {
        lock_guard<mutex> lock(mtx);
        resetElectionTimer();
    }
    running = true;
    nodeThread = thread(&RaftNode::run, this);
}

void RaftNode::stop() {
    running = false;
    if (nodeThread.joinable()) nodeThread.join();
}

string RaftNode::tag() const {
    return "[Node " + to_string(id) + " | T" + to_string(currentTerm) + "]";
}

int RaftNode::getRandomTimeout() {
    uniform_int_distribution<int> dist(ELECTION_MIN_MS, ELECTION_MAX_MS);
    return dist(rng);
}

// Called whenever we hear from a valid leader or grant a vote
void RaftNode::resetElectionTimer() {
    electionDeadline = chrono::steady_clock::now() + chrono::milliseconds(getRandomTimeout());
}

// ============================================================================
// Main event loop
// Phase II reset the timeout on ANY message. Now the node keeps two
// deadlines (election / heartbeat) and only resets the election deadline for
// a valid leader message or a granted vote - this is what the Raft paper says.
// ============================================================================

void RaftNode::run() {
    while (running) {
        int waitMs;
        {
            lock_guard<mutex> lock(mtx);
            auto now = chrono::steady_clock::now();
            auto deadline = (state == NodeState::Leader) ? nextHeartbeat : electionDeadline;
            waitMs = (int)chrono::duration_cast<chrono::milliseconds>(deadline - now).count();
            if (crashed) waitMs = 50;
        }
        if (waitMs < 1) waitMs = 1;

        shared_ptr<Message> msg;
        bool received = messageQueue->wait_and_pop(msg, waitMs);

        lock_guard<mutex> lock(mtx);
        if (crashed) {                 // a crashed node ignores everything
            messageQueue->clear();
            continue;
        }
        if (received && msg) processMessage(msg);
        onTimer();
    }
}

void RaftNode::onTimer() {
    auto now = chrono::steady_clock::now();
    if (state == NodeState::Leader) {
        if (now >= nextHeartbeat) broadcastAppendEntries();   // heartbeat (+ any missing entries)
    } else if (now >= electionDeadline) {
        becomeCandidate();
    }
}

// ============================================================================
// State transitions
// ============================================================================

void RaftNode::becomeFollower(int term) {
    // Phase III fix: only clear votedFor when the term actually increases.
    // Phase II cleared it every time, so a node could vote twice in one term.
    if (term > currentTerm) {
        currentTerm = term;
        votedFor = -1;
    }
    if (state != NodeState::Follower)
        L().detail(tag() + " steps down to Follower");
    state = NodeState::Follower;
    votesReceived = 0;
}

void RaftNode::becomeCandidate() {
    state = NodeState::Candidate;
    currentTerm++;
    votedFor = id;
    votesReceived = 1;  // vote for self
    knownLeader = -1;
    resetElectionTimer();
    Metrics::get().electionsStarted++;

    L().log(tag() + " election timeout -> CANDIDATE, requesting votes");

    for (int peerId : NetworkSimulator::getInstance().getAllNodeIds()) {
        if (peerId == id) continue;
        auto msg = make_shared<RequestVoteArgs>();
        msg->senderId = id;
        msg->receiverId = peerId;
        msg->term = currentTerm;
        msg->candidateId = id;
        msg->lastLogIndex = lastLogIndex();   // Phase III: now actually sent
        msg->lastLogTerm = lastLogTerm();
        NetworkSimulator::getInstance().sendMessage(msg);
    }
    if (votesReceived >= majority()) becomeLeader();   // only true for a 1-node cluster
}

void RaftNode::becomeLeader() {
    state = NodeState::Leader;
    knownLeader = id;
    Metrics::get().recordLeader(currentTerm, id);

    // Initialise leader bookkeeping: assume followers are up to date, back off if not
    for (int i = 0; i < clusterSize; ++i) {
        nextIndex[i] = lastLogIndex() + 1;
        matchIndex[i] = 0;
    }

    L().blank();
    L().log(">>> " + tag() + " WON THE ELECTION with " + to_string(votesReceived) + "/" +
            to_string(clusterSize) + " votes -> LEADER <<<");

    // Raft Section 8: append a no-op entry for this term. Entries from older terms
    // can only be committed together with an entry from the current term.
    LogEntry noop;
    noop.term = currentTerm;
    log.push_back(noop);
    matchIndex[id] = lastLogIndex();

    broadcastAppendEntries();
}

// ============================================================================
// Message handling
// ============================================================================

void RaftNode::processMessage(shared_ptr<Message> msg) {
    // Rule for ALL servers: a higher term means we are out of date -> become follower
    if (msg->term > currentTerm) {
        if (state == NodeState::Leader)
            L().log(tag() + " saw higher term " + to_string(msg->term) + " from Node " +
                    to_string(msg->senderId) + " -> old leader STEPS DOWN");
        becomeFollower(msg->term);
    }

    switch (msg->getType()) {
        case MessageType::RequestVoteArgs:
            handleRequestVoteArgs(static_pointer_cast<RequestVoteArgs>(msg));
            break;
        case MessageType::RequestVoteReply:
            handleRequestVoteReply(static_pointer_cast<RequestVoteReply>(msg));
            break;
        case MessageType::AppendEntriesArgs:
            handleAppendEntriesArgs(static_pointer_cast<AppendEntriesArgs>(msg));
            break;
        case MessageType::AppendEntriesReply:
            handleAppendEntriesReply(static_pointer_cast<AppendEntriesReply>(msg));
            break;
    }
}

void RaftNode::handleRequestVoteArgs(shared_ptr<RequestVoteArgs> args) {
    auto reply = make_shared<RequestVoteReply>();
    reply->senderId = id;
    reply->receiverId = args->senderId;
    reply->voteGranted = false;

    // Election restriction (Raft 5.4.1): candidate's log must be at least as
    // up-to-date as ours. Compare last terms first, then lengths.
    bool candidateUpToDate =
        (args->lastLogTerm > lastLogTerm()) ||
        (args->lastLogTerm == lastLogTerm() && args->lastLogIndex >= lastLogIndex());

    if (args->term == currentTerm &&
        (votedFor == -1 || votedFor == args->candidateId) &&
        candidateUpToDate) {
        votedFor = args->candidateId;
        reply->voteGranted = true;
        resetElectionTimer();
        L().detail("  -> [Node " + to_string(id) + "] votes for Node " + to_string(args->candidateId) +
                   " in Term " + to_string(currentTerm));
    } else if (args->term == currentTerm && !candidateUpToDate) {
        L().detail("  -> [Node " + to_string(id) + "] REFUSES vote for Node " + to_string(args->candidateId) +
                   " (candidate's log is behind)");
    }

    reply->term = currentTerm;
    NetworkSimulator::getInstance().sendMessage(reply);
}

void RaftNode::handleRequestVoteReply(shared_ptr<RequestVoteReply> reply) {
    if (state == NodeState::Candidate && reply->term == currentTerm && reply->voteGranted) {
        votesReceived++;
        if (votesReceived >= majority()) becomeLeader();
    }
}

// ----------------------------------------------------------------------------
// Follower side of log replication
// ----------------------------------------------------------------------------
void RaftNode::handleAppendEntriesArgs(shared_ptr<AppendEntriesArgs> args) {
    auto reply = make_shared<AppendEntriesReply>();
    reply->senderId = id;
    reply->receiverId = args->senderId;
    reply->success = false;

    if (args->term < currentTerm) {
        // Message from an old leader (e.g. one that was partitioned away).
        // Reply with our higher term so it learns it must step down.
        reply->term = currentTerm;
        reply->lastLogIndex = lastLogIndex();
        NetworkSimulator::getInstance().sendMessage(reply);
        return;
    }

    // Valid leader for this term
    if (state != NodeState::Follower) becomeFollower(args->term);
    knownLeader = args->leaderId;
    resetElectionTimer();

    // 1. Log matching check: we must have the entry just before the new ones,
    //    with the same term. If not, refuse; the leader will back up and retry.
    if (args->prevLogIndex > lastLogIndex() ||
        log[args->prevLogIndex].term != args->prevLogTerm) {
        reply->term = currentTerm;
        reply->lastLogIndex = lastLogIndex();
        NetworkSimulator::getInstance().sendMessage(reply);
        return;
    }

    // 2. Append new entries. If an existing entry conflicts (same index,
    //    different term), delete it and everything after it.
    int index = args->prevLogIndex;
    for (const auto& entry : args->entries) {
        index++;
        if (index <= lastLogIndex()) {
            if (log[index].term == entry.term) continue;          // already have it
            L().log("  -> [Node " + to_string(id) + "] removes conflicting entries #" + to_string(index) +
                    "..#" + to_string(lastLogIndex()) + " (written by an old leader, never committed)");
            log.resize(index);
        }
        log.push_back(entry);
        if (entry.txn.type != TxnType::NOOP)
            L().detail("  -> [Node " + to_string(id) + "] appended #" + to_string(index) + ": " +
                       entry.txn.toString() + " (term " + to_string(entry.term) + ")");
    }

    // 3. Advance our commit index to what the leader has committed
    int lastNewIndex = args->prevLogIndex + (int)args->entries.size();
    if (args->leaderCommit > commitIndex) {
        commitIndex = min(args->leaderCommit, lastNewIndex);
        applyCommittedEntries();
    }

    reply->term = currentTerm;
    reply->success = true;
    reply->matchIndex = lastNewIndex;
    NetworkSimulator::getInstance().sendMessage(reply);
}

// ----------------------------------------------------------------------------
// Leader side of log replication
// ----------------------------------------------------------------------------
void RaftNode::handleAppendEntriesReply(shared_ptr<AppendEntriesReply> reply) {
    if (state != NodeState::Leader || reply->term != currentTerm) return;
    int peer = reply->senderId;

    if (reply->success) {
        matchIndex[peer] = max(matchIndex[peer], reply->matchIndex);
        nextIndex[peer] = matchIndex[peer] + 1;
        advanceCommitIndex();
    } else {
        // Follower's log didn't match: step back and try again right away
        nextIndex[peer] = max(1, min(nextIndex[peer] - 1, reply->lastLogIndex + 1));
        sendAppendEntries(peer);
    }
}

void RaftNode::sendAppendEntries(int peerId) {
    auto msg = make_shared<AppendEntriesArgs>();
    msg->senderId = id;
    msg->receiverId = peerId;
    msg->term = currentTerm;
    msg->leaderId = id;
    msg->prevLogIndex = nextIndex[peerId] - 1;
    msg->prevLogTerm = log[msg->prevLogIndex].term;
    msg->leaderCommit = commitIndex;
    // Send everything the follower is missing (empty = plain heartbeat)
    for (int i = nextIndex[peerId]; i <= lastLogIndex(); ++i)
        msg->entries.push_back(log[i]);
    NetworkSimulator::getInstance().sendMessage(msg);
}

void RaftNode::broadcastAppendEntries() {
    for (int peerId : NetworkSimulator::getInstance().getAllNodeIds())
        if (peerId != id) sendAppendEntries(peerId);
    nextHeartbeat = chrono::steady_clock::now() + chrono::milliseconds(HEARTBEAT_MS);
}

// An entry is committed once it is stored on a MAJORITY of nodes.
// (Phase II only printed "Majority 5/5" after a sleep - now we really count.)
void RaftNode::advanceCommitIndex() {
    matchIndex[id] = lastLogIndex();
    for (int n = lastLogIndex(); n > commitIndex; --n) {
        // Raft 5.4.2: a leader only commits entries from its OWN term by counting
        if (log[n].term != currentTerm) break;
        int count = 0;
        for (int i = 0; i < clusterSize; ++i)
            if (matchIndex[i] >= n) count++;
        if (count >= majority()) {
            int oldCommit = commitIndex;
            commitIndex = n;
            for (int k = oldCommit + 1; k <= n; ++k)
                if (log[k].txn.type != TxnType::NOOP)
                    L().log("[Consensus] " + tag() + " entry #" + to_string(k) + " stored on " +
                            to_string(count) + "/" + to_string(clusterSize) + " nodes (majority = " +
                            to_string(majority()) + ") -> COMMITTED");
            applyCommittedEntries();
            break;
        }
    }
}

// Apply committed entries to the ledger, strictly in log order
void RaftNode::applyCommittedEntries() {
    while (lastApplied < commitIndex) {
        lastApplied++;
        const LogEntry& e = log[lastApplied];
        string result = ledger.apply(e.txn);
        if (e.txn.type == TxnType::NOOP) continue;
        if (state == NodeState::Leader)
            L().log("[Ledger] " + tag() + " applied #" + to_string(lastApplied) + ": " + result);
    }
}

// ============================================================================
// Client API (called from the main thread, so every function takes the lock)
// ============================================================================

bool RaftNode::isLeader() const {
    lock_guard<mutex> lock(mtx);
    return state == NodeState::Leader && !crashed;
}

int RaftNode::getTerm() const {
    lock_guard<mutex> lock(mtx);
    return currentTerm;
}

NodeStatus RaftNode::getStatus() const {
    lock_guard<mutex> lock(mtx);
    return NodeStatus{id, state, currentTerm, lastLogIndex(), commitIndex, lastApplied, crashed, ledger};
}

vector<LogEntry> RaftNode::getCommittedLog() const {
    lock_guard<mutex> lock(mtx);
    return vector<LogEntry>(log.begin() + 1, log.begin() + 1 + commitIndex);
}

ProposeResult RaftNode::proposeTransaction(const Transaction& txn) {
    lock_guard<mutex> lock(mtx);
    ProposeResult r;
    if (state != NodeState::Leader || crashed) return r;

    LogEntry entry;
    entry.term = currentTerm;
    entry.txn = txn;
    log.push_back(entry);
    matchIndex[id] = lastLogIndex();

    r.accepted = true;
    r.index = lastLogIndex();
    r.term = currentTerm;

    L().log(tag() + " appended #" + to_string(r.index) + " to its log, replicating to followers...");
    broadcastAppendEntries();      // send now instead of waiting for the next heartbeat
    return r;
}

bool RaftNode::isCommitted(int index, int term, bool& lost) const {
    lock_guard<mutex> lock(mtx);
    lost = false;
    if (index > lastLogIndex() || log[index].term != term) {
        lost = true;               // a newer leader removed/overwrote this slot
        return false;
    }
    return index <= commitIndex;
}

double RaftNode::readBalance(const string& account, int& appliedIndex) const {
    lock_guard<mutex> lock(mtx);
    appliedIndex = lastApplied;
    return ledger.getBalance(account);
}

// ============================================================================
// Fault injection
// Phase II "step down" slept the MAIN thread, so the old leader kept running
// and always won the next election. A crash now really stops the node.
// ============================================================================

void RaftNode::crash() {
    lock_guard<mutex> lock(mtx);
    if (crashed) return;
    crashed = true;
    NetworkSimulator::getInstance().setCrashed(id, true);
    messageQueue->clear();
    L().log("!!! [Fault] " + tag() + " CRASHED (was " + stateName(state) + ")");
}

void RaftNode::recover() {
    lock_guard<mutex> lock(mtx);
    if (!crashed) return;
    // Volatile state is lost: rebuild the ledger from the log as entries get committed again.
    // currentTerm, votedFor and log are "persistent" and survive.
    state = NodeState::Follower;
    votesReceived = 0;
    commitIndex = 0;
    lastApplied = 0;
    knownLeader = -1;
    ledger.reset();
    crashed = false;
    resetElectionTimer();
    NetworkSimulator::getInstance().setCrashed(id, false);
    L().log("+++ [Recovery] " + tag() + " restarted as Follower with " + to_string(lastLogIndex()) +
            " log entries; will catch up from the leader");
}

bool RaftNode::isCrashed() const {
    lock_guard<mutex> lock(mtx);
    return crashed;
}
