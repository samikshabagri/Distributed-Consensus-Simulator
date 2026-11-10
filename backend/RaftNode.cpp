#include "RaftNode.h"
#include "Logger.h"
#include <algorithm>

using namespace std;

string stateName(NodeState s) {
    switch (s) {
        case NodeState::Follower:  return "Follower";
        case NodeState::Candidate: return "Candidate";
        case NodeState::Leader:    return "LEADER";
    }
    return "Follower";
}

RaftNode::RaftNode(int id, int quorumSize, bool autoScaled)
    : id(id), quorumSize(quorumSize), autoScaled(autoScaled),
      currentTerm(0), votedFor(-1), state(NodeState::Follower),
      votesReceived(0), commitIndex(0), lastApplied(0),
      nextIndex(quorumSize, 1), matchIndex(quorumSize, 0),
      rng((unsigned int)chrono::high_resolution_clock::now().time_since_epoch().count() + id * 7919),
      running(false), crashed(false) {
    log.push_back(LogEntry{}); // dummy index 0
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

void RaftNode::resetElectionTimer() {
    uniform_int_distribution<int> dist(ELECTION_MIN_MS, ELECTION_MAX_MS);
    electionDeadline = chrono::steady_clock::now() + chrono::milliseconds(dist(rng));
}

void RaftNode::ensurePeer(int peerId) {
    if (peerId >= (int)nextIndex.size()) {
        nextIndex.resize(peerId + 1, 1);
        matchIndex.resize(peerId + 1, 0);
    }
}

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
        if (crashed) {
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
        if (now >= nextHeartbeat) broadcastAppendEntries();
    } else if (now >= electionDeadline) {
        becomeCandidate();
    }
}

void RaftNode::becomeFollower(int term) {
    if (term > currentTerm) {
        currentTerm = term;
        votedFor = -1;
    }
    state = NodeState::Follower;
    votesReceived = 0;
}

void RaftNode::becomeCandidate() {
    state = NodeState::Candidate;
    currentTerm++;
    votedFor = id;
    votesReceived = 1;
    resetElectionTimer();

    Logger::get().log(tag() + " election timeout -> CANDIDATE");

    for (int peerId : NetworkSimulator::getInstance().getAllNodeIds()) {
        if (peerId == id) continue;
        auto msg = make_shared<RequestVoteArgs>();
        msg->senderId = id;
        msg->receiverId = peerId;
        msg->term = currentTerm;
        msg->candidateId = id;
        msg->lastLogIndex = lastLogIndex();
        msg->lastLogTerm = lastLogTerm();
        NetworkSimulator::getInstance().sendMessage(msg);
    }
    if (votesReceived >= majority()) becomeLeader();
}

void RaftNode::becomeLeader() {
    state = NodeState::Leader;
    vector<int> allIds = NetworkSimulator::getInstance().getAllNodeIds();
    int maxId = id;
    for (int pid : allIds) maxId = max(maxId, pid);
    nextIndex.assign(maxId + 1, lastLogIndex() + 1);
    matchIndex.assign(maxId + 1, 0);

    Logger::get().log(">>> " + tag() + " WON ELECTION -> LEADER <<<");

    LogEntry noop;
    noop.term = currentTerm;
    log.push_back(noop);
    matchIndex[id] = lastLogIndex();

    broadcastAppendEntries();
}

void RaftNode::processMessage(shared_ptr<Message> msg) {
    if (msg->term > currentTerm) {
        if (state == NodeState::Leader)
            Logger::get().log(tag() + " stepped down (saw higher term " + to_string(msg->term) + ")");
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

    bool upToDate = (args->lastLogTerm > lastLogTerm()) ||
                    (args->lastLogTerm == lastLogTerm() && args->lastLogIndex >= lastLogIndex());

    if (args->term == currentTerm &&
        (votedFor == -1 || votedFor == args->candidateId) &&
        upToDate) {
        votedFor = args->candidateId;
        reply->voteGranted = true;
        resetElectionTimer();
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

void RaftNode::handleAppendEntriesArgs(shared_ptr<AppendEntriesArgs> args) {
    auto reply = make_shared<AppendEntriesReply>();
    reply->senderId = id;
    reply->receiverId = args->senderId;
    reply->success = false;

    if (args->term < currentTerm) {
        reply->term = currentTerm;
        reply->lastLogIndex = lastLogIndex();
        NetworkSimulator::getInstance().sendMessage(reply);
        return;
    }

    if (state != NodeState::Follower) becomeFollower(args->term);
    resetElectionTimer();

    if (args->prevLogIndex > lastLogIndex() ||
        log[args->prevLogIndex].term != args->prevLogTerm) {
        reply->term = currentTerm;
        reply->lastLogIndex = lastLogIndex();
        NetworkSimulator::getInstance().sendMessage(reply);
        return;
    }

    int index = args->prevLogIndex;
    for (const auto& entry : args->entries) {
        index++;
        if (index <= lastLogIndex()) {
            if (log[index].term == entry.term) continue;
            log.resize(index);
        }
        log.push_back(entry);
    }

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

void RaftNode::handleAppendEntriesReply(shared_ptr<AppendEntriesReply> reply) {
    if (state != NodeState::Leader || reply->term != currentTerm) return;
    int peer = reply->senderId;
    ensurePeer(peer);

    if (reply->success) {
        matchIndex[peer] = max(matchIndex[peer], reply->matchIndex);
        nextIndex[peer] = matchIndex[peer] + 1;
        advanceCommitIndex();
    } else {
        nextIndex[peer] = max(1, min(nextIndex[peer] - 1, reply->lastLogIndex + 1));
        sendAppendEntries(peer);
    }
}

void RaftNode::sendAppendEntries(int peerId) {
    ensurePeer(peerId);
    auto msg = make_shared<AppendEntriesArgs>();
    msg->senderId = id;
    msg->receiverId = peerId;
    msg->term = currentTerm;
    msg->leaderId = id;
    msg->prevLogIndex = nextIndex[peerId] - 1;
    msg->prevLogTerm = log[msg->prevLogIndex].term;
    msg->leaderCommit = commitIndex;
    for (int i = nextIndex[peerId]; i <= lastLogIndex(); ++i)
        msg->entries.push_back(log[i]);
    NetworkSimulator::getInstance().sendMessage(msg);
}

void RaftNode::broadcastAppendEntries() {
    for (int peerId : NetworkSimulator::getInstance().getAllNodeIds())
        if (peerId != id) sendAppendEntries(peerId);
    nextHeartbeat = chrono::steady_clock::now() + chrono::milliseconds(HEARTBEAT_MS);
}

void RaftNode::advanceCommitIndex() {
    ensurePeer(id);
    matchIndex[id] = lastLogIndex();
    for (int n = lastLogIndex(); n > commitIndex; --n) {
        if (log[n].term != currentTerm) break;
        int count = 0;
        for (size_t i = 0; i < matchIndex.size(); ++i)
            if (matchIndex[i] >= n) count++;
        if (count >= majority()) {
            commitIndex = n;
            applyCommittedEntries();
            break;
        }
    }
}

void RaftNode::applyCommittedEntries() {
    while (lastApplied < commitIndex) {
        lastApplied++;
        ledger.apply(log[lastApplied].txn);
    }
}

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
    return NodeStatus{
        id, state, currentTerm, lastLogIndex(), commitIndex, lastApplied,
        crashed, autoScaled, readsHandled.load(), writesHandled.load(), ledger
    };
}

ProposeResult RaftNode::proposeTransaction(const Transaction& txn) {
    lock_guard<mutex> lock(mtx);
    ProposeResult r;
    if (state != NodeState::Leader || crashed) return r;

    LogEntry entry;
    entry.term = currentTerm;
    entry.txn = txn;
    log.push_back(entry);
    ensurePeer(id);
    matchIndex[id] = lastLogIndex();
    writesHandled++;

    r.accepted = true;
    r.index = lastLogIndex();
    r.term = currentTerm;

    broadcastAppendEntries();
    return r;
}

bool RaftNode::isCommitted(int index, int term, bool& lost) const {
    lock_guard<mutex> lock(mtx);
    lost = false;
    if (index > lastLogIndex() || log[index].term != term) {
        lost = true;
        return false;
    }
    return index <= commitIndex;
}

double RaftNode::readBalance(const string& account) {
    readsHandled++;
    lock_guard<mutex> lock(mtx);
    return ledger.getBalance(account);
}

void RaftNode::crash() {
    lock_guard<mutex> lock(mtx);
    if (crashed) return;
    crashed = true;
    NetworkSimulator::getInstance().setCrashed(id, true);
    messageQueue->clear();
    Logger::get().log("!!! [Crash] " + tag() + " crashed");
}

void RaftNode::recover() {
    lock_guard<mutex> lock(mtx);
    if (!crashed) return;
    state = NodeState::Follower;
    votesReceived = 0;
    commitIndex = 0;
    lastApplied = 0;
    ledger.reset();
    crashed = false;
    resetElectionTimer();
    NetworkSimulator::getInstance().setCrashed(id, false);
    Logger::get().log("+++ [Recover] " + tag() + " restarted as Follower");
}

bool RaftNode::isCrashed() const {
    lock_guard<mutex> lock(mtx);
    return crashed;
}
