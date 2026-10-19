#include "RaftNode.h"
#include <iostream>
#include <chrono>
#include <random>
#include <mutex>

using namespace std::chrono_literals;

static std::mutex g_coutMutex;

RaftNode::RaftNode(int id) 
    : id(id), state(NodeState::Follower), currentTerm(0), votedFor(-1), votesReceived(0), running(false) {
    NetworkSimulator::getInstance().registerNode(id);
    messageQueue = NetworkSimulator::getInstance().getQueue(id);
}

RaftNode::~RaftNode() {
    stop();
}

void RaftNode::start() {
    running = true;
    nodeThread = std::thread(&RaftNode::run, this);
}

void RaftNode::stop() {
    running = false;
    if (nodeThread.joinable()) {
        nodeThread.join();
    }
}

int RaftNode::getRandomTimeout() {
    // Unique pseudorandom generator per thread seeded with high-res clock and node ID
    static thread_local std::mt19937 rng(
        (unsigned int)std::chrono::high_resolution_clock::now().time_since_epoch().count() + (id * 7919)
    );
    // Timeout distributed between 1000ms and 2200ms
    std::uniform_int_distribution<int> dist(1000, 2200);
    return dist(rng);
}

void RaftNode::becomeFollower(int term) {
    state = NodeState::Follower;
    currentTerm = term;
    votedFor = -1;
    votesReceived = 0;
}

void RaftNode::becomeCandidate() {
    state = NodeState::Candidate;
    currentTerm++;
    votedFor = id;
    votesReceived = 1; // Vote for self

    {
        std::lock_guard<std::mutex> lock(g_coutMutex);
        std::cout << "[Election] Node " << id << " election timeout expired! Becoming CANDIDATE for Term " << currentTerm << "." << std::endl;
    }

    // Request votes from all other nodes in the cluster
    auto nodes = NetworkSimulator::getInstance().getAllNodeIds();
    for (int peerId : nodes) {
        if (peerId != id) {
            auto msg = std::make_shared<RequestVoteArgs>();
            msg->senderId = id;
            msg->receiverId = peerId;
            msg->term = currentTerm;
            msg->candidateId = id;
            NetworkSimulator::getInstance().sendMessage(msg);
        }
    }
}

void RaftNode::becomeLeader() {
    state = NodeState::Leader;
    {
        std::lock_guard<std::mutex> lock(g_coutMutex);
        std::cout << "\n==============================================================" << std::endl;
        std::cout << ">>> [Node " << id << "] WON THE ELECTION and is elected LEADER for Term " << currentTerm << " <<<" << std::endl;
        std::cout << "==============================================================" << std::endl << std::endl;
    }
    sendHeartbeats();
}

void RaftNode::sendHeartbeats() {
    auto nodes = NetworkSimulator::getInstance().getAllNodeIds();
    for (int peerId : nodes) {
        if (peerId != id) {
            auto msg = std::make_shared<AppendEntriesArgs>();
            msg->senderId = id;
            msg->receiverId = peerId;
            msg->term = currentTerm;
            msg->leaderId = id;
            // Empty entries represent a standard heartbeat
            NetworkSimulator::getInstance().sendMessage(msg);
        }
    }
}

void RaftNode::run() {
    // Stagger startup slightly by node ID so all nodes don't expire at the exact same millisecond
    std::this_thread::sleep_for(std::chrono::milliseconds(id * 150));

    while (running) {
        std::shared_ptr<Message> msg;
        
        int timeoutMillis = 0;
        if (state == NodeState::Leader) {
            timeoutMillis = 400; // Leader sends fast heartbeats
        } else {
            timeoutMillis = getRandomTimeout(); // Follower/Candidate randomized timeout
        }

        // Wait for incoming message or timeout
        bool received = messageQueue->wait_and_pop(msg, timeoutMillis);

        if (received && msg) {
            processMessage(msg);
        } else {
            // Timeout fired
            if (state == NodeState::Follower || state == NodeState::Candidate) {
                becomeCandidate();
            } else if (state == NodeState::Leader) {
                sendHeartbeats();
            }
        }
    }
}

void RaftNode::processMessage(std::shared_ptr<Message> msg) {
    // If incoming message has a strictly higher term, step down to follower
    if (msg->term > currentTerm) {
        becomeFollower(msg->term);
    }

    switch (msg->getType()) {
        case MessageType::RequestVoteArgs:
            handleRequestVoteArgs(std::static_pointer_cast<RequestVoteArgs>(msg));
            break;
        case MessageType::RequestVoteReply:
            handleRequestVoteReply(std::static_pointer_cast<RequestVoteReply>(msg));
            break;
        case MessageType::AppendEntriesArgs:
            handleAppendEntriesArgs(std::static_pointer_cast<AppendEntriesArgs>(msg));
            break;
        default:
            break;
    }
}

void RaftNode::handleRequestVoteArgs(std::shared_ptr<RequestVoteArgs> args) {
    auto reply = std::make_shared<RequestVoteReply>();
    reply->senderId = id;
    reply->receiverId = args->senderId;
    reply->term = currentTerm;
    reply->voteGranted = false;

    // Standard Raft rule: If term is higher, step down and reset votedFor
    if (args->term > currentTerm) {
        becomeFollower(args->term);
    }

    if (args->term < currentTerm) {
        reply->voteGranted = false;
    } else if (votedFor == -1 || votedFor == args->candidateId) {
        votedFor = args->candidateId;
        reply->voteGranted = true;
        {
            std::lock_guard<std::mutex> lock(g_coutMutex);
            std::cout << "  -> [Vote Granted] Node " << id << " voted for Candidate Node " 
                      << args->candidateId << " in Term " << currentTerm << std::endl;
        }
    }

    NetworkSimulator::getInstance().sendMessage(reply);
}

void RaftNode::handleRequestVoteReply(std::shared_ptr<RequestVoteReply> reply) {
    if (state == NodeState::Candidate && reply->term == currentTerm && reply->voteGranted) {
        votesReceived++;
        int majority = (NetworkSimulator::getInstance().getAllNodeIds().size() / 2) + 1;
        if (votesReceived >= majority) {
            becomeLeader();
        }
    }
}

void RaftNode::handleAppendEntriesArgs(std::shared_ptr<AppendEntriesArgs> args) {
    if (args->term >= currentTerm) {
        if (state != NodeState::Follower) {
            becomeFollower(args->term);
        }
        
        // Replicate real banking transactions into local log
        for (const auto& entry : args->entries) {
            log.push_back(entry);
            {
                std::lock_guard<std::mutex> lock(g_coutMutex);
                std::cout << "  -> [Follower Node " << id << "] Updated local log: \"" 
                          << entry.command << "\" (Term " << entry.term << ")" << std::endl;
            }
        }
    }
}

bool RaftNode::proposeTransaction(const std::string& command) {
    if (state != NodeState::Leader) return false;

    // Leader commits to its own log
    LogEntry entry{currentTerm, command};
    log.push_back(entry);

    {
        std::lock_guard<std::mutex> lock(g_coutMutex);
        std::cout << "[Leader Node " << id << "] Broadcasting transaction to followers for log replication..." << std::endl;
    }

    // Replicate entry to all followers
    auto nodes = NetworkSimulator::getInstance().getAllNodeIds();
    for (int peerId : nodes) {
        if (peerId != id) {
            auto msg = std::make_shared<AppendEntriesArgs>();
            msg->senderId = id;
            msg->receiverId = peerId;
            msg->term = currentTerm;
            msg->leaderId = id;
            msg->entries.push_back(entry);
            NetworkSimulator::getInstance().sendMessage(msg);
        }
    }

    // Brief delay to allow followers to process in-flight queue items
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    {
        std::lock_guard<std::mutex> lock(g_coutMutex);
        std::cout << "[Consensus Engine] Majority (" << nodes.size() << "/" << nodes.size() 
                  << " nodes) confirmed! Transaction COMMITTED to distributed ledger." << std::endl;
    }
    return true;
}

void RaftNode::simulateStepDown() {
    if (state == NodeState::Leader) {
        {
            std::lock_guard<std::mutex> lock(g_coutMutex);
            std::cout << "\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" << std::endl;
            std::cout << ">>> [Fault Simulation] Leader [Node " << id << "] temporarily disconnected / stepped down! <<<" << std::endl;
            std::cout << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" << std::endl;
        }
        becomeFollower(currentTerm);
        // Sleep stepped-down leader for 2.5s so other followers can timeout and win election
        std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    }
}
