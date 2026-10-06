#include "RaftNode.h"
#include <iostream>
#include <chrono>

using namespace std::chrono_literals;

RaftNode::RaftNode(int id) : id(id), state(NodeState::Follower), currentTerm(0), votedFor(-1), votesReceived(0), running(false) {
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
    // Generate a random timeout between 150ms and 300ms
    static std::random_device rd;
    static std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(150, 300);
    return dis(gen);
}

void RaftNode::becomeFollower(int term) {
    state = NodeState::Follower;
    currentTerm = term;
    votedFor = -1;
    std::cout << "[Node " << id << "] became FOLLOWER for term " << currentTerm << "\n";
}

void RaftNode::becomeCandidate() {
    state = NodeState::Candidate;
    currentTerm++;
    votedFor = id;
    votesReceived = 1; // Vote for self
    std::cout << "[Node " << id << "] became CANDIDATE for term " << currentTerm << "\n";

    // Request votes from all other nodes
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
    std::cout << ">>> [Node " << id << "] became LEADER for term " << currentTerm << " <<<\n";
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
            NetworkSimulator::getInstance().sendMessage(msg);
        }
    }
}

void RaftNode::run() {
    while (running) {
        std::shared_ptr<Message> msg;
        
        int timeoutMillis = 0;
        if (state == NodeState::Leader) {
            timeoutMillis = 50; // Fast heartbeats
        } else {
            timeoutMillis = getRandomTimeout(); // Randomized election timeout
        }

        // Wait for a message or timeout
        bool received = messageQueue->wait_and_pop(msg, std::chrono::milliseconds(timeoutMillis));

        if (received && msg) {
            processMessage(msg);
        } else {
            // Timeout occurred
            if (state == NodeState::Follower || state == NodeState::Candidate) {
                // Election timeout: start a new election
                becomeCandidate();
            } else if (state == NodeState::Leader) {
                // Heartbeat timeout: send heartbeats to maintain leadership
                sendHeartbeats();
            }
        }
    }
}

void RaftNode::processMessage(std::shared_ptr<Message> msg) {
    // If a message has a higher term, immediately step down to follower
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

    if (args->term < currentTerm) {
        reply->voteGranted = false;
    } else if (votedFor == -1 || votedFor == args->candidateId) {
        votedFor = args->candidateId;
        reply->voteGranted = true;
        std::cout << "[Node " << id << "] voted for Node " << args->candidateId << " in term " << currentTerm << "\n";
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
        // Valid heartbeat from leader, receiving this message automatically 
        // resets the election timeout in the main run() loop.
    }
}
