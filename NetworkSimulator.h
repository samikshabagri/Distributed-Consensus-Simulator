#ifndef NETWORKSIMULATOR_H
#define NETWORKSIMULATOR_H

#include "ThreadSafeQueue.h"
#include "Message.h"
#include "ConsistentHash.h"
#include <unordered_map>
#include <memory>
#include <vector>

using namespace std;

class NetworkSimulator {
private:
    unordered_map<int, shared_ptr<ThreadSafeQueue<shared_ptr<Message>>>> nodeQueues;
    vector<int> nodeIds;
    ConsistentHash consistentHasher;

    // Private constructor for Singleton
    NetworkSimulator() = default;

public:
    // Singleton pattern
    static NetworkSimulator& getInstance() {
        static NetworkSimulator instance;
        return instance;
    }

    // Register a node and create its mailbox
    void registerNode(int nodeId) {
        nodeQueues[nodeId] = make_shared<ThreadSafeQueue<shared_ptr<Message>>>();
        nodeIds.push_back(nodeId);
        consistentHasher.addNode(nodeId);
    }

    // Route a message to the correct node's queue
    void sendMessage(shared_ptr<Message> msg) {
        auto it = nodeQueues.find(msg->receiverId);
        if (it != nodeQueues.end()) {
            it->second->push(msg);
        }
    }

    // Get a specific node's queue (used by the node itself to read messages)
    shared_ptr<ThreadSafeQueue<shared_ptr<Message>>> getQueue(int nodeId) {
        return nodeQueues[nodeId];
    }
    
    // Get list of all registered nodes
    const vector<int>& getAllNodeIds() const {
        return nodeIds;
    }

    // Route an incoming client request to the correct node using Consistent Hashing
    int getRouteForClient(const string& clientKey) {
        return consistentHasher.getClientRoute(clientKey);
    }
};

#endif // NETWORKSIMULATOR_H
