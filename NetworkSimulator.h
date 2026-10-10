#ifndef NETWORKSIMULATOR_H
#define NETWORKSIMULATOR_H
#include "ThreadSafeQueue.h"
#include "Message.h"
#include <unordered_map>
#include <memory>
#include <vector>

class NetworkSimulator {
private:
    std::unordered_map<int, std::shared_ptr<ThreadSafeQueue<std::shared_ptr<Message>>>> nodeQueues;
    std::vector<int> nodeIds;

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
        nodeQueues[nodeId] = std::make_shared<ThreadSafeQueue<std::shared_ptr<Message>>>();
        nodeIds.push_back(nodeId);
    }

    // Route a message to the correct node's queue
    void sendMessage(std::shared_ptr<Message> msg) {
        auto it = nodeQueues.find(msg->receiverId);
        if (it != nodeQueues.end()) {
            it->second->push(msg);
        }
    }

    // Get a specific node's queue (used by the node itself to read messages)
    std::shared_ptr<ThreadSafeQueue<std::shared_ptr<Message>>> getQueue(int nodeId) {
        return nodeQueues[nodeId];
    }
    
    // Get list of all registered nodes
    const std::vector<int>& getAllNodeIds() const {
        return nodeIds;
    }
};

#endif // NETWORKSIMULATOR_H
