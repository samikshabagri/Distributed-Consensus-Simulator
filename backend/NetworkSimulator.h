#ifndef NETWORKSIMULATOR_H
#define NETWORKSIMULATOR_H

#include "ThreadSafeQueue.h"
#include "Message.h"
#include "ConsistentHash.h"
#include <unordered_map>
#include <memory>
#include <vector>
#include <set>
#include <mutex>

using namespace std;

// Simple in-memory network router + Consistent Hashing for follower read routing
class NetworkSimulator {
private:
    using MsgPtr = shared_ptr<Message>;

    unordered_map<int, shared_ptr<ThreadSafeQueue<MsgPtr>>> nodeQueues;
    vector<int> nodeIds;
    set<int> crashedNodes;
    ConsistentHash consistentHasher;
    mutable mutex mtx;

    NetworkSimulator() = default;

public:
    static NetworkSimulator& getInstance() {
        static NetworkSimulator instance;
        return instance;
    }

    void registerNode(int nodeId) {
        lock_guard<mutex> lock(mtx);
        nodeQueues[nodeId] = make_shared<ThreadSafeQueue<MsgPtr>>();
        nodeIds.push_back(nodeId);
        consistentHasher.addNode(nodeId);
    }

    void sendMessage(MsgPtr msg) {
        lock_guard<mutex> lock(mtx);
        if (crashedNodes.count(msg->senderId) || crashedNodes.count(msg->receiverId))
            return;
        auto it = nodeQueues.find(msg->receiverId);
        if (it != nodeQueues.end()) it->second->push(msg);
    }

    shared_ptr<ThreadSafeQueue<MsgPtr>> getQueue(int nodeId) {
        lock_guard<mutex> lock(mtx);
        return nodeQueues[nodeId];
    }

    vector<int> getAllNodeIds() const {
        lock_guard<mutex> lock(mtx);
        return nodeIds;
    }

    void setCrashed(int nodeId, bool crashed) {
        lock_guard<mutex> lock(mtx);
        if (crashed) crashedNodes.insert(nodeId);
        else crashedNodes.erase(nodeId);
    }

    // Consistent hashing read routing: pick a healthy FOLLOWER (skip leader & crashed nodes)
    int getReadReplica(const string& key, int leaderId) const {
        lock_guard<mutex> lock(mtx);
        set<int> excluded = crashedNodes;
        if (leaderId >= 0) excluded.insert(leaderId);
        return consistentHasher.getRouteExcluding(key, excluded);
    }
};

#endif
