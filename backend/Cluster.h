#ifndef CLUSTER_H
#define CLUSTER_H

#include "RaftNode.h"
#include "Logger.h"
#include <vector>
#include <memory>
#include <mutex>

using namespace std;

class Cluster {
private:
    int initialSize;
    mutable mutex nodesMtx;
    vector<unique_ptr<RaftNode>> nodes;

public:
    explicit Cluster(int n = 5) : initialSize(n) {
        for (int i = 0; i < n; ++i)
            nodes.push_back(make_unique<RaftNode>(i, initialSize, false));
    }

    void start() {
        lock_guard<mutex> lock(nodesMtx);
        for (auto& node : nodes) node->start();
    }

    void stop() {
        lock_guard<mutex> lock(nodesMtx);
        for (auto& node : nodes) node->signalStop();
        for (auto& node : nodes) node->stop();
    }

    RaftNode* findLeader() const {
        lock_guard<mutex> lock(nodesMtx);
        RaftNode* best = nullptr;
        for (const auto& node : nodes)
            if (node->isLeader() && (!best || node->getTerm() > best->getTerm()))
                best = node.get();
        return best;
    }

    RaftNode* waitForLeader(int timeoutMs) const {
        auto end = chrono::steady_clock::now() + chrono::milliseconds(timeoutMs);
        while (chrono::steady_clock::now() < end) {
            if (RaftNode* l = findLeader()) return l;
            this_thread::sleep_for(chrono::milliseconds(20));
        }
        return nullptr;
    }

    int addFollower() {
        lock_guard<mutex> lock(nodesMtx);
        int newId = (int)nodes.size();
        auto node = make_unique<RaftNode>(newId, initialSize, true);
        node->start();
        nodes.push_back(std::move(node));
        return newId;
    }

    int nodeCount() const {
        lock_guard<mutex> lock(nodesMtx);
        return (int)nodes.size();
    }

    int activeFollowerCount() const {
        lock_guard<mutex> lock(nodesMtx);
        int count = 0;
        for (const auto& node : nodes) {
            NodeStatus s = node->getStatus();
            if (!s.crashed && s.state != NodeState::Leader) count++;
        }
        return count;
    }

    bool submitWrite(const Transaction& txn, int timeoutMs = 2000) {
        RaftNode* leader = findLeader();
        if (!leader) return false;

        ProposeResult pr = leader->proposeTransaction(txn);
        if (!pr.accepted) return false;

        bool lost = false;
        auto end = chrono::steady_clock::now() + chrono::milliseconds(timeoutMs);
        while (chrono::steady_clock::now() < end) {
            if (leader->isCommitted(pr.index, pr.term, lost)) return true;
            if (lost) return false;
            this_thread::sleep_for(chrono::milliseconds(2));
        }
        return false;
    }

    double readFromNode(int nodeId, const string& account) {
        RaftNode* target = nullptr;
        {
            lock_guard<mutex> lock(nodesMtx);
            if (nodeId >= 0 && nodeId < (int)nodes.size())
                target = nodes[nodeId].get();
        }
        return target ? target->readBalance(account) : 0.0;
    }

    void crashNode(int id) {
        RaftNode* target = nullptr;
        {
            lock_guard<mutex> lock(nodesMtx);
            if (id >= 0 && id < (int)nodes.size()) target = nodes[id].get();
        }
        if (target) target->crash();
    }

    void recoverNode(int id) {
        RaftNode* target = nullptr;
        {
            lock_guard<mutex> lock(nodesMtx);
            if (id >= 0 && id < (int)nodes.size()) target = nodes[id].get();
        }
        if (target) target->recover();
    }

    vector<NodeStatus> getStatuses() const {
        lock_guard<mutex> lock(nodesMtx);
        vector<NodeStatus> list;
        for (const auto& node : nodes) list.push_back(node->getStatus());
        return list;
    }
};

#endif
