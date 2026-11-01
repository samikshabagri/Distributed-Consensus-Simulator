#ifndef NETWORKSIMULATOR_H
#define NETWORKSIMULATOR_H

#include "ThreadSafeQueue.h"
#include "Message.h"
#include "ConsistentHash.h"
#include "Metrics.h"
#include <unordered_map>
#include <memory>
#include <vector>
#include <set>
#include <queue>
#include <random>
#include <thread>
#include <atomic>

using namespace std;

// ---------------------------------------------------------------------------
// NetworkSimulator (Phase III)
//
// Still a Singleton that routes messages into each node's mailbox, but now
// it can also make the network misbehave (fault injection):
//
//   * Crashed nodes    - nothing is delivered to or from them
//   * Partitions       - nodes are split into groups; messages only flow
//                        inside a group (this creates "split-brain")
//   * Packet loss      - each message is dropped with probability p
//   * Delay            - each message waits a random min..max ms
//
// Delayed messages wait in a MIN-HEAP (std::priority_queue ordered by
// delivery time). A separate "network thread" sleeps until the earliest
// message is due and then puts it into the receiver's mailbox. This is the
// discrete-event min-heap idea from our Phase I proposal, reused here.
// ---------------------------------------------------------------------------
class NetworkSimulator {
private:
    using MsgPtr = shared_ptr<Message>;
    using Clock = chrono::steady_clock;

    struct DelayedMessage {
        Clock::time_point deliverAt;
        long long seq;          // tie-breaker so equal times stay FIFO
        MsgPtr msg;
    };
    // Comparator that turns priority_queue (a max-heap) into a min-heap
    struct LaterFirst {
        bool operator()(const DelayedMessage& a, const DelayedMessage& b) const {
            if (a.deliverAt != b.deliverAt) return a.deliverAt > b.deliverAt;
            return a.seq > b.seq;
        }
    };

    unordered_map<int, shared_ptr<ThreadSafeQueue<MsgPtr>>> nodeQueues;
    vector<int> nodeIds;
    ConsistentHash consistentHasher;

    // ---- fault injection state (protected by mtx) ----
    mutex mtx;
    condition_variable cv;
    set<int> crashedNodes;
    unordered_map<int, int> partitionGroup;   // nodeId -> group number (empty map = no partition)
    double lossRate = 0.0;
    int minDelayMs = 0, maxDelayMs = 0;
    mt19937 rng{12345};

    priority_queue<DelayedMessage, vector<DelayedMessage>, LaterFirst> inFlight;
    long long seqCounter = 0;
    thread networkThread;
    atomic<bool> running{false};

    NetworkSimulator() = default;

    // Must be called with mtx held
    bool canTalk(int a, int b) const {
        if (crashedNodes.count(a) || crashedNodes.count(b)) return false;
        if (!partitionGroup.empty()) {
            auto ga = partitionGroup.find(a), gb = partitionGroup.find(b);
            int groupA = (ga == partitionGroup.end()) ? 0 : ga->second;
            int groupB = (gb == partitionGroup.end()) ? 0 : gb->second;
            if (groupA != groupB) return false;
        }
        return true;
    }

    // Must be called with mtx held
    void deliverNow(const MsgPtr& msg) {
        auto it = nodeQueues.find(msg->receiverId);
        if (it != nodeQueues.end()) {
            it->second->push(msg);
            Metrics::get().messagesDelivered++;
        }
    }

    // Network thread: delivers delayed messages when their time comes
    void networkLoop() {
        unique_lock<mutex> lock(mtx);
        while (running) {
            if (inFlight.empty()) {
                cv.wait_for(lock, chrono::milliseconds(50));
                continue;
            }
            auto due = inFlight.top().deliverAt;
            if (Clock::now() < due) {
                cv.wait_until(lock, due);      // wakes early if a new message arrives
                continue;
            }
            DelayedMessage dm = inFlight.top();
            inFlight.pop();
            // Re-check at delivery time: a partition or crash may have happened meanwhile
            if (canTalk(dm.msg->senderId, dm.msg->receiverId)) deliverNow(dm.msg);
            else Metrics::get().messagesDropped++;
        }
    }

public:
    static NetworkSimulator& getInstance() {
        static NetworkSimulator instance;
        return instance;
    }

    ~NetworkSimulator() { stop(); }

    void start() {
        if (running) return;
        running = true;
        networkThread = thread(&NetworkSimulator::networkLoop, this);
    }

    void stop() {
        running = false;
        cv.notify_all();
        if (networkThread.joinable()) networkThread.join();
    }

    void registerNode(int nodeId) {
        lock_guard<mutex> lock(mtx);
        nodeQueues[nodeId] = make_shared<ThreadSafeQueue<MsgPtr>>();
        nodeIds.push_back(nodeId);
        consistentHasher.addNode(nodeId);
    }

    // Route a message, applying crash / partition / loss / delay rules
    void sendMessage(MsgPtr msg) {
        lock_guard<mutex> lock(mtx);
        Metrics::get().messagesSent++;

        if (!canTalk(msg->senderId, msg->receiverId)) {
            Metrics::get().messagesDropped++;
            return;
        }
        if (lossRate > 0.0) {
            uniform_real_distribution<double> coin(0.0, 1.0);
            if (coin(rng) < lossRate) {
                Metrics::get().messagesDropped++;
                return;
            }
        }
        if (maxDelayMs <= 0) {
            deliverNow(msg);            // fast path: no delay configured
            return;
        }
        uniform_int_distribution<int> d(minDelayMs, maxDelayMs);
        inFlight.push({Clock::now() + chrono::milliseconds(d(rng)), seqCounter++, msg});
        cv.notify_all();
    }

    shared_ptr<ThreadSafeQueue<MsgPtr>> getQueue(int nodeId) {
        lock_guard<mutex> lock(mtx);
        return nodeQueues[nodeId];
    }

    const vector<int>& getAllNodeIds() const { return nodeIds; }   // fixed after startup

    // ---------------- fault injection controls ----------------
    void setSeed(unsigned int seed) { lock_guard<mutex> lock(mtx); rng.seed(seed); }

    void setCrashed(int nodeId, bool crashed) {
        lock_guard<mutex> lock(mtx);
        if (crashed) crashedNodes.insert(nodeId);
        else crashedNodes.erase(nodeId);
    }

    // Example: setPartition({{0,1},{2,3,4}}) -> two groups that cannot talk
    void setPartition(const vector<vector<int>>& groups) {
        lock_guard<mutex> lock(mtx);
        partitionGroup.clear();
        for (size_t g = 0; g < groups.size(); ++g)
            for (int id : groups[g]) partitionGroup[id] = (int)g + 1;
    }

    void healPartition() {
        lock_guard<mutex> lock(mtx);
        partitionGroup.clear();
    }

    void setPacketLoss(double rate) { lock_guard<mutex> lock(mtx); lossRate = rate; }

    void setDelay(int minMs, int maxMs) {
        lock_guard<mutex> lock(mtx);
        minDelayMs = minMs;
        maxDelayMs = maxMs;
    }

    void resetFaults() {
        lock_guard<mutex> lock(mtx);
        crashedNodes.clear();
        partitionGroup.clear();
        lossRate = 0.0;
        minDelayMs = maxDelayMs = 0;
    }

    // ---------------- routing ----------------
    int getRouteForClient(const string& clientKey) {
        lock_guard<mutex> lock(mtx);
        return consistentHasher.getClientRoute(clientKey);
    }

    // CQRS read routing: pick a node by consistent hashing, skipping the leader
    // (writes only) and any crashed node.
    int getReadReplica(const string& key, int leaderId) {
        lock_guard<mutex> lock(mtx);
        set<int> excluded = crashedNodes;
        excluded.insert(leaderId);
        return consistentHasher.getRouteExcluding(key, excluded);
    }
};

#endif // NETWORKSIMULATOR_H
