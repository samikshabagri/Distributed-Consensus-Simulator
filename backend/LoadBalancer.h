#ifndef LOADBALANCER_H
#define LOADBALANCER_H

#include "Cluster.h"
#include <deque>
#include <thread>
#include <atomic>
#include <mutex>
#include <random>

using namespace std;

struct LoadSnapshot {
    int currentLoad = 0;
    int threshold = 0;
    long long totalReads = 0;
    long long totalWrites = 0;
    bool bursting = false;
    int burstSent = 0;
    int burstTotal = 0;
    string lastReadAcc;
    int lastReadNode = -1;
    double lastReadBal = 0.0;
    vector<int> loadHistory;
};

class LoadBalancer {
private:
    static constexpr int CAPACITY_PER_FOLLOWER = 120;
    static constexpr int MAX_NODES = 10;

    Cluster& cluster;
    mutable mutex mtx;

    atomic<long long> totalReads{0};
    atomic<long long> totalWrites{0};
    atomic<int> currentLoad{0};
    atomic<bool> running{false};
    atomic<bool> bursting{false};
    atomic<int> burstSent{0};
    atomic<int> burstTotal{0};

    deque<int> loadHistory;
    string lastReadAcc = "ACC_101";
    int lastReadNode = -1;
    double lastReadBal = 0.0;

    thread trafficThread;
    thread burstThread;

    void pushLoadSample(int value) {
        lock_guard<mutex> lock(mtx);
        loadHistory.push_back(value);
        if (loadHistory.size() > 24) loadHistory.pop_front();
    }

    void checkAutoScale() {
        int followers = cluster.activeFollowerCount();
        int threshold = max(1, followers) * CAPACITY_PER_FOLLOWER;
        if (currentLoad.load() > threshold && cluster.nodeCount() < MAX_NODES) {
            int newId = cluster.addFollower();
            Logger::get().log("[LoadBalancer] High load (" + to_string(currentLoad.load()) +
                              " > " + to_string(threshold) + ") -> Shot up Follower Node " + to_string(newId));
        }
    }

    void sendBackgroundTraffic(int count, int stepOffset) {
        const vector<string> accounts = {
            "ACC_101", "ACC_202", "ACC_303", "ACC_404",
            "ACC_505", "ACC_606", "ACC_707", "ACC_808"
        };
        for (int i = 0; i < count; ++i) {
            int idx = (stepOffset + i) % (int)accounts.size();
            if (i % 25 == 0) {
                Transaction t;
                t.type = TxnType::TRANSFER;
                t.user = "Traffic";
                t.account = accounts[idx];
                t.toAccount = accounts[(idx + 1) % (int)accounts.size()];
                t.amount = 1.0;
                if (cluster.submitWrite(t)) totalWrites++;
            } else {
                int fId = -1;
                double bal = 0;
                handleRead(accounts[idx], fId, bal, false);
            }
        }
    }

public:
    explicit LoadBalancer(Cluster& c) : cluster(c) {
        for (int i = 0; i < 24; ++i) loadHistory.push_back(0);
    }

    ~LoadBalancer() {
        stop();
    }

    // Continuous background traffic (~200-300 random messages/sec)
    void startContinuousTraffic() {
        if (running.exchange(true)) return;
        trafficThread = thread([this]() {
            mt19937 rng((unsigned int)chrono::steady_clock::now().time_since_epoch().count());
            uniform_int_distribution<int> dist(50, 75); // 4 ticks/sec -> ~200-300 msg/sec
            deque<int> recentTicks;
            int step = 0;

            while (running.load()) {
                this_thread::sleep_for(chrono::milliseconds(250));
                if (!running.load()) break;
                if (bursting.load()) continue;

                int n = dist(rng);
                sendBackgroundTraffic(n, step);
                step += n;

                recentTicks.push_back(n);
                if (recentTicks.size() > 4) recentTicks.pop_front();
                int load = 0;
                for (int v : recentTicks) load += v;

                currentLoad = load;
                pushLoadSample(load);
            }
        });
    }

    void stop() {
        running = false;
        if (trafficThread.joinable()) trafficThread.join();
        if (burstThread.joinable()) burstThread.join();
    }

    // All WRITES go strictly to the Raft Leader
    bool handleWrite(const Transaction& txn, int& leaderId) {
        RaftNode* leader = cluster.findLeader();
        leaderId = leader ? leader->getId() : -1;
        bool ok = cluster.submitWrite(txn);
        if (ok) {
            totalWrites++;
            currentLoad++;
            Logger::get().log("[Write -> Leader Node " + to_string(leaderId) + "] " + txn.toString());
        } else {
            Logger::get().log("[Write FAILED] No leader / could not commit");
        }
        return ok;
    }

    // All READS go strictly to a FOLLOWER selected by Consistent Hashing
    bool handleRead(const string& account, int& followerId, double& balance, bool logIt = true) {
        RaftNode* leader = cluster.findLeader();
        int leaderId = leader ? leader->getId() : -1;

        followerId = NetworkSimulator::getInstance().getReadReplica(account, leaderId);
        if (followerId < 0) return false;

        balance = cluster.readFromNode(followerId, account);
        totalReads++;
        currentLoad++;

        {
            lock_guard<mutex> lock(mtx);
            lastReadAcc = account;
            lastReadNode = followerId;
            lastReadBal = balance;
        }

        if (logIt) {
            ostringstream out;
            out << fixed << setprecision(2)
                << "[Read -> ConsistentHash -> Follower Node " << followerId
                << "] " << account << " = $" << balance;
            Logger::get().log(out.str());
        }
        return true;
    }

    // Send a large burst of messages at once to trigger load-driven follower scaling
    void startBurst(int totalMessages = 3000) {
        if (bursting.exchange(true)) return;
        if (burstThread.joinable()) burstThread.join();

        burstSent = 0;
        burstTotal = totalMessages;
        Logger::get().log("[LoadBalancer] Starting high-load burst of " + to_string(totalMessages) + " messages...");

        burstThread = thread([this, totalMessages]() {
            int waves = 20;
            int perWave = max(1, totalMessages / waves);
            deque<int> recentWaves;

            for (int w = 0; w < waves && running.load(); ++w) {
                int countThisWave = (w == waves - 1) ? (totalMessages - burstSent.load()) : perWave;
                sendBackgroundTraffic(countThisWave, w * perWave);
                burstSent += countThisWave;

                recentWaves.push_back(countThisWave);
                if (recentWaves.size() > 6) recentWaves.pop_front();
                int windowLoad = 0;
                for (int v : recentWaves) windowLoad += v;

                currentLoad = windowLoad;
                pushLoadSample(windowLoad);
                checkAutoScale();
                this_thread::sleep_for(chrono::milliseconds(140));
            }

            // Smoothly step down back to continuous baseline
            for (int i = 0; i < 3 && running.load(); ++i) {
                int decayed = max(240, currentLoad.load() / 2);
                currentLoad = decayed;
                pushLoadSample(decayed);
                this_thread::sleep_for(chrono::milliseconds(150));
            }
            Logger::get().log("[LoadBalancer] Burst complete (" + to_string(totalMessages) + " messages processed)");
            bursting = false;
        });
    }

    LoadSnapshot getSnapshot() const {
        lock_guard<mutex> lock(mtx);
        int followers = max(1, cluster.activeFollowerCount());
        LoadSnapshot s;
        s.currentLoad = currentLoad.load();
        s.threshold = followers * CAPACITY_PER_FOLLOWER;
        s.totalReads = totalReads.load();
        s.totalWrites = totalWrites.load();
        s.bursting = bursting.load();
        s.burstSent = burstSent.load();
        s.burstTotal = burstTotal.load();
        s.lastReadAcc = lastReadAcc;
        s.lastReadNode = lastReadNode;
        s.lastReadBal = lastReadBal;
        s.loadHistory.assign(loadHistory.begin(), loadHistory.end());
        return s;
    }
};

#endif
