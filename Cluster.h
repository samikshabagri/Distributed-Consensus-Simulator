#ifndef CLUSTER_H
#define CLUSTER_H

#include "RaftNode.h"
#include "Logger.h"
#include "Metrics.h"
#include <vector>
#include <memory>
#include <sstream>
#include <iomanip>

using namespace std;

// ---------------------------------------------------------------------------
// Cluster: owns the nodes and plays the role of the "client" / test harness.
// Shared by main.cpp (demo scenarios) and tests.cpp (automated tests).
// ---------------------------------------------------------------------------
class Cluster {
private:
    int txnCounter = 0;

public:
    vector<unique_ptr<RaftNode>> nodes;

    explicit Cluster(int n) {
        for (int i = 0; i < n; ++i) nodes.push_back(make_unique<RaftNode>(i, n));
    }

    void start() {
        NetworkSimulator::getInstance().start();
        for (auto& node : nodes) node->start();
    }

    void stop() {
        for (auto& node : nodes) node->signalStop();   // tell every node first, so no one
        for (auto& node : nodes) node->stop();         // starts an election while others shut down
        NetworkSimulator::getInstance().stop();
    }

    // During a partition two nodes may both think they are leader (split-brain).
    // The real leader is the one with the HIGHEST term.
    RaftNode* findLeader() {
        RaftNode* best = nullptr;
        for (auto& node : nodes)
            if (node->isLeader() && (!best || node->getTerm() > best->getTerm())) best = node.get();
        return best;
    }

    // All nodes that currently believe they are leader
    vector<int> allLeaders() {
        vector<int> ids;
        for (auto& node : nodes) if (node->isLeader()) ids.push_back(node->getId());
        return ids;
    }

    // Wait until a leader exists (optionally: a leader other than 'notThisId')
    RaftNode* waitForLeader(int timeoutMs, int notThisId = -1) {
        auto end = chrono::steady_clock::now() + chrono::milliseconds(timeoutMs);
        while (chrono::steady_clock::now() < end) {
            RaftNode* l = findLeader();
            if (l && l->getId() != notThisId) return l;
            this_thread::sleep_for(chrono::milliseconds(20));
        }
        return nullptr;
    }

    // Client write: send to 'target' (normally the leader) and wait for commit.
    // Returns true if committed within timeoutMs.
    bool submit(RaftNode* target, const Transaction& txn, const string& scenario, int timeoutMs = 2000) {
        int txnId = ++txnCounter;
        Logger::get().divider();
        Logger::get().log("[Client] txn " + to_string(txnId) + ": " + txn.toString());

        if (!target) {
            Logger::get().log("[Client] no leader available - request refused");
            Metrics::get().recordTxn({txnId, scenario, txn.toString(), -1, 0, false, 0});
            return false;
        }

        int hashNode = NetworkSimulator::getInstance().getRouteForClient(txn.account);
        Logger::get().log("[Router] writes always go to the leader -> Node " + to_string(target->getId()) +
                          "  (account " + txn.account + " is owned by Node " + to_string(hashNode) +
                          " on the hash ring, used for reads)");

        auto start = chrono::steady_clock::now();
        ProposeResult pr = target->proposeTransaction(txn);
        if (!pr.accepted) {
            Logger::get().log("[Client] Node " + to_string(target->getId()) + " is not leader - refused");
            Metrics::get().recordTxn({txnId, scenario, txn.toString(), target->getId(), 0, false, 0});
            return false;
        }

        bool committed = false, lost = false;
        auto end = start + chrono::milliseconds(timeoutMs);
        while (chrono::steady_clock::now() < end) {
            if (target->isCommitted(pr.index, pr.term, lost)) { committed = true; break; }
            if (lost) break;
            this_thread::sleep_for(chrono::milliseconds(2));
        }
        long long ms = chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - start).count();

        if (committed)
            Logger::get().log("[Client] txn " + to_string(txnId) + " COMMITTED at log index #" +
                              to_string(pr.index) + " in " + to_string(ms) + " ms");
        else
            Logger::get().log("[Client] txn " + to_string(txnId) + " NOT committed after " + to_string(ms) +
                              " ms (leader could not reach a majority)");

        Metrics::get().recordTxn({txnId, scenario, txn.toString(), target->getId(), pr.index, committed, ms});
        return committed;
    }

    // Wait until every running node has applied everything the leader committed
    bool waitForConvergence(int timeoutMs) {
        auto end = chrono::steady_clock::now() + chrono::milliseconds(timeoutMs);
        while (chrono::steady_clock::now() < end) {
            RaftNode* l = findLeader();
            if (l) {
                int target = l->getStatus().commitIndex;
                bool all = true;
                for (auto& node : nodes) {
                    NodeStatus s = node->getStatus();
                    if (!s.crashed && s.lastApplied < target) all = false;
                }
                if (all) return true;
            }
            this_thread::sleep_for(chrono::milliseconds(20));
        }
        return false;
    }

    // State Machine Safety check:
    //  (1) committed logs must agree on every index both nodes have committed
    //  (2) nodes that applied the same number of entries must have identical ledgers
    bool checkConsistency(bool print = true) {
        bool ok = true;
        vector<vector<LogEntry>> logs;
        vector<NodeStatus> st;
        for (auto& node : nodes) { logs.push_back(node->getCommittedLog()); st.push_back(node->getStatus()); }

        for (size_t a = 0; a < nodes.size(); ++a)
            for (size_t b = a + 1; b < nodes.size(); ++b) {
                size_t common = min(logs[a].size(), logs[b].size());
                for (size_t i = 0; i < common; ++i)
                    if (logs[a][i].term != logs[b][i].term ||
                        logs[a][i].txn.toString() != logs[b][i].txn.toString()) ok = false;
                if (!st[a].crashed && !st[b].crashed && st[a].lastApplied == st[b].lastApplied &&
                    !(st[a].ledger == st[b].ledger)) ok = false;
            }
        if (print)
            Logger::get().log(string("[Check] Committed logs and ledgers identical on all nodes: ") +
                              (ok ? "PASS" : "FAIL"));
        return ok;
    }

    // CLI "trace view" of the whole cluster
    void printStatus(const string& title) {
        ostringstream out;
        out << "\n  " << title << "\n";
        out << "  +------+-----------+------+---------+--------+---------+----------------------------------------------------------+\n";
        out << "  | Node | State     | Term | Log len | Commit | Applied | Ledger (account:balance)                                 |\n";
        out << "  +------+-----------+------+---------+--------+---------+----------------------------------------------------------+\n";
        for (auto& node : nodes) {
            NodeStatus s = node->getStatus();
            ostringstream led;
            led << fixed << setprecision(0);
            for (auto& kv : s.ledger.getAll()) led << kv.first.substr(4) << ":" << kv.second << " ";
            string ledStr = led.str();
            if (ledStr.size() > 56) ledStr = ledStr.substr(0, 53) + "...";
            out << "  | " << setw(4) << s.id << " | " << left << setw(9)
                << (s.crashed ? "CRASHED" : stateName(s.state)) << right
                << " | " << setw(4) << s.term << " | " << setw(7) << s.lastLogIndex
                << " | " << setw(6) << s.commitIndex << " | " << setw(7) << s.lastApplied
                << " | " << left << setw(56) << ledStr << right << " |\n";
        }
        out << "  +------+-----------+------+---------+--------+---------+----------------------------------------------------------+";
        Logger::get().raw(out.str());
    }

    void sleepMs(int ms) { this_thread::sleep_for(chrono::milliseconds(ms)); }
};

// Small helpers to build transactions
inline Transaction deposit(const string& user, const string& acc, double amt) {
    Transaction t; t.type = TxnType::DEPOSIT; t.user = user; t.account = acc; t.amount = amt; return t;
}
inline Transaction withdraw(const string& user, const string& acc, double amt) {
    Transaction t; t.type = TxnType::WITHDRAW; t.user = user; t.account = acc; t.amount = amt; return t;
}
inline Transaction transfer(const string& user, const string& from, const string& to, double amt) {
    Transaction t; t.type = TxnType::TRANSFER; t.user = user; t.account = from; t.toAccount = to; t.amount = amt; return t;
}

#endif // CLUSTER_H
