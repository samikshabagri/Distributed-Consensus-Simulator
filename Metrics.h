#ifndef METRICS_H
#define METRICS_H

#include <atomic>
#include <mutex>
#include <map>
#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <numeric>
#include "Logger.h"

using namespace std;

// ---------------------------------------------------------------------------
// Metrics (Phase III roadmap item 2: "performance metrics and latency logging")
//
// Counters use std::atomic because many node threads update them at once.
// The per-transaction records and the term->leader table use a mutex.
//
// Election Safety check: Raft guarantees AT MOST ONE leader per term.
// Every time a node becomes leader it records (term -> id) here. If a second,
// different node ever claims the same term, we flag a safety violation.
// ---------------------------------------------------------------------------
struct TxnRecord {
    int txnId;
    string scenario;
    string description;
    int leaderId;
    int logIndex;
    bool committed;
    long long latencyMs;
};

class Metrics {
private:
    mutex mtx;
    map<int, int> leaderOfTerm;
    bool safetyViolation = false;
    vector<TxnRecord> txns;
    vector<long long> failoverTimes;

    Metrics() = default;

public:
    atomic<long long> messagesSent{0};
    atomic<long long> messagesDropped{0};
    atomic<long long> messagesDelivered{0};
    atomic<int> electionsStarted{0};
    atomic<int> leadersElected{0};

    static Metrics& get() {
        static Metrics instance;
        return instance;
    }

    void recordLeader(int term, int nodeId) {
        lock_guard<mutex> lock(mtx);
        leadersElected++;
        auto it = leaderOfTerm.find(term);
        if (it != leaderOfTerm.end() && it->second != nodeId) {
            safetyViolation = true;
            Logger::get().log("!!! ELECTION SAFETY VIOLATION: two leaders in term " + to_string(term));
        }
        leaderOfTerm[term] = nodeId;
    }

    bool electionSafetyHolds() {
        lock_guard<mutex> lock(mtx);
        return !safetyViolation;
    }

    void recordTxn(const TxnRecord& r) {
        lock_guard<mutex> lock(mtx);
        txns.push_back(r);
    }

    void recordFailover(long long ms) {
        lock_guard<mutex> lock(mtx);
        failoverTimes.push_back(ms);
    }

    // Writes one CSV row per client transaction (useful for graphs in the report)
    bool writeCsv(const string& filename) {
        lock_guard<mutex> lock(mtx);
        ofstream f(filename);
        if (!f) return false;
        f << "txn_id,scenario,description,leader,log_index,committed,latency_ms\n";
        for (auto& r : txns)
            f << r.txnId << "," << r.scenario << ",\"" << r.description << "\"," << r.leaderId << ","
              << r.logIndex << "," << (r.committed ? "yes" : "no") << "," << r.latencyMs << "\n";
        return true;
    }

    void printSummary() {
        lock_guard<mutex> lock(mtx);
        vector<long long> lat;
        int committed = 0, failed = 0;
        for (auto& r : txns) {
            if (r.committed) { committed++; lat.push_back(r.latencyMs); }
            else failed++;
        }
        ostringstream out;
        out << "\n================== PERFORMANCE METRICS ==================\n";
        out << "  Client transactions submitted : " << txns.size() << "\n";
        out << "  Committed                     : " << committed << "\n";
        out << "  Not committed (no majority)   : " << failed << "\n";
        if (!lat.empty()) {
            sort(lat.begin(), lat.end());
            double avg = accumulate(lat.begin(), lat.end(), 0.0) / lat.size();
            out << fixed << setprecision(1);
            out << "  Commit latency  min/avg/max   : " << lat.front() << " / " << avg << " / " << lat.back() << " ms\n";
            out << "  Commit latency  median        : " << lat[lat.size() / 2] << " ms\n";
        }
        if (!failoverTimes.empty()) {
            double avg = accumulate(failoverTimes.begin(), failoverTimes.end(), 0.0) / failoverTimes.size();
            out << "  Leader failover time (avg)    : " << fixed << setprecision(0) << avg << " ms over "
                << failoverTimes.size() << " failover(s)\n";
        }
        out << "  Elections started             : " << electionsStarted << "\n";
        out << "  Leaders elected               : " << leadersElected << "\n";
        out << "  Messages sent / delivered     : " << messagesSent << " / " << messagesDelivered << "\n";
        out << "  Messages dropped (faults)     : " << messagesDropped << "\n";
        out << "  Election Safety (<=1 leader/term): " << (safetyViolation ? "VIOLATED" : "HOLDS") << "\n";
        out << "=========================================================";
        Logger::get().raw(out.str());
    }
};

#endif // METRICS_H
