// ============================================================================
//  Distributed Consensus Simulator - Phase III
//  Raft consensus + Consistent Hashing + replicated banking ledger
//
//  Usage:  raft_sim [scenario] [--seed N] [--quiet]
//      scenario 0 = run all (default)
//               1 = normal operation (log replication + majority commit)
//               2 = leader crash, failover and recovery / catch-up
//               3 = network partition (split-brain) and healing
//               4 = unreliable network (packet loss + delay)
//               5 = CQRS reads from followers via the hash ring
// ============================================================================
#include "Cluster.h"
#include <cstdlib>
#include <cstring>

using namespace std;

static void banner(const string& title) {
    Logger::get().raw("\n######################################################################");
    Logger::get().raw("#  " + title);
    Logger::get().raw("######################################################################");
}

// ----------------------------------------------------------------------------
// Scenario 1: normal operation
// ----------------------------------------------------------------------------
static void scenarioNormal(Cluster& c) {
    banner("SCENARIO 1: Normal operation - log replication and majority commit");
    RaftNode* leader = c.waitForLeader(5000);
    c.submit(leader, deposit("Alice", "ACC_101", 500), "1-normal");
    c.submit(leader, deposit("Bob", "ACC_202", 300), "1-normal");
    c.submit(leader, deposit("Charlie", "ACC_303", 1200), "1-normal");
    c.submit(leader, transfer("Charlie", "ACC_303", "ACC_101", 250), "1-normal");
    c.submit(leader, withdraw("Bob", "ACC_202", 150), "1-normal");
    // Deliberately invalid: Bob only has $150 left. Committed to the log, but every
    // replica's ledger REJECTS it in the same way (deterministic state machine).
    c.submit(leader, withdraw("Bob", "ACC_202", 900), "1-normal");

    c.waitForConvergence(3000);
    c.printStatus("Cluster after Scenario 1");
    c.checkConsistency();
}

// ----------------------------------------------------------------------------
// Scenario 2: leader crash -> a DIFFERENT node is elected -> old leader
// recovers and catches up through log replication
// ----------------------------------------------------------------------------
static void scenarioCrash(Cluster& c) {
    banner("SCENARIO 2: Leader crash, failover and recovery");
    RaftNode* oldLeader = c.waitForLeader(5000);
    if (!oldLeader) return;
    int oldId = oldLeader->getId();

    auto t0 = chrono::steady_clock::now();
    oldLeader->crash();
    RaftNode* newLeader = c.waitForLeader(6000, oldId);
    long long failover = chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - t0).count();
    if (!newLeader) { Logger::get().log("No new leader elected!"); return; }
    Metrics::get().recordFailover(failover);
    Logger::get().log("[Failover] Node " + to_string(newLeader->getId()) + " took over from crashed Node " +
                      to_string(oldId) + " in " + to_string(failover) + " ms");

    c.submit(newLeader, deposit("David", "ACC_404", 800), "2-crash");
    c.submit(newLeader, transfer("Alice", "ACC_101", "ACC_404", 100), "2-crash");
    c.printStatus("While Node " + to_string(oldId) + " is down (4/5 nodes still form a majority)");

    oldLeader->recover();
    c.waitForConvergence(4000);
    c.printStatus("After Node " + to_string(oldId) + " recovered and caught up");
    c.checkConsistency();
}

// ----------------------------------------------------------------------------
// Scenario 3: network partition / split-brain
//   minority side = {leader, one follower}  -> cannot commit
//   majority side = other three nodes       -> elects new leader, commits
//   heal -> old leader sees higher term, steps down, its uncommitted entry is erased
// ----------------------------------------------------------------------------
static void scenarioPartition(Cluster& c) {
    banner("SCENARIO 3: Network partition (split-brain) and healing");
    RaftNode* oldLeader = c.waitForLeader(5000);
    if (!oldLeader) return;
    int oldId = oldLeader->getId();
    int n = (int)c.nodes.size();

    vector<int> minority = {oldId, (oldId + 1) % n};
    vector<int> majority;
    for (int i = 0; i < n; ++i)
        if (i != minority[0] && i != minority[1]) majority.push_back(i);

    NetworkSimulator::getInstance().setPartition({minority, majority});
    Logger::get().log("!!! [Fault] NETWORK PARTITION: {" + to_string(minority[0]) + "," + to_string(minority[1]) +
                      "}  |  {" + to_string(majority[0]) + "," + to_string(majority[1]) + "," +
                      to_string(majority[2]) + "}");

    // Client still talks to the old leader, which cannot reach a majority
    c.submit(oldLeader, deposit("Eve", "ACC_505", 9999), "3-partition", 2500);

    RaftNode* newLeader = c.waitForLeader(6000, oldId);
    if (newLeader) {
        vector<int> leaders = c.allLeaders();
        string ids;
        for (int id : leaders) ids += to_string(id) + " ";
        Logger::get().log("[Split-brain] nodes that believe they are leader: " + ids +
                          "-> only Node " + to_string(newLeader->getId()) + " (term " +
                          to_string(newLeader->getTerm()) + ") has a majority and can commit");
        c.submit(newLeader, deposit("Frank", "ACC_505", 400), "3-partition");
        c.submit(newLeader, withdraw("Frank", "ACC_505", 50), "3-partition");
    }
    c.printStatus("During the partition");

    NetworkSimulator::getInstance().healPartition();
    Logger::get().log("+++ [Recovery] partition healed");
    c.waitForConvergence(4000);
    c.sleepMs(300);
    c.printStatus("After healing (Eve's $9999 deposit was never committed, so it is gone everywhere)");
    c.checkConsistency();
}

// ----------------------------------------------------------------------------
// Scenario 4: unreliable network
// ----------------------------------------------------------------------------
static void scenarioUnreliable(Cluster& c) {
    banner("SCENARIO 4: Unreliable network - 20% packet loss, 20-120 ms delay");
    NetworkSimulator::getInstance().setPacketLoss(0.20);
    NetworkSimulator::getInstance().setDelay(20, 120);

    vector<Transaction> txns = {
        deposit("Grace", "ACC_606", 700), transfer("Grace", "ACC_606", "ACC_202", 200),
        deposit("Hannah", "ACC_707", 50), withdraw("Grace", "ACC_606", 100),
        transfer("David", "ACC_404", "ACC_707", 300)};
    for (auto& t : txns) c.submit(c.waitForLeader(6000), t, "4-unreliable", 4000);

    NetworkSimulator::getInstance().setPacketLoss(0.0);
    NetworkSimulator::getInstance().setDelay(0, 0);
    Logger::get().log("+++ network back to normal");
    c.waitForConvergence(4000);
    c.printStatus("After Scenario 4");
    c.checkConsistency();
}

// ----------------------------------------------------------------------------
// Scenario 5: CQRS - writes go to the leader, reads are spread over followers
// ----------------------------------------------------------------------------
static void scenarioReads(Cluster& c) {
    banner("SCENARIO 5: CQRS - reads served by followers chosen by the hash ring");
    RaftNode* leader = c.waitForLeader(5000);
    if (!leader) return;

    // A write immediately followed by a follower read can show a STALE value:
    // the follower only applies the entry after the leader's next message tells
    // it the new commit index (eventual consistency for reads).
    c.submit(leader, deposit("Alice", "ACC_101", 1), "5-cqrs");

    vector<string> accounts = {"ACC_101", "ACC_202", "ACC_303", "ACC_404", "ACC_505", "ACC_606", "ACC_707"};
    int leaderCommit = leader->getStatus().commitIndex;
    for (auto& acc : accounts) {
        int replica = NetworkSimulator::getInstance().getReadReplica(acc, leader->getId());
        int applied = 0;
        double bal = c.nodes[replica]->readBalance(acc, applied);
        ostringstream out;
        out << fixed << setprecision(2) << "[Read] " << acc << " -> hash ring -> Node " << replica
            << " (follower): balance $" << bal << "   [replica applied #" << applied
            << ", leader committed #" << leaderCommit << (applied < leaderCommit ? " -> STALE read]" : " -> up to date]");
        Logger::get().log(out.str());
    }
    c.waitForConvergence(2000);
    int applied = 0;
    int replica = NetworkSimulator::getInstance().getReadReplica("ACC_101", leader->getId());
    double bal = c.nodes[replica]->readBalance("ACC_101", applied);
    ostringstream out;
    out << fixed << setprecision(2) << "[Read] after one heartbeat: ACC_101 on Node " << replica << " = $" << bal
        << " (applied #" << applied << ")";
    Logger::get().log(out.str());
}

int main(int argc, char* argv[]) {
    int scenario = 0;
    unsigned int seed = 42;
    for (int i = 1; i < argc; ++i) {
        if (!strcmp(argv[i], "--seed") && i + 1 < argc) seed = (unsigned int)atoi(argv[++i]);
        else if (!strcmp(argv[i], "--quiet")) Logger::get().setVerbose(false);
        else scenario = atoi(argv[i]);
    }

    Logger::get().raw("===============================================================");
    Logger::get().raw("   Distributed Consensus Banking Simulator - PHASE III         ");
    Logger::get().raw("   Raft (election + log replication) + Consistent Hashing      ");
    Logger::get().raw("===============================================================");
    Logger::get().raw("Fault-injection seed: " + to_string(seed) + "   (run with --seed N to change)");

    NetworkSimulator::getInstance().setSeed(seed);
    Cluster cluster(5);
    cluster.start();
    Logger::get().log("Cluster of 5 nodes started. Waiting for the first leader election...");

    // Scenarios 2-5 need some accounts to exist, so scenario 1 always runs first
    scenarioNormal(cluster);
    if (scenario == 0 || scenario == 2) scenarioCrash(cluster);
    if (scenario == 0 || scenario == 3) scenarioPartition(cluster);
    if (scenario == 0 || scenario == 4) scenarioUnreliable(cluster);
    if (scenario == 0 || scenario == 5) scenarioReads(cluster);

    NetworkSimulator::getInstance().resetFaults();
    cluster.waitForConvergence(3000);
    cluster.printStatus("FINAL cluster state");
    bool consistent = cluster.checkConsistency();

    Metrics::get().printSummary();
    if (Metrics::get().writeCsv("results.csv"))
        Logger::get().raw("Per-transaction results written to results.csv");

    cluster.stop();
    return (consistent && Metrics::get().electionSafetyHolds()) ? 0 : 1;
}
