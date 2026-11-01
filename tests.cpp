// ============================================================================
//  Automated tests - Phase III roadmap item 3
//  "Formalize unit test cases for Hash Ring and Election"
//
//  Build:  g++ -std=c++17 -pthread tests.cpp RaftNode.cpp -o raft_tests
//  Run:    raft_tests          (exit code 0 = all passed)
// ============================================================================
#include "Cluster.h"
#include <cmath>

using namespace std;

static int passed = 0, failed = 0;

static void check(bool condition, const string& name) {
    if (condition) { passed++; Logger::get().raw("  [PASS] " + name); }
    else           { failed++; Logger::get().raw("  [FAIL] " + name); }
}

// ---------------------------------------------------------------------------
// Unit tests: Consistent Hash ring
// ---------------------------------------------------------------------------
static void testHashRing() {
    Logger::get().raw("\n== Consistent Hash Ring ==");
    ConsistentHash ring;
    for (int i = 0; i < 5; ++i) ring.addNode(i);

    check(ring.ringSize() == 5 * 50, "ring holds 50 virtual nodes per real node");

    // Every key maps to a valid node, and always the same node (deterministic)
    bool valid = true, stable = true;
    for (int k = 0; k < 1000; ++k) {
        string key = "ACC_" + to_string(k);
        int a = ring.getClientRoute(key), b = ring.getClientRoute(key);
        if (a < 0 || a > 4) valid = false;
        if (a != b) stable = false;
    }
    check(valid, "every key maps to a node 0..4");
    check(stable, "same key -> same node every time");

    // Load balance: with virtual nodes each node should get roughly 20% of keys
    vector<int> count(5, 0);
    const int N = 10000;
    for (int k = 0; k < N; ++k) count[ring.getClientRoute("ACC_" + to_string(k))]++;
    bool balanced = true;
    string dist;
    for (int c : count) { if (c < N * 0.10 || c > N * 0.30) balanced = false; dist += to_string(c) + " "; }
    check(balanced, "10000 keys spread evenly (each node 10%-30%): " + dist);

    // Minimal migration: removing a node only moves the keys that node owned
    vector<int> before(N);
    for (int k = 0; k < N; ++k) before[k] = ring.getClientRoute("ACC_" + to_string(k));
    ring.removeNode(2);
    bool onlyNode2Moved = true;
    int moved = 0;
    for (int k = 0; k < N; ++k) {
        int after = ring.getClientRoute("ACC_" + to_string(k));
        if (after != before[k]) {
            moved++;
            if (before[k] != 2) onlyNode2Moved = false;
        }
    }
    check(onlyNode2Moved, "removing Node 2 moves ONLY Node 2's keys (" + to_string(moved) + " of " + to_string(N) + " moved)");
    check(moved == count[2], "number of moved keys equals Node 2's share");
    ring.addNode(2);

    // Wrap-around: a hash larger than every ring point must go to the first node
    bool wrapOk = true;
    {
        ConsistentHash small;
        small.addNode(7);
        for (int k = 0; k < 200; ++k) if (small.getClientRoute("key" + to_string(k)) != 7) wrapOk = false;
    }
    check(wrapOk, "wrap-around: every key finds a node even past the end of the ring");

    // Exclusion (used for CQRS reads): never returns an excluded node
    bool exclOk = true;
    for (int k = 0; k < 1000; ++k) {
        int r = ring.getRouteExcluding("ACC_" + to_string(k), {0, 3});
        if (r == 0 || r == 3 || r < 0) exclOk = false;
    }
    check(exclOk, "read routing skips excluded nodes (leader / crashed)");
    check(ring.getRouteExcluding("x", {0, 1, 2, 3, 4}) == -1, "returns -1 when every node is excluded");
}

// ---------------------------------------------------------------------------
// Unit tests: Bank ledger (state machine)
// ---------------------------------------------------------------------------
static void testLedger() {
    Logger::get().raw("\n== Bank Ledger (state machine) ==");
    BankLedger l;
    l.apply(deposit("A", "ACC_1", 100));
    check(fabs(l.getBalance("ACC_1") - 100) < 1e-9, "deposit adds money");
    l.apply(withdraw("A", "ACC_1", 30));
    check(fabs(l.getBalance("ACC_1") - 70) < 1e-9, "withdraw removes money");
    string r = l.apply(withdraw("A", "ACC_1", 500));
    check(r.find("REJECTED") != string::npos && fabs(l.getBalance("ACC_1") - 70) < 1e-9,
          "overdraft is rejected and balance unchanged");
    l.apply(transfer("A", "ACC_1", "ACC_2", 20));
    check(fabs(l.getBalance("ACC_1") - 50) < 1e-9 && fabs(l.getBalance("ACC_2") - 20) < 1e-9,
          "transfer moves money between accounts");
    Transaction noop;
    BankLedger before = l;
    l.apply(noop);
    check(l == before, "NOOP entry does not change the ledger");

    // Determinism: same sequence on two replicas -> identical state
    BankLedger r1, r2;
    vector<Transaction> seq = {deposit("A", "X", 10), transfer("A", "X", "Y", 50), deposit("B", "Y", 5),
                               withdraw("B", "Y", 3), transfer("B", "Y", "X", 2)};
    for (auto& t : seq) { r1.apply(t); r2.apply(t); }
    check(r1 == r2, "two replicas applying the same log end up identical");
}

// ---------------------------------------------------------------------------
// Integration tests: real 5-node cluster with threads
// ---------------------------------------------------------------------------
static void testCluster() {
    Logger::get().raw("\n== Raft cluster (5 nodes, real threads) ==");
    Logger::get().setSilent(true);   // hide the trace, show only PASS/FAIL
    Cluster c(5);
    c.start();

    RaftNode* leader = c.waitForLeader(5000);
    check(leader != nullptr, "a leader is elected within 5 s");
    if (!leader) { c.stop(); return; }
    check(c.allLeaders().size() == 1, "exactly one leader in a healthy cluster");

    // Log replication + majority commit
    bool allCommitted = true;
    allCommitted &= c.submit(leader, deposit("Alice", "ACC_101", 500), "test");
    allCommitted &= c.submit(leader, deposit("Bob", "ACC_202", 300), "test");
    allCommitted &= c.submit(leader, transfer("Alice", "ACC_101", "ACC_202", 100), "test");
    check(allCommitted, "3 transactions committed by majority");
    check(c.waitForConvergence(3000), "all 5 nodes applied all committed entries");
    check(c.checkConsistency(false), "log matching + identical ledgers on all nodes");

    // Leader crash -> a different node takes over
    int oldId = leader->getId();
    leader->crash();
    RaftNode* newLeader = c.waitForLeader(6000, oldId);
    check(newLeader != nullptr && newLeader->getId() != oldId, "after leader crash a DIFFERENT node is elected");
    if (newLeader) check(c.submit(newLeader, deposit("Carl", "ACC_303", 50), "test"),
                         "cluster with 4/5 nodes still commits");

    // Recovered node catches up
    c.nodes[oldId]->recover();
    check(c.waitForConvergence(5000), "recovered node catches up with the leader's log");
    check(c.checkConsistency(false), "consistency holds after recovery");

    // Two nodes crashed: 3/5 is still a majority
    RaftNode* l2 = c.waitForLeader(3000);
    if (l2) {
        int a = (l2->getId() + 1) % 5, b = (l2->getId() + 2) % 5;
        c.nodes[a]->crash();
        c.nodes[b]->crash();
        check(c.submit(l2, deposit("Dan", "ACC_404", 10), "test"), "3 of 5 nodes alive -> still commits");
        // Three crashed: 2/5 is NOT a majority
        int d = (l2->getId() + 3) % 5;
        c.nodes[d]->crash();
        check(!c.submit(l2, deposit("Dan", "ACC_404", 10), "test", 1500), "only 2 of 5 alive -> cannot commit");
        c.nodes[a]->recover(); c.nodes[b]->recover(); c.nodes[d]->recover();
        c.waitForLeader(6000);
        c.waitForConvergence(5000);
    }

    // Partition: minority leader cannot commit; majority elects a new leader
    RaftNode* l3 = c.waitForLeader(6000);
    if (l3) {
        int id = l3->getId();
        vector<int> minority = {id, (id + 1) % 5}, majority;
        for (int i = 0; i < 5; ++i) if (i != minority[0] && i != minority[1]) majority.push_back(i);
        NetworkSimulator::getInstance().setPartition({minority, majority});
        check(!c.submit(l3, deposit("Eve", "ACC_505", 9999), "test", 1500), "minority-side leader cannot commit");
        RaftNode* l4 = c.waitForLeader(6000, id);
        check(l4 != nullptr, "majority side elects its own leader");
        if (l4) {
            check(l4->getTerm() > l3->getTerm(), "new leader has a higher term");
            check(c.submit(l4, deposit("Frank", "ACC_505", 40), "test"), "majority side commits");
        }
        NetworkSimulator::getInstance().healPartition();
        check(c.waitForConvergence(5000), "after healing every node converges");
        check(c.allLeaders().size() == 1, "after healing the old leader has stepped down");
        check(c.checkConsistency(false), "uncommitted minority entry discarded, ledgers identical");
        RaftNode* fin = c.findLeader();
        if (fin) check(fin->getStatus().ledger.getBalance("ACC_505") < 100, "Eve's uncommitted $9999 never applied");
    }

    // Packet loss + delay
    NetworkSimulator::getInstance().setPacketLoss(0.2);
    NetworkSimulator::getInstance().setDelay(10, 60);
    bool lossy = true;
    for (int i = 0; i < 3; ++i) lossy &= c.submit(c.waitForLeader(6000), deposit("G", "ACC_606", 1), "test", 4000);
    check(lossy, "commits still succeed with 20% packet loss and delay");
    NetworkSimulator::getInstance().resetFaults();
    c.waitForConvergence(5000);
    check(c.checkConsistency(false), "consistency holds after unreliable network");

    check(Metrics::get().electionSafetyHolds(), "Election Safety: never two leaders in the same term");
    c.stop();
}

int main() {
    Logger::get().raw("Distributed Consensus Simulator - automated tests");
    testHashRing();
    testLedger();
    testCluster();
    Logger::get().raw("\n==============================");
    Logger::get().raw("  " + to_string(passed) + " passed, " + to_string(failed) + " failed");
    Logger::get().raw("==============================");
    return failed == 0 ? 0 : 1;
}
