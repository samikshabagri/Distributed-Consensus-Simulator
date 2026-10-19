#include "RaftNode.h"
#include <vector>
#include <memory>
#include <iostream>
#include <thread>
#include <chrono>

using namespace std;

struct BankingTxn {
    string user;
    string account;
    string operation;
    double amount;
};

int main() {
    cout << "===============================================================" << endl;
    cout << "       Distributed Consensus Banking Simulator (Raft + Hash)    " << endl;
    cout << "===============================================================" << endl << endl;

    int numNodes = 5;
    vector<unique_ptr<RaftNode>> nodes;

    // Initialize 5-node cluster
    for (int i = 0; i < numNodes; ++i) {
        nodes.push_back(make_unique<RaftNode>(i));
    }

    // Start each node on its own thread
    for (auto& node : nodes) {
        node->start();
    }

    cout << "Starting cluster of 5 nodes. Waiting for initial Leader Election..." << endl << endl;

    // Wait for the first election to complete
    this_thread::sleep_for(chrono::seconds(3));

    // Pre-defined sample banking activities
    vector<BankingTxn> sampleTxns = {
        {"Alice",   "ACC_101", "DEPOSIT",    500.00},
        {"Bob",     "ACC_202", "WITHDRAWAL", 150.00},
        {"Charlie", "ACC_303", "TRANSFER",   300.00},
        {"David",   "ACC_404", "DEPOSIT",   1200.00},
        {"Emma",    "ACC_505", "PAYMENT",     85.50},
        {"Frank",   "ACC_101", "DEPOSIT",    250.00},
        {"Grace",   "ACC_202", "TRANSFER",   400.00},
        {"Hannah",  "ACC_303", "WITHDRAWAL",  75.00}
    };

    int txnCounter = 0;

    // Continuous loop running indefinitely
    while (true) {
        // Find the currently active cluster leader
        RaftNode* currentLeader = nullptr;
        for (auto& node : nodes) {
            if (node->isLeader()) {
                currentLeader = node.get();
                break;
            }
        }

        // If an election is currently underway, wait briefly and retry
        if (!currentLeader) {
            this_thread::sleep_for(chrono::milliseconds(500));
            continue;
        }

        // Pick next banking transaction
        const auto& txn = sampleTxns[txnCounter % sampleTxns.size()];
        txnCounter++;

        cout << "\n----------------------------------------------------------------------" << endl;
        cout << "[Banking Client] User " << txn.user << " requested " << txn.operation 
             << " of $" << txn.amount << " on Account " << txn.account << endl;

        // 1. Consistent Hashing determines partition on the hash ring
        int mappedNode = NetworkSimulator::getInstance().getRouteForClient(txn.account);
        cout << "[Consistent Hash] Account " << txn.account << " mapped to Node " 
             << mappedNode << " on Hash Ring." << endl;

        // 2. Client request routed to current Cluster Leader
        cout << "[Client Router] Routed transaction to Cluster Leader [Node " 
             << currentLeader->getId() << "] (Term " << currentLeader->getTerm() << ")." << endl;

        // 3. Leader commits transaction through Raft Log Replication
        string command = txn.user + " " + txn.operation + " $" + to_string((int)txn.amount) + " (" + txn.account + ")";
        currentLeader->proposeTransaction(command);

        cout << "[Bank State] Account " << txn.account << " balance updated. Total Committed Txns: " << txnCounter << endl;
        cout << "----------------------------------------------------------------------" << endl;

        // Every 5 transactions, simulate a leader stepping down to demonstrate dynamic re-election
        if (txnCounter % 5 == 0) {
            currentLeader->simulateStepDown();
            this_thread::sleep_for(chrono::seconds(3)); // Allow followers to elect a new leader
        } else {
            // Wait 2.5 seconds before submitting the next banking transaction
            this_thread::sleep_for(chrono::milliseconds(2500));
        }
    }

    return 0;
}
