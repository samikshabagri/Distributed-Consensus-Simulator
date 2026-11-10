#include "WebServer.h"
#include "Cluster.h"
#include "LoadBalancer.h"
#include <iostream>

using namespace std;

int main() {
    cout << "=========================================================" << endl;
    cout << "  Distributed Consensus Simulator (Raft + Hash + LB)" << endl;
    cout << "=========================================================" << endl;

    Cluster cluster(5);
    cluster.start();

    Logger::get().log("Waiting for initial Raft leader election...");
    cluster.waitForLeader(5000);

    LoadBalancer lb(cluster);

    // Seed initial bank accounts via the Leader
    int leaderId = -1;
    lb.handleWrite({TxnType::DEPOSIT, "Alice",   "ACC_101", "", 500},  leaderId);
    lb.handleWrite({TxnType::DEPOSIT, "Bob",     "ACC_202", "", 300},  leaderId);
    lb.handleWrite({TxnType::DEPOSIT, "Charlie", "ACC_303", "", 1200}, leaderId);
    lb.handleWrite({TxnType::DEPOSIT, "David",   "ACC_404", "", 800},  leaderId);
    lb.handleWrite({TxnType::DEPOSIT, "Eve",     "ACC_505", "", 450},  leaderId);
    lb.handleWrite({TxnType::DEPOSIT, "Frank",   "ACC_606", "", 650},  leaderId);
    lb.handleWrite({TxnType::DEPOSIT, "Grace",   "ACC_707", "", 900},  leaderId);
    lb.handleWrite({TxnType::DEPOSIT, "Hannah",  "ACC_808", "", 720},  leaderId);

    lb.startContinuousTraffic();

    WebServer server(cluster, lb, 8080);
    server.start();

    lb.stop();
    cluster.stop();
    return 0;
}
