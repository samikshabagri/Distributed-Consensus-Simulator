#include "RaftNode.h"
#include <vector>
#include <memory>
#include <iostream>

int main() {
    std::cout << "===========================================\n";
    std::cout << " Starting Raft Distributed Consensus Engine  \n";
    std::cout << "===========================================\n";
    
    int numNodes = 5;
    std::vector<std::unique_ptr<RaftNode>> nodes;
    
    // Initialize cluster
    for (int i = 0; i < numNodes; ++i) {
        nodes.push_back(std::make_unique<RaftNode>(i));
    }
    
    // Start nodes
    for (auto& node : nodes) {
        node->start();
    }
    
    // Let the simulation run for a while to observe leader election
    std::this_thread::sleep_for(std::chrono::seconds(5));
    
    std::cout << "\nShutting down cluster...\n";
    for (auto& node : nodes) {
        node->stop();
    }
    
    std::cout << "Shutdown complete.\n";
    return 0;
}
