#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <memory>
#include <thread>
#include <chrono>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <map>
#include <functional>
#include <atomic>
#include <random>

// --- Data Structures ---
struct Transaction {
    std::string transactionId;
    std::string accountNumber;
    std::string operationType;
    double amount;
    std::string timestamp;
};

// --- Thread-Safe Queue ---
template <typename T>
class ThreadSafeQueue {
private:
    std::queue<T> queue_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;

public:
    void push(T item) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.push(std::move(item));
        }
        cv_.notify_one();
    }

    T pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return !queue_.empty(); });
        T item = std::move(queue_.front());
        queue_.pop();
        return item;
    }

    bool empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }
};

// --- Consistent Hashing ---
class ConsistentHash {
private:
    std::map<size_t, int> ring_;
    std::hash<std::string> hasher_;

public:
    void addNode(int nodeId) {
        size_t hashValue = hasher_("Node_" + std::to_string(nodeId));
        ring_[hashValue] = nodeId;
    }

    int getRoute(const std::string& key) {
        if (ring_.empty()) return -1;
        size_t hashValue = hasher_(key);
        auto it = ring_.lower_bound(hashValue);
        if (it == ring_.end()) {
            return ring_.begin()->second;
        }
        return it->second;
    }
};

// --- Raft Node Stub ---
class RaftNode {
private:
    int nodeId_;
    ThreadSafeQueue<Transaction> inputQueue_;
    std::atomic<bool> running_{false};
    std::thread workerThread_;

    void processTransactions() {
        // While loop processing incoming transactions for this specific Raft node
        while (running_) {
            if (!inputQueue_.empty()) {
                Transaction txn = inputQueue_.pop();
                std::cout << "[Raft Node " << nodeId_ << "] Consensused Transaction " 
                          << txn.transactionId << " (" << txn.operationType 
                          << " $" << txn.amount << " for " << txn.accountNumber << ")\n";
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }
    }

public:
    explicit RaftNode(int id) : nodeId_(id) {}

    void start() {
        running_ = true;
        workerThread_ = std::thread(&RaftNode::processTransactions, this);
    }

    void stop() {
        running_ = false;
        if (workerThread_.joinable()) {
            workerThread_.join();
        }
    }

    void submitTransaction(const Transaction& txn) {
        inputQueue_.push(txn);
    }

    int getId() const { return nodeId_; }
};

// --- Helper Functions ---
void createDummyBankingFile(const std::string& filename) {
    std::ofstream outFile(filename);
    if (!outFile.is_open()) return;

    outFile << "TXN_1001 | ACC_98451 | DEPOSIT | 5000.00 | 2026-10-18T10:00:00Z\n";
    outFile << "TXN_1002 | ACC_12384 | WITHDRAWAL | 1200.50 | 2026-10-18T10:01:15Z\n";
    outFile << "TXN_1003 | ACC_55412 | TRANSFER | 2500.00 | 2026-10-18T10:02:30Z\n";
    outFile << "TXN_1004 | ACC_98451 | BALANCE_CHECK | 0.00 | 2026-10-18T10:03:00Z\n";
    outFile << "TXN_1005 | ACC_77321 | DEPOSIT | 10000.00 | 2026-10-18T10:04:45Z\n";
    outFile << "TXN_1006 | ACC_12384 | WITHDRAWAL | 500.25 | 2026-10-18T10:05:10Z\n";
    outFile << "TXN_1007 | ACC_55412 | DEPOSIT | 1500.00 | 2026-10-18T10:06:20Z\n";
    
    outFile.close();
}

std::string trim(const std::string& str) {
    size_t first = str.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return "";
    size_t last = str.find_last_not_of(" \t\r\n");
    return str.substr(first, (last - first + 1));
}

int main() {
    const std::string filename = "banking_data.txt";
    createDummyBankingFile(filename);

    constexpr int clusterSize = 3;
    std::map<int, std::unique_ptr<RaftNode>> cluster;
    ConsistentHash ring;

    for (int i = 0; i < clusterSize; ++i) {
        cluster[i] = std::make_unique<RaftNode>(i);
        cluster[i]->start();
        ring.addNode(i);
    }

    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Error: Could not open " << filename << std::endl;
        return 1;
    }

    std::cout << "===================================================\n";
    std::cout << " Streaming Banking Transactions into Consensus Ring \n";
    std::cout << "===================================================\n\n";

    std::string line;

    // --- Primary File Processing While Loop ---
    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '#') continue;

        std::stringstream ss(line);
        std::string token;
        std::vector<std::string> tokens;

        while (std::getline(ss, token, '|')) {
            tokens.push_back(trim(token));
        }

        if (tokens.size() >= 5) {
            Transaction txn;
            txn.transactionId = tokens[0];
            txn.accountNumber = tokens[1];
            txn.operationType = tokens[2];
            txn.amount = std::stod(tokens[3]);
            txn.timestamp = tokens[4];

            // Determine target Raft node via Consistent Hashing on Account Number
            int targetNodeId = ring.getRoute(txn.accountNumber);

            std::cout << "[Client Router] Account " << txn.accountNumber 
                      << " mapped to Node " << targetNodeId 
                      << " for Transaction " << txn.transactionId << "\n";

            cluster[targetNodeId]->submitTransaction(txn);
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    }

    file.close();

    // Allow workers time to finish processing queue items
    std::this_thread::sleep_for(std::chrono::seconds(1));

    std::cout << "\n===================================================\n";
    std::cout << " Shutting down cluster...                         \n";
    std::cout << "===================================================\n";

    for (auto& [id, node] : cluster) {
        node->stop();
    }

    return 0;
}
