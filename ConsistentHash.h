#ifndef CONSISTENT_HASH_H
#define CONSISTENT_HASH_H

#include <map>
#include <string>
#include <functional>

// A simple implementation of Consistent Hashing
class ConsistentHash {
private:
    // std::map automatically sorts by the hash, naturally creating a "ring"
    std::map<size_t, int> ring;
    std::hash<std::string> hasher;

public:
    void addNode(int nodeId) {
        size_t h = hasher("Node_" + std::to_string(nodeId));
        ring[h] = nodeId;
    }

    void removeNode(int nodeId) {
        size_t h = hasher("Node_" + std::to_string(nodeId));
        ring.erase(h);
    }

    // Finds the next available node for a given client or data key
    int getClientRoute(const std::string& clientKey) {
        if (ring.empty()) return -1;
        
        size_t h = hasher(clientKey);
        // lower_bound finds the first node whose hash is >= the client's hash
        auto it = ring.lower_bound(h);
        
        // If we reached the end of the map, wrap around to the beginning of the ring
        if (it == ring.end()) {
            return ring.begin()->second;
        }
        
        return it->second;
    }
};

#endif // CONSISTENT_HASH_H

