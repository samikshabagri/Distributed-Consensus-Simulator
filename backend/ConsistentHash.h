#ifndef CONSISTENT_HASH_H
#define CONSISTENT_HASH_H

#include <map>
#include <set>
#include <string>
#include <cstdint>

using namespace std;

// Simple Consistent Hash Ring with virtual nodes
class ConsistentHash {
private:
    map<uint64_t, int> ring;
    static constexpr int VNODES = 50;

public:
    // 64-bit FNV-1a hash + bit mix so keys spread evenly across the ring
    static uint64_t hashKey(const string& s) {
        uint64_t h = 1469598103934665603ULL;
        for (unsigned char c : s) {
            h ^= c;
            h *= 1099511628211ULL;
        }
        h ^= h >> 33; h *= 0xff51afd7ed558ccdULL;
        h ^= h >> 33; h *= 0xc4ceb9fe1a85ec53ULL;
        h ^= h >> 33;
        return h;
    }

    void addNode(int nodeId) {
        for (int v = 0; v < VNODES; ++v)
            ring[hashKey("Node_" + to_string(nodeId) + "#" + to_string(v))] = nodeId;
    }

    void removeNode(int nodeId) {
        for (int v = 0; v < VNODES; ++v)
            ring.erase(hashKey("Node_" + to_string(nodeId) + "#" + to_string(v)));
    }

    // Walk clockwise around the ring and pick the first node NOT in 'excluded'
    int getRouteExcluding(const string& key, const set<int>& excluded) const {
        if (ring.empty()) return -1;
        auto it = ring.lower_bound(hashKey(key));
        for (size_t steps = 0; steps < ring.size(); ++steps) {
            if (it == ring.end()) it = ring.begin();
            if (!excluded.count(it->second)) return it->second;
            ++it;
        }
        return -1;
    }
};

#endif
