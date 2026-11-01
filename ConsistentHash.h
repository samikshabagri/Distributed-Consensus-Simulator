#ifndef CONSISTENT_HASH_H
#define CONSISTENT_HASH_H

#include <map>
#include <set>
#include <string>
#include <cstdint>

using namespace std;

// ---------------------------------------------------------------------------
// Consistent Hashing (Phase III)
//
// Changes from Phase II:
//  1. Virtual nodes: each real node is placed on the ring VNODES times
//     ("Node_3#0", "Node_3#1", ...). With only 5 points the ring is very
//     uneven; with 5 x 50 points each node gets roughly 1/5 of the keys.
//  2. FNV-1a hash instead of std::hash: std::hash gives different values on
//     different compilers (MinGW vs GCC on Linux), FNV-1a is the same
//     everywhere, so routing results are reproducible and testable.
//  3. Routing with an exclusion set: used for CQRS reads, where we want a
//     FOLLOWER (not the leader, not a crashed node) to serve the read.
// ---------------------------------------------------------------------------
class ConsistentHash {
private:
    map<uint64_t, int> ring;   // sorted by hash -> behaves like a circle
    static constexpr int VNODES = 50;

public:
    // hashKey = 64-bit FNV-1a: start with an offset, then for each byte XOR and multiply by a prime.
    // Keys like "ACC_101" / "ACC_202" differ only near the end, which FNV alone
    // barely spreads into the high bits, so we finish with a "mixing" step
    // (the fmix64 finaliser from MurmurHash3) that scrambles all 64 bits.
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

    // Finds the first node clockwise from the key's position (wraps around)
    int getClientRoute(const string& clientKey) const {
        return getRouteExcluding(clientKey, {});
    }

    // Same walk, but skips nodes in 'excluded'. Returns -1 if none is usable.
    int getRouteExcluding(const string& key, const set<int>& excluded) const {
        if (ring.empty()) return -1;
        auto it = ring.lower_bound(hashKey(key));
        for (size_t steps = 0; steps < ring.size(); ++steps) {
            if (it == ring.end()) it = ring.begin();      // wrap around the ring
            if (!excluded.count(it->second)) return it->second;
            ++it;
        }
        return -1;
    }

    size_t ringSize() const { return ring.size(); }
};

#endif // CONSISTENT_HASH_H
