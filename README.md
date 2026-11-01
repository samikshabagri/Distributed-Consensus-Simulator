# Distributed Consensus Simulator (Raft Protocol & Consistent Hashing)

A C++17 simulation of the **Raft distributed consensus algorithm** running a replicated
**banking ledger**, with **consistent hashing** for read routing and a **fault-injection
network** (crashes, partitions, packet loss, delay). It uses only the C++ standard library.
There are no sockets: every node is a `std::thread`, and messages travel through in-memory thread-safe queues.

Team **L1 Cache** (DSCPP-III-2026-T255), Graphic Era (Deemed to be University).

---

## Build & Run

```bash
# Linux / macOS / Windows (MinGW-w64 g++)
g++ -std=c++17 -O2 -pthread main.cpp  RaftNode.cpp -o raft_sim
g++ -std=c++17 -O2 -pthread tests.cpp RaftNode.cpp -o raft_tests

./raft_sim            # run all 5 demo scenarios
./raft_sim 3          # run scenario 1 + scenario 3 only
./raft_sim --quiet    # hide follower-level detail
./raft_sim --seed 7   # change the fault-injection random seed
./raft_tests          # 37 automated tests, exit code 0 = all passed
```

On Windows the commands produce `raft_sim.exe` and `raft_tests.exe`. You can also run `make`.

Each run writes **`results.csv`**, with one row per client transaction (leader, log index,
committed yes/no, latency in ms). You can use it to make graphs for the report.

---

## Demo scenarios (`raft_sim`)

| # | Scenario | What it shows |
|---|----------|---------------|
| 1 | Normal operation | Leader election, log replication, commit only after a **majority (3/5)** stores the entry, ledger applied on every node, overdraft **rejected identically** on every replica |
| 2 | Leader crash & recovery | Leader really stops, a **different** node wins the election (failover time is measured), the crashed node restarts and **catches up** through log replication |
| 3 | Network partition (split-brain) | {old leader + 1} cut off from {3 others}. The old leader **cannot commit**. The majority elects a new leader with a higher term and commits. After healing, the old leader **steps down** and its uncommitted entry is erased |
| 4 | Unreliable network | 20% packet loss plus 20–120 ms delay. Lost messages are re-sent with the next heartbeat, so commits still succeed (with higher latency) |
| 5 | CQRS reads | Writes go to the leader. Reads go to a **follower** chosen by the hash ring. A read just after a write can be **stale** (shown on screen) until the next heartbeat |

At the end the program prints a cluster status table, checks that all committed logs and
ledgers are identical (State Machine Safety), and prints performance metrics.

---

## Architecture

| File | Role |
|------|------|
| `ThreadSafeQueue.h` | Mailbox per node: `std::queue` + `mutex` + `condition_variable` |
| `Message.h` | Polymorphic messages: `RequestVote`, `AppendEntries` and their replies |
| `NetworkSimulator.h` | Singleton router plus fault injection. Delayed messages wait in a **min-heap** (`priority_queue`) served by a network thread |
| `ConsistentHash.h` | Hash ring on `std::map` with 50 **virtual nodes** per server and a portable FNV-1a + mix hash. Can skip nodes (leader/crashed) for reads |
| `BankLedger.h` | `Transaction` and the replicated state machine (`map<account, balance>`) |
| `RaftNode.h/.cpp` | Raft: election, log replication, commit, apply, crash/recover |
| `Metrics.h` | Counters, commit latency, failover time, election-safety check, CSV export |
| `Logger.h` | Thread-safe console output with millisecond timestamps (trace) |
| `Cluster.h` | Client/test harness: find leader, submit and wait for commit, status table, consistency check |
| `main.cpp` | Demo scenarios |
| `tests.cpp` | Automated unit and integration tests |

### Raft rules implemented
* **Leader election** with randomized timeouts (1000–2200 ms) and heartbeats every 250 ms.
* **Election restriction**: a node votes only for a candidate whose log is at least as up to date
  (`lastLogTerm`, `lastLogIndex`).
* **One vote per term**: `votedFor` is reset only when the term increases.
* **Log matching**: followers check `prevLogIndex`/`prevLogTerm`, delete conflicting entries, and
  the leader backs up `nextIndex` on failure.
* **Majority commit**: the leader counts `matchIndex` and commits index N only when a majority has
  it **and** `log[N].term == currentTerm`. A **no-op** entry is appended on election (Raft §8).
* **State machine**: committed entries are applied to the ledger in order. Every node ends with
  the same balances.
* **Thread safety**: one mutex per node protects all Raft state (ThreadSanitizer clean).

---

## Changes from Phase II

| Phase II | Phase III |
|----------|-----------|
| "Majority 5/5 confirmed" was printed after a 300 ms sleep | Followers send `AppendEntriesReply`. The leader counts a real majority with `matchIndex` / `commitIndex` |
| Followers appended entries without any check | `prevLogIndex`/`prevLogTerm` check and conflict truncation |
| `lastLogIndex/lastLogTerm` never sent or checked | Election restriction implemented |
| `becomeFollower()` reset `votedFor` in the same term (double voting possible) | Reset only when the term increases |
| Step-down slept the **main** thread, so the old leader kept running and always won again | `crash()` really stops the node. A different node is elected. `recover()` rebuilds its state from the log |
| "Balance updated" was only a print | Real `BankLedger`, including overdraft rejection |
| Hash-ring result was printed but unused | Used for CQRS read routing to followers |
| 2 data races (ThreadSanitizer) | 0 races: per-node mutex |
| `std::hash` (differs between compilers), 1 point per node | Portable hash, 50 virtual nodes per server, balanced load |
| Election timeout reset on any message | Reset only on a valid leader message or a granted vote |
| No tests | 37 automated tests |

---

## Limitations (honest scope)
* Thread scheduling is nondeterministic, so two runs never print exactly the same trace. The
  `--seed` option fixes only the random choices made by the network simulator.
* "Persistent" state (term, vote, log) is kept in memory. A real server would write it to disk.
* Follower reads can be stale (eventual consistency). Strongly consistent reads would need the
  ReadIndex / lease mechanism from the Raft thesis.
* Cluster membership is fixed at 5 nodes (no joint consensus). There is no log compaction or snapshots.

## References
1. Ongaro & Ousterhout (2014). *In Search of an Understandable Consensus Algorithm.* USENIX ATC.
2. Karger et al. (1997). *Consistent Hashing and Random Trees.* ACM STOC.
3. Cormen et al. (2009). *Introduction to Algorithms*, 3rd ed.
4. Stroustrup (2013). *The C++ Programming Language*, 4th ed.
