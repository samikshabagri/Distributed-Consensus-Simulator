# Distributed Consensus Simulator (Raft Protocol & Consistent Hashing)

This is a C++ simulation of the **Raft Distributed Consensus Algorithm** and **Consistent Hashing**, showcasing core **Data Structures & Algorithms (DSA)** and **Object-Oriented Programming (OOP)**.

It simulates a cluster of 5 distributed server nodes that elect a leader, route client banking transactions via Consistent Hashing, replicate logs to followers, and achieve consensus continuously.

## Key Architecture

* **In-Memory Networking:** The network is simulated using custom thread-safe queues (`ThreadSafeQueue`), allowing nodes to pass messages safely without socket programming.
* **Randomized Timers:** Uses randomized election timeouts (1200ms–2500ms) to automatically prevent split votes during leader election.
* **Consistent Hashing (`ConsistentHash.h`):** Maps client bank accounts across a virtual hash ring (`std::map`), guaranteeing minimal partition migration.
* **Log Replication & Banking Engine:** Simulates client deposit, withdrawal, and transfer commands replicated across followers by the leader until majority quorum is reached.
* **Fault Tolerance & Re-Election:** Demonstrates dynamic leader failovers where the cluster automatically recovers, elects a new leader, and continues transaction processing without downtime.

---

## Expected Output

When running `raft_sim.exe`, the simulation runs continuously:
1. **Cluster Bootup & Election:** 5 nodes initialize as Followers. Timeouts fire, a node becomes Candidate, collects votes, and wins election as Leader.
2. **Banking Transactions Stream:** Client deposits and transfers stream into the system. Consistent Hashing maps accounts to the hash ring.
3. **Log Replication:** The Leader broadcasts log entries to Followers, each follower appends the transaction to its local log, and majority quorum commits it.
4. **Dynamic Failover:** Periodically simulates leader stepping down, triggering an instant new election while maintaining data integrity.
