# Distributed Consensus Simulator (Raft Protocol)

This is a C++ simulation of the **Raft Distributed Consensus Algorithm**, showcasing core **Data Structures & Algorithms (DSA)** and **Object-Oriented Programming (OOP)**.

It simulates a cluster of distributed server nodes that elect a leader and achieve consensus, handling randomized timeouts and message passing natively in-memory.

## Key Architecture

* **In-Memory Networking:** The network is simulated using custom thread-safe queues (`ThreadSafeQueue`), allowing nodes to pass messages safely without complex socket programming.
* **Randomized Timers:** Uses randomized election timeouts to automatically prevent split votes during leader election.
* **Object-Oriented Design:** 
  * Strict encapsulation hides the internal state of each server (`RaftNode`).
  * Network messages use polymorphism (an abstract `Message` base class) for clean and scalable communication.
  * A Singleton `NetworkSimulator` routes messages globally.

---

## Expected Output

When running the simulation, you will observe:
1. **5 Nodes** booting up in the `Follower` state.
2. A randomized timeout will occur.
3. One node will transition to `Candidate` and cast a `RequestVoteArgs` message to the cluster.
4. The remaining followers will grant their votes.
5. The Candidate will achieve a majority and announce itself as `Leader`.
6. The Leader will immediately begin broadcasting `AppendEntriesArgs` (Heartbeats) to suppress further elections.
