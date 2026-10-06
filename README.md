# Distributed Consensus Simulator (Raft Protocol)

This is a C++ simulation of the **Raft Distributed Consensus Algorithm**, showcasing core **Data Structures & Algorithms (DSA)** and **Object-Oriented Programming (OOP)**.

It simulates a cluster of distributed server nodes that elect a leader and achieve consensus, handling randomized timeouts and message passing natively in-memory.

## 🚀 Key Architecture

### 1. Data Structures & Algorithms (DSA)
* **Concurrent Networking:** We simulate the network in-memory using a custom `ThreadSafeQueue` built with `std::queue`, `std::mutex`, and `std::condition_variable` to safely prevent race conditions.
* **Randomized Timers:** Implements Raft's randomized election timeout algorithm to naturally resolve split votes.

### 2. Object-Oriented Programming (OOP)
* **Polymorphism & Inheritance:** Network communication utilizes an Abstract Base Class (`Message`). Remote procedure calls like `RequestVoteArgs` inherit from this, allowing nodes to process incoming data polymorphically.
* **Encapsulation:** The internal state machine of a server (Follower/Candidate/Leader) and voting logic are strictly encapsulated within the `RaftNode` class.
* **Singleton Pattern:** Used in `NetworkSimulator` to represent the global network fabric routing messages between isolated node queues.

---

## 📊 Expected Output

When running the simulation, you will observe:
1. **5 Nodes** booting up in the `Follower` state.
2. A randomized timeout will occur.
3. One node will transition to `Candidate` and cast a `RequestVoteArgs` message to the cluster.
4. The remaining followers will grant their votes.
5. The Candidate will achieve a majority and announce itself as `Leader`.
6. The Leader will immediately begin broadcasting `AppendEntriesArgs` (Heartbeats) to suppress further elections.
