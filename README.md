# Distributed Consensus Simulator (Raft Protocol)

This is a C++ simulation of the **Raft Distributed Consensus Algorithm**, designed heavily around core **Data Structures & Algorithms (DSA)** and **Object-Oriented Programming (OOP)** concepts.

It simulates a cluster of distributed server nodes that elect a leader and achieve consensus, handling randomized timeouts and message passing natively in-memory.

## 🚀 Key Academic Concepts (For Viva/Review)

This project avoids generic networking libraries in favor of building custom data structures and OOP hierarchies from scratch to demonstrate academic proficiency.

### 1. Data Structures & Algorithms (DSA)
* **Custom Thread-Safe Queue (`ThreadSafeQueue.h`):** Instead of using Windows Sockets, the network is simulated entirely in-memory. To prevent race conditions when multiple nodes (threads) send messages simultaneously, we built a custom concurrent queue using `std::queue`, protected by `std::mutex` and `std::condition_variable`.
* **Randomized Timers:** Implements Raft's randomized election timeout algorithm to naturally prevent split votes without needing complex centralized coordination.

### 2. Object-Oriented Programming (OOP)
* **Polymorphism & Inheritance (`Message.h`):** All network communication utilizes an Abstract Base Class called `Message`. Specific remote procedure calls like `RequestVoteArgs` and `AppendEntriesArgs` inherit from this. Nodes process all incoming data polymorphically via `std::shared_ptr<Message>`.
* **Encapsulation (`RaftNode.h`):** The internal state machine of a server (whether it is a Follower, Candidate, or Leader) and its voting logic are strictly encapsulated within the `RaftNode` class. The `main.cpp` program has no access to a node's internal state, ensuring strict data hiding.
* **Singleton Pattern (`NetworkSimulator.h`):** Used to represent the global network fabric that routes messages between the isolated node queues.

---

## 🛠️ How to Compile and Run

Because this project uses advanced C++11 multithreading, it requires a compiler with POSIX thread support (like MSYS2 MinGW-w64 or Visual Studio).

### For Windows:
1. Double-click the included `build.bat` file.
2. OR, open your terminal and run:
   ```bash
   .\build.bat
   ```

### Manual Compilation (Linux / Mac / POSIX MinGW):
```bash
g++ -std=c++14 Message.h ThreadSafeQueue.h NetworkSimulator.h RaftNode.cpp main.cpp -o raft_sim -pthread
./raft_sim
```

---

## 📊 Expected Output

When running the simulation, you will observe:
1. **5 Nodes** booting up in the `Follower` state.
2. A randomized timeout will occur.
3. One node will transition to `Candidate` and cast a `RequestVoteArgs` message to the cluster.
4. The remaining followers will grant their votes.
5. The Candidate will achieve a majority and announce itself as `Leader`.
6. The Leader will immediately begin broadcasting `AppendEntriesArgs` (Heartbeats) to suppress further elections.

---

## 👨‍💻 Authorship & Contributions
* **Mellow-drop** (75%): Core Raft Algorithm (Leader Election, State Machine transitions, Node encapsulation) and Network Simulation layer.
* **samikshabagri** (25%): Core DSA & OOP Foundations (Custom `ThreadSafeQueue` concurrent data structure and polymorphic `Message` class hierarchy).
