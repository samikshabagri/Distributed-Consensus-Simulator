# Distributed Consensus Simulator

A simple C++ simulation of a distributed banking system using **Raft consensus**, **Consistent Hashing**, and an **auto-scaling Load Balancer**, paired with a live web dashboard.

## What It Does

- **Raft Consensus:** Elects a Leader, replicates transactions to Followers, and automatically elects a new Leader if a server crashes.
- **Read / Write Separation:** All **writes** go to the **Leader**; all **reads** go to **Followers** selected using **Consistent Hashing**.
- **Auto-Scaling Load Balancer:** Handles bursts of thousands of messages and automatically spins up new Follower servers when load gets too high.
- **Web Dashboard:** Displays live server states, the Consistent Hash ring, message load over time, account balances, and interactive controls.

## Structure

- `backend/` — Core C++ cluster, Raft, Consistent Hashing, Load Balancer, and HTTP server.
- `frontend/` — Graphical web dashboard (`index.html`).

## Build & Run

```bash
g++ -std=c++17 -O2 -pthread -static backend/main.cpp backend/RaftNode.cpp -lws2_32 -o backend/raft_sim.exe
./backend/raft_sim.exe
```

Open **http://localhost:8080** in your browser.
