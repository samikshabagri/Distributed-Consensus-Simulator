#ifndef MESSAGE_H
#define MESSAGE_H

#include <string>
#include <vector>
#include "BankLedger.h"

using namespace std;

enum class MessageType {
    RequestVoteArgs,
    RequestVoteReply,
    AppendEntriesArgs,
    AppendEntriesReply
};

class Message {
public:
    virtual ~Message() = default;
    virtual MessageType getType() const = 0;

    // Phase III: every field now has a default value (no uninitialised data)
    int senderId = -1;
    int receiverId = -1;
    int term = 0;
};

class RequestVoteArgs : public Message {
public:
    MessageType getType() const override { return MessageType::RequestVoteArgs; }
    int candidateId = -1;
    // Phase III: these are now filled in and checked (election restriction).
    // A node only votes for a candidate whose log is at least as up-to-date.
    int lastLogIndex = 0;
    int lastLogTerm = 0;
};

class RequestVoteReply : public Message {
public:
    MessageType getType() const override { return MessageType::RequestVoteReply; }
    bool voteGranted = false;
};

// One slot of the replicated log
struct LogEntry {
    int term = 0;
    Transaction txn;
};

class AppendEntriesArgs : public Message {
public:
    MessageType getType() const override { return MessageType::AppendEntriesArgs; }
    int leaderId = -1;
    int prevLogIndex = 0;     // index of the entry just before the new ones
    int prevLogTerm = 0;      // term of that entry (log matching check)
    int leaderCommit = 0;     // leader's commit index
    vector<LogEntry> entries; // empty = plain heartbeat
};

// Phase III: followers now actually reply, so the leader can count a majority
class AppendEntriesReply : public Message {
public:
    MessageType getType() const override { return MessageType::AppendEntriesReply; }
    bool success = false;
    int matchIndex = 0;     // on success: last index the follower now has in common with the leader
    int lastLogIndex = 0;   // on failure: follower's last index (helps leader jump back faster)
};

#endif // MESSAGE_H
