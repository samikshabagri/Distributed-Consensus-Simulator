#ifndef MESSAGE_H
#define MESSAGE_H

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

    int senderId = -1;
    int receiverId = -1;
    int term = 0;
};

class RequestVoteArgs : public Message {
public:
    MessageType getType() const override { return MessageType::RequestVoteArgs; }
    int candidateId = -1;
    int lastLogIndex = 0;
    int lastLogTerm = 0;
};

class RequestVoteReply : public Message {
public:
    MessageType getType() const override { return MessageType::RequestVoteReply; }
    bool voteGranted = false;
};

struct LogEntry {
    int term = 0;
    Transaction txn;
};

class AppendEntriesArgs : public Message {
public:
    MessageType getType() const override { return MessageType::AppendEntriesArgs; }
    int leaderId = -1;
    int prevLogIndex = 0;
    int prevLogTerm = 0;
    int leaderCommit = 0;
    vector<LogEntry> entries;
};

class AppendEntriesReply : public Message {
public:
    MessageType getType() const override { return MessageType::AppendEntriesReply; }
    bool success = false;
    int matchIndex = 0;
    int lastLogIndex = 0;
};

#endif
