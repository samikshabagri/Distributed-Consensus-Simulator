#pragma once
#include <string>
#include <vector>

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
    
    int senderId;
    int receiverId;
    int term;
};

class RequestVoteArgs : public Message {
public:
    MessageType getType() const override { return MessageType::RequestVoteArgs; }
    int candidateId;
    int lastLogIndex;
    int lastLogTerm;
};

class RequestVoteReply : public Message {
public:
    MessageType getType() const override { return MessageType::RequestVoteReply; }
    bool voteGranted;
};

struct LogEntry {
    int term;
    std::string command;
};

class AppendEntriesArgs : public Message {
public:
    MessageType getType() const override { return MessageType::AppendEntriesArgs; }
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    int leaderCommit;
    std::vector<LogEntry> entries; // Added for log replication phase
};

class AppendEntriesReply : public Message {
public:
    MessageType getType() const override { return MessageType::AppendEntriesReply; }
    bool success;
};
