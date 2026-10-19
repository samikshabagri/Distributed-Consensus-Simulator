#ifndef MESSAGE_H
#define MESSAGE_H

#include <string>
#include <vector>

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
    string command;
};

class AppendEntriesArgs : public Message {
public:
    MessageType getType() const override { return MessageType::AppendEntriesArgs; }
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    int leaderCommit;
    vector<LogEntry> entries;
};

class AppendEntriesReply : public Message {
public:
    MessageType getType() const override { return MessageType::AppendEntriesReply; }
    bool success;
};

#endif // MESSAGE_H
