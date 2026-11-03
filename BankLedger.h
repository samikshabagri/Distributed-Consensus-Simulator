#ifndef BANKLEDGER_H
#define BANKLEDGER_H

#include <map>
#include <string>
#include <sstream>
#include <iomanip>

using namespace std;

// ---------------------------------------------------------------------------
// Transaction: one client command stored inside a Raft log entry.
// NOOP is the empty entry a new leader appends when it wins an election
// (Raft paper, Section 8) so that entries from older terms get committed.
// ---------------------------------------------------------------------------
enum class TxnType { NOOP, DEPOSIT, WITHDRAW, TRANSFER };

struct Transaction {
    TxnType type = TxnType::NOOP;
    string user;
    string account;      // source account
    string toAccount;    // only used by TRANSFER
    double amount = 0;

    string toString() const {
        ostringstream out;
        out << fixed << setprecision(2);
        switch (type) {
            case TxnType::NOOP:     out << "NOOP (new leader marker)"; break;
            case TxnType::DEPOSIT:  out << user << " DEPOSIT $" << amount << " -> " << account; break;
            case TxnType::WITHDRAW: out << user << " WITHDRAW $" << amount << " <- " << account; break;
            case TxnType::TRANSFER: out << user << " TRANSFER $" << amount << " " << account << " -> " << toAccount; break;
        }
        return out.str();
    }
};

// ---------------------------------------------------------------------------
// BankLedger: the replicated "state machine".
// Every node owns one ledger and applies COMMITTED log entries to it in log
// order. Because every node applies the same entries in the same order, and
// apply() is deterministic, every ledger ends up identical. That is the whole
// point of Raft (State Machine Safety).
// ---------------------------------------------------------------------------
class BankLedger {
private:
    map<string, double> balances;   // account -> balance (sorted, so printing is stable)

public:
    // Applies one transaction. Returns a short result message.
    // A withdrawal/transfer with insufficient funds is REJECTED - and since
    // this check is deterministic, every replica rejects it the same way.
    string apply(const Transaction& t) {
        ostringstream out;
        out << fixed << setprecision(2);
        switch (t.type) {
            case TxnType::NOOP:
                return "no-op";
            case TxnType::DEPOSIT:
                balances[t.account] += t.amount;
                out << t.account << " balance = $" << balances[t.account];
                return out.str();
            case TxnType::WITHDRAW:
                if (balances[t.account] < t.amount) {
                    out << "REJECTED (insufficient funds: " << t.account << " has $" << balances[t.account] << ")";
                    return out.str();
                }
                balances[t.account] -= t.amount;
                out << t.account << " balance = $" << balances[t.account];
                return out.str();
            case TxnType::TRANSFER:
                if (balances[t.account] < t.amount) {
                    out << "REJECTED (insufficient funds: " << t.account << " has $" << balances[t.account] << ")";
                    return out.str();
                }
                balances[t.account]   -= t.amount;
                balances[t.toAccount] += t.amount;
                out << t.account << " = $" << balances[t.account] << ", "
                    << t.toAccount << " = $" << balances[t.toAccount];
                return out.str();
        }
        return "unknown";
    }

    double getBalance(const string& account) const {
        auto it = balances.find(account);
        return (it == balances.end()) ? 0.0 : it->second;
    }

    const map<string, double>& getAll() const { return balances; }

    void reset() { balances.clear(); }

    bool operator==(const BankLedger& other) const { return balances == other.balances; }
};

#endif // BANKLEDGER_H
