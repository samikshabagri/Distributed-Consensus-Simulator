#ifndef BANKLEDGER_H
#define BANKLEDGER_H

#include <map>
#include <string>
#include <sstream>
#include <iomanip>

using namespace std;

enum class TxnType { NOOP, DEPOSIT, WITHDRAW, TRANSFER };

struct Transaction {
    TxnType type = TxnType::NOOP;
    string user;
    string account;
    string toAccount;
    double amount = 0;

    string toString() const {
        ostringstream out;
        out << fixed << setprecision(2);
        switch (type) {
            case TxnType::NOOP:     return "NOOP";
            case TxnType::DEPOSIT:  out << user << " DEPOSIT $" << amount << " -> " << account; break;
            case TxnType::WITHDRAW: out << user << " WITHDRAW $" << amount << " <- " << account; break;
            case TxnType::TRANSFER: out << user << " TRANSFER $" << amount << " " << account << " -> " << toAccount; break;
        }
        return out.str();
    }
};

class BankLedger {
private:
    map<string, double> balances;

public:
    string apply(const Transaction& t) {
        ostringstream out;
        out << fixed << setprecision(2);
        if (t.type == TxnType::NOOP) return "no-op";

        if (t.type == TxnType::DEPOSIT) {
            balances[t.account] += t.amount;
            out << t.account << " = $" << balances[t.account];
            return out.str();
        }
        if (t.type == TxnType::WITHDRAW) {
            if (balances[t.account] < t.amount) return "REJECTED (insufficient funds)";
            balances[t.account] -= t.amount;
            out << t.account << " = $" << balances[t.account];
            return out.str();
        }
        if (t.type == TxnType::TRANSFER) {
            if (balances[t.account] < t.amount) return "REJECTED (insufficient funds)";
            balances[t.account] -= t.amount;
            balances[t.toAccount] += t.amount;
            out << t.account << " -> " << t.toAccount << " ($" << t.amount << ")";
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
};

#endif
