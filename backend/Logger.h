#ifndef LOGGER_H
#define LOGGER_H

#include <iostream>
#include <deque>
#include <vector>
#include <string>
#include <mutex>

using namespace std;

class Logger {
private:
    mutex mtx;
    deque<string> recent;
    static constexpr size_t MAX_LOGS = 40;

    Logger() = default;

public:
    static Logger& get() {
        static Logger instance;
        return instance;
    }

    void log(const string& text) {
        lock_guard<mutex> lock(mtx);
        cout << text << endl;
        recent.push_back(text);
        if (recent.size() > MAX_LOGS) recent.pop_front();
    }

    vector<string> getRecent() {
        lock_guard<mutex> lock(mtx);
        return vector<string>(recent.begin(), recent.end());
    }
};

#endif
