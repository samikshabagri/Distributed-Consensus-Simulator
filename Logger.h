#ifndef LOGGER_H
#define LOGGER_H

#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>
#include <mutex>
#include <chrono>
#include <atomic>

using namespace std;

// ---------------------------------------------------------------------------
// Logger (Phase III)
// One global mutex for the console (same idea as g_coutMutex in Phase II),
// plus a timestamp in milliseconds since the program started on every line.
// The timestamps turn the console output into a "trace" of the run, so we can
// see exactly when elections, commits and failures happened.
// ---------------------------------------------------------------------------
class Logger {
private:
    mutex mtx;
    chrono::steady_clock::time_point startTime;
    atomic<bool> verbose{true};    // when false, follower-level detail is hidden
    atomic<bool> silent{false};    // when true, only raw() lines are printed (used by tests)

    Logger() : startTime(chrono::steady_clock::now()) {}

public:
    static Logger& get() {
        static Logger instance;
        return instance;
    }

    long long elapsedMs() const {
        return chrono::duration_cast<chrono::milliseconds>(
                   chrono::steady_clock::now() - startTime).count();
    }

    void setVerbose(bool v) { verbose = v; }
    bool isVerbose() const { return verbose; }
    void setSilent(bool s) { silent = s; }

    // Normal line: "[  1234 ms] text"
    void log(const string& text) {
        if (silent) return;
        lock_guard<mutex> lock(mtx);
        cout << "[" << setw(6) << elapsedMs() << " ms] " << text << endl;
    }

    // Detail line: only printed in verbose mode (follower appends etc.)
    void detail(const string& text) {
        if (!verbose) return;
        log(text);
    }

    // Empty line (hidden in silent mode)
    void blank() {
        if (silent) return;
        raw("");
    }

    // Separator between client transactions
    void divider() {
        if (silent) return;
        raw("----------------------------------------------------------------------");
    }

    // Plain line with no timestamp (banners and tables)
    void raw(const string& text) {
        lock_guard<mutex> lock(mtx);
        cout << text << endl;
    }
};

#endif // LOGGER_H
