#ifndef THREADSAFEQUEUE_H
#define THREADSAFEQUEUE_H

#include <queue>
#include <mutex>
#include <condition_variable>
#include <chrono>

using namespace std;

template <typename T>
class ThreadSafeQueue {
private:
    queue<T> q;
    mutex mtx;
    condition_variable cv;

public:
    void push(T item) {
        {
            lock_guard<mutex> lock(mtx);
            q.push(std::move(item));
        }
        cv.notify_one();
    }

    bool wait_and_pop(T& out, int timeoutMs) {
        unique_lock<mutex> lock(mtx);
        if (!cv.wait_for(lock, chrono::milliseconds(timeoutMs), [this] { return !q.empty(); }))
            return false;
        out = std::move(q.front());
        q.pop();
        return true;
    }

    void clear() {
        lock_guard<mutex> lock(mtx);
        queue<T> empty;
        swap(q, empty);
    }
};

#endif
