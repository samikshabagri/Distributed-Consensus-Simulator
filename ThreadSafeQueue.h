#ifndef THREADSAFEQUEUE_H
#define THREADSAFEQUEUE_H

#include <queue>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <chrono>

using namespace std;

template <typename T>
class ThreadSafeQueue {
private:
    queue<T> q;
    mutex mtx;
    condition_variable cv;

public:
    // Pushes an item into the queue safely
    void push(T item) {
        unique_lock<mutex> lock(mtx);
        q.push(move(item));
        lock.unlock();
        cv.notify_one();
    }

    // Tries to pop an item, returns false if empty
    bool try_pop(T& popped_value) {
        lock_guard<mutex> lock(mtx);
        if (q.empty()) {
            return false;
        }
        popped_value = move(q.front());
        q.pop();
        return true;
    }

    // Waits until an item is available or the timeout occurs
    bool wait_and_pop(T& popped_value, int timeoutMillis) {
        unique_lock<mutex> lock(mtx);
        if (!cv.wait_for(lock, chrono::milliseconds(timeoutMillis), [this] { return !q.empty(); })) {
            return false; // Timeout occurred
        }
        popped_value = move(q.front());
        q.pop();
        return true;
    }

    bool empty() {
        lock_guard<mutex> lock(mtx);
        return q.empty();
    }
};

#endif // THREADSAFEQUEUE_H
