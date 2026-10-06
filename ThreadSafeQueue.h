#pragma once
#include <queue>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <chrono>

template <typename T>
class ThreadSafeQueue {
private:
    std::queue<T> queue;
    std::mutex mtx;
    std::condition_variable cv;

public:
    // Pushes an item into the queue safely
    void push(T item) {
        std::unique_lock<std::mutex> lock(mtx);
        queue.push(std::move(item));
        lock.unlock();
        cv.notify_one();
    }

    // Tries to pop an item, returns false if empty
    bool try_pop(T& popped_value) {
        std::lock_guard<std::mutex> lock(mtx);
        if (queue.empty()) {
            return false;
        }
        popped_value = std::move(queue.front());
        queue.pop();
        return true;
    }

    // Waits until an item is available or the timeout occurs
    template<typename Rep, typename Period>
    bool wait_and_pop(T& popped_value, const std::chrono::duration<Rep, Period>& timeout) {
        std::unique_lock<std::mutex> lock(mtx);
        if (!cv.wait_for(lock, timeout, [this] { return !queue.empty(); })) {
            return false; // Timeout occurred
        }
        popped_value = std::move(queue.front());
        queue.pop();
        return true;
    }

    bool empty() {
        std::lock_guard<std::mutex> lock(mtx);
        return queue.empty();
    }
};
