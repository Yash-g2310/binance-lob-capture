#pragma once

#include "lob_capture/types.hpp"

#include <algorithm>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <utility>

namespace lob_capture {

// Bounded FIFO. push() blocks when the consumer is behind instead of silently
// discarding inbound market data. If this creates sustained backpressure, the
// WebSocket may disconnect; reconnect + book resynchronization handle that case.
class MessageQueue {
public:
    explicit MessageQueue(std::size_t capacity = 50'000) : capacity_(capacity) {
        if (capacity_ == 0) throw std::invalid_argument("queue capacity must be positive");
    }

    bool push(RawMessage message) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_full_.wait(lock, [this] { return closed_ || queue_.size() < capacity_; });
        if (closed_) return false;
        queue_.push(std::move(message));
        high_water_ = std::max(high_water_, queue_.size());
        not_empty_.notify_one();
        return true;
    }

    bool pop(RawMessage& message) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait(lock, [this] { return closed_ || !queue_.empty(); });
        if (queue_.empty()) return false;
        message = std::move(queue_.front());
        queue_.pop();
        not_full_.notify_one();
        return true;
    }

    void close() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed_ = true;
        }
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    std::size_t high_water_mark() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return high_water_;
    }

private:
    const std::size_t capacity_;
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    std::queue<RawMessage> queue_;
    bool closed_{false};
    std::size_t high_water_{0};
};

}  // namespace lob_capture
