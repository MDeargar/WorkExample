#pragma once

#include <mutex>
#include <condition_variable>
#include <deque>

#include <optional>

namespace tp {

template <typename T>
class UnboundedBlockingQueue {
public:
    bool Put(T value) {
        std::lock_guard lock(mutex_);
        if (is_closed_) {
            return false;
        }

        buffer_.push_back(std::move(value));
        buffer_empty_.notify_one();

        return true;
    }

    std::optional<T> Take() {
        std::unique_lock lock(mutex_);
        while (buffer_.empty() && !is_closed_) {
            buffer_empty_.wait(lock);
        }

        if (buffer_.empty()) {
            return std::nullopt;
        }

        T result(std::move(buffer_.front()));
        buffer_.pop_front();

        return {std::move(result)};
    }

    void Close() {
        CloseImpl(/*clear=*/false);
    }

    void Cancel() {
        CloseImpl(/*clear=*/true);
    }

private:
    void CloseImpl(bool clear) {
        std::lock_guard lock(mutex_);
        is_closed_ = true;

        if (clear) {
            buffer_.clear();
        }

        buffer_empty_.notify_all();
    }

private:
    bool is_closed_{false};
    std::deque<T> buffer_;
    std::mutex mutex_;
    std::condition_variable buffer_empty_;
};

}  // namespace tp
