#include "codec/parallel/processing_types.hpp"
#include "codec/parallel/processing_config.hpp"

namespace L2 {
namespace Parallel {

// Global configuration instance
ProcessingConfig g_config;

// Global state definitions following L2_database design
std::counting_semaphore<> temp_slots{g_config.max_temp_folders};
FolderQueue folder_queue{100}; // bounded to 100 folders
std::queue<std::string> archive_queue;
std::mutex archive_queue_mutex;

// Active folder shared state
std::unique_ptr<ActiveFolder> active_folder;
std::mutex active_folder_mutex;

// FolderQueue implementation with closeable semantics
bool FolderQueue::try_push(FolderWorkItem&& item) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_ || queue_.size() >= max_size_) {
        return false; // queue is closed or full
    }
    queue_.push_back(std::move(item));
    return true;
}

bool FolderQueue::try_pop(FolderWorkItem& item) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (queue_.empty()) {
        return false;
    }
    item = std::move(queue_.front());
    queue_.pop_front();
    return true;
}

std::optional<FolderWorkItem> FolderQueue::try_pop() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (queue_.empty()) {
        return std::nullopt;
    }
    FolderWorkItem item = std::move(queue_.front());
    queue_.pop_front();
    return item;
}

bool FolderQueue::is_empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.empty();
}

bool FolderQueue::is_full() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size() >= max_size_;
}

void FolderQueue::close() {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
}

bool FolderQueue::is_closed() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return closed_;
}

bool FolderQueue::is_closed_and_empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return closed_ && queue_.empty();
}

} // namespace Parallel
} // namespace L2