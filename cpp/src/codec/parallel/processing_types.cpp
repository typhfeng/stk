#include "codec/parallel/processing_types.hpp"
#include "codec/parallel/processing_config.hpp"

namespace L2 {
namespace Parallel {

// Global configuration instance
ProcessingConfig g_config;

// Global state definitions
std::atomic<int> active_temp_folders{0};
std::atomic<bool> producers_done{false};
AssetQueue asset_queue{1000}; // bounded to 1000 items
std::unordered_map<std::string, FolderMeta> folder_meta;
std::mutex folder_meta_mutex;
std::queue<std::string> archive_queue;
std::mutex archive_queue_mutex;

// AssetQueue implementation
bool AssetQueue::try_push(const AssetWorkItem& item) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (queue_.size() >= max_size_) {
        return false; // queue is full
    }
    queue_.push_back(item);
    return true;
}

bool AssetQueue::try_pop(AssetWorkItem& item) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (queue_.empty()) {
        return false;
    }
    item = queue_.front();
    queue_.pop_front();
    return true;
}

bool AssetQueue::is_empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.empty();
}

bool AssetQueue::is_full() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size() >= max_size_;
}

} // namespace Parallel
} // namespace L2