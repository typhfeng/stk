#pragma once

#include <string>
#include <queue>
#include <mutex>
#include <atomic>
#include <unordered_map>
#include <deque>

namespace L2 {
namespace Parallel {

// Work item for asset queue (per-asset granularity)
struct AssetWorkItem {
    std::string folder_id;
    std::string asset_dir;
    std::string asset_code;
    std::string date_str;
    std::string temp_root;
};

// Folder metadata with atomic refcounts
struct FolderMeta {
    std::atomic<int> remaining;    // decremented once per completed asset
    int total;                     // fixed at enqueue-time (for progress)
    std::atomic<int> processed;    // increments once per completed asset
    std::string temp_root;         // folder to delete on completion
    std::string date_str;          // e.g., 20170104 (for progress line)
    
    FolderMeta(int total_assets, std::string temp_root_val, std::string date_str_val)
        : remaining(total_assets), total(total_assets), processed(0),
          temp_root(std::move(temp_root_val)), date_str(std::move(date_str_val)) {}
    
    // Delete copy constructor and assignment - atomics are not copyable
    FolderMeta(const FolderMeta&) = delete;
    FolderMeta& operator=(const FolderMeta&) = delete;
    
    // Allow move constructor and assignment
    FolderMeta(FolderMeta&& other) noexcept
        : remaining(other.remaining.load()), total(other.total), processed(other.processed.load()),
          temp_root(std::move(other.temp_root)), date_str(std::move(other.date_str)) {}
    
    FolderMeta& operator=(FolderMeta&& other) noexcept {
        if (this != &other) {
            remaining.store(other.remaining.load());
            total = other.total;
            processed.store(other.processed.load());
            temp_root = std::move(other.temp_root);
            date_str = std::move(other.date_str);
        }
        return *this;
    }
};

// Bounded MPMC asset queue (simple mutex + deque implementation)
class AssetQueue {
private:
    mutable std::mutex mutex_;
    std::deque<AssetWorkItem> queue_;
    const size_t max_size_;

public:
    explicit AssetQueue(size_t max_size = 1000) : max_size_(max_size) {}
    
    bool try_push(const AssetWorkItem& item);
    bool try_pop(AssetWorkItem& item);
    bool is_empty() const;
    bool is_full() const;
};

// Global state - all atomic or simple containers
extern std::atomic<int> active_temp_folders;
extern std::atomic<bool> producers_done;
extern AssetQueue asset_queue;
extern std::unordered_map<std::string, FolderMeta> folder_meta;
extern std::mutex folder_meta_mutex;
extern std::queue<std::string> archive_queue;
extern std::mutex archive_queue_mutex;

} // namespace Parallel
} // namespace L2