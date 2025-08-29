#pragma once

#include <string>
#include <vector>
#include <queue>
#include <mutex>
#include <atomic>
#include <deque>
#include <semaphore>
#include <filesystem>
#include <optional>
#include <thread>

namespace L2 {
namespace Parallel {

// Forward declaration
extern std::counting_semaphore<> temp_slots;

// RAII token for temp folder ownership - automatically cleans up on destruction
class FolderToken {
private:
    std::string temp_root_;
    bool valid_;

public:
    explicit FolderToken(std::string temp_root) 
        : temp_root_(std::move(temp_root)), valid_(true) {}
    
    // Move-only semantics
    FolderToken(const FolderToken&) = delete;
    FolderToken& operator=(const FolderToken&) = delete;
    
    FolderToken(FolderToken&& other) noexcept 
        : temp_root_(std::move(other.temp_root_)), valid_(other.valid_) {
        other.valid_ = false;
    }
    
    FolderToken& operator=(FolderToken&& other) noexcept {
        if (this != &other) {
            cleanup();
            temp_root_ = std::move(other.temp_root_);
            valid_ = other.valid_;
            other.valid_ = false;
        }
        return *this;
    }
    
    ~FolderToken() {
        cleanup();
    }
    
    const std::string& temp_root() const { return temp_root_; }
    bool is_valid() const { return valid_; }

private:
    void cleanup() {
        if (valid_) {
            // Release semaphore immediately (non-blocking)
            temp_slots.release();
            
            // Spawn detached thread for folder removal (non-blocking)
            if (std::filesystem::exists(temp_root_)) {
                std::string path_to_remove = temp_root_;
                std::thread cleanup_thread([path_to_remove]() {
                    std::filesystem::remove_all(path_to_remove);
                });
                cleanup_thread.detach();
            }
            
            valid_ = false;
        }
    }
};

// Asset information for folder processing
struct AssetInfo {
    std::string asset_dir;
    std::string asset_code;
};

// Work item for folder queue (per-folder granularity)
struct FolderWorkItem {
    std::string folder_id;
    std::string date_str;
    std::vector<AssetInfo> asset_list;
    FolderToken folder_token;
    
    FolderWorkItem(std::string folder_id, std::string date_str, 
                   std::vector<AssetInfo> asset_list, FolderToken folder_token)
        : folder_id(std::move(folder_id)), date_str(std::move(date_str)), 
          asset_list(std::move(asset_list)), folder_token(std::move(folder_token)) {}
    
    // Move-only semantics (due to FolderToken)
    FolderWorkItem(const FolderWorkItem&) = delete;
    FolderWorkItem& operator=(const FolderWorkItem&) = delete;
    
    FolderWorkItem(FolderWorkItem&& other) noexcept
        : folder_id(std::move(other.folder_id)), date_str(std::move(other.date_str)),
          asset_list(std::move(other.asset_list)), folder_token(std::move(other.folder_token)) {}
    
    FolderWorkItem& operator=(FolderWorkItem&& other) noexcept {
        if (this != &other) {
            folder_id = std::move(other.folder_id);
            date_str = std::move(other.date_str);
            asset_list = std::move(other.asset_list);
            folder_token = std::move(other.folder_token);
        }
        return *this;
    }
};

// Active folder shared state for cooperative processing
struct ActiveFolder {
    std::string date_str;
    std::vector<AssetInfo> asset_list;
    FolderToken folder_token;
    std::atomic<int> processed{0};
    int total;
    std::atomic<int> next_asset_index{0};
    
    ActiveFolder(std::string date_str, std::vector<AssetInfo> asset_list, FolderToken folder_token)
        : date_str(std::move(date_str)), asset_list(std::move(asset_list)), 
          folder_token(std::move(folder_token)), total(static_cast<int>(this->asset_list.size())) {}
    
    // Move-only semantics (manual implementation due to atomics)
    ActiveFolder(const ActiveFolder&) = delete;
    ActiveFolder& operator=(const ActiveFolder&) = delete;
    
    ActiveFolder(ActiveFolder&& other) noexcept
        : date_str(std::move(other.date_str)), asset_list(std::move(other.asset_list)),
          folder_token(std::move(other.folder_token)), processed(other.processed.load()),
          total(other.total), next_asset_index(other.next_asset_index.load()) {}
    
    ActiveFolder& operator=(ActiveFolder&& other) noexcept {
        if (this != &other) {
            date_str = std::move(other.date_str);
            asset_list = std::move(other.asset_list);
            folder_token = std::move(other.folder_token);
            processed.store(other.processed.load());
            total = other.total;
            next_asset_index.store(other.next_asset_index.load());
        }
        return *this;
    }
};

// Closeable bounded MPMC folder queue
class FolderQueue {
private:
    mutable std::mutex mutex_;
    std::deque<FolderWorkItem> queue_;
    const size_t max_size_;
    bool closed_ = false;

public:
    explicit FolderQueue(size_t max_size = 100) : max_size_(max_size) {}
    
    bool try_push(FolderWorkItem&& item);
    bool try_pop(FolderWorkItem& item);
    std::optional<FolderWorkItem> try_pop();
    bool is_empty() const;
    bool is_full() const;
    void close();
    bool is_closed() const;
    bool is_closed_and_empty() const;
};

// Global state following the L2_database design
extern std::counting_semaphore<> temp_slots;
extern FolderQueue folder_queue;
extern std::queue<std::string> archive_queue;
extern std::mutex archive_queue_mutex;

// Active folder shared state (optional)
extern std::unique_ptr<ActiveFolder> active_folder;
extern std::mutex active_folder_mutex;

} // namespace Parallel
} // namespace L2