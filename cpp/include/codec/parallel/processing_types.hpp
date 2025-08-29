#pragma once

#include <string>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace L2 {
namespace Parallel {

/**
 * Task structure for encoding work distributed to worker threads
 */
struct EncodingTask {
    std::string asset_dir;      // Full path to asset directory (e.g., temp/20170103/600000.SH)
    std::string asset_code;     // Asset code (e.g., 600000.SH)
    std::string date_str;       // Date string (e.g., 20170103)
    std::string output_base;    // Base output directory
};

/**
 * Thread-safe task queue for distributing encoding work
 */
class TaskQueue {
private:
    std::queue<EncodingTask> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> finished_{false};

public:
    void push(const EncodingTask& task);
    bool pop(EncodingTask& task);
    void finish();
    size_t size() const;
};

/**
 * Simplified buffer state for decompression and encoding coordination
 */
class BufferState {
private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> decompression_finished_{false};
    
    std::string temp_base_;
    std::queue<std::string> archives_;        // Archives to process
    std::queue<std::string> ready_folders_;   // Folders ready for encoding
    
public:
    explicit BufferState(const std::string& temp_base);
    ~BufferState();
    
    void add_archive(const std::string& archive_path);
    bool get_next_archive(std::string& archive_path);
    void signal_folder_ready(const std::string& date_folder);
    std::string get_ready_folder();
    void signal_decompression_finished();
    std::string get_date_folder(const std::string& archive_path) const;
};

} // namespace Parallel
} // namespace L2
