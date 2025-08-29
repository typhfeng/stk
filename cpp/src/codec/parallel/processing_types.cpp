#include "codec/parallel/processing_types.hpp"
#include "codec/parallel/processing_config.hpp"
#include <filesystem>
#include <cstdlib>

namespace L2 {
namespace Parallel {

// Global configuration instance
ProcessingConfig g_config;

// TaskQueue implementation
void TaskQueue::push(const EncodingTask& task) {
    std::lock_guard<std::mutex> lock(mutex_);
    tasks_.push(task);
    cv_.notify_one();
}

bool TaskQueue::pop(EncodingTask& task) {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return !tasks_.empty() || finished_.load(); });
    
    if (tasks_.empty()) {
        return false;
    }
    
    task = tasks_.front();
    tasks_.pop();
    return true;
}

void TaskQueue::finish() {
    finished_.store(true);
    cv_.notify_all();
}

size_t TaskQueue::size() const {
    std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(mutex_));
    return tasks_.size();
}

// BufferState implementation
BufferState::BufferState(const std::string& temp_base) 
    : temp_base_(temp_base) {
    std::filesystem::create_directories(temp_base_);
}

BufferState::~BufferState() {
    if (std::filesystem::exists(temp_base_)) {
        std::filesystem::remove_all(temp_base_);
    }
}

void BufferState::add_archive(const std::string& archive_path) {
    std::lock_guard<std::mutex> lock(mutex_);
    archives_.push(archive_path);
    cv_.notify_all();
}

bool BufferState::get_next_archive(std::string& archive_path) {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { 
        return !archives_.empty() || decompression_finished_.load(); 
    });
    
    if (archives_.empty()) {
        return false;
    }
    
    archive_path = archives_.front();
    archives_.pop();
    return true;
}

void BufferState::signal_folder_ready(const std::string& date_folder) {
    std::lock_guard<std::mutex> lock(mutex_);
    ready_folders_.push(date_folder);
    cv_.notify_all();
}

std::string BufferState::get_ready_folder() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { 
        return !ready_folders_.empty() || decompression_finished_.load(); 
    });
    
    if (ready_folders_.empty()) {
        return "";
    }
    
    std::string date_folder = ready_folders_.front();
    ready_folders_.pop();
    return date_folder;
}

void BufferState::signal_decompression_finished() {
    decompression_finished_.store(true);
    cv_.notify_all();
}

std::string BufferState::get_date_folder(const std::string& archive_path) const {
    std::string archive_name = std::filesystem::path(archive_path).stem().string();
    return temp_base_ + "/" + archive_name;
}

} // namespace Parallel
} // namespace L2
