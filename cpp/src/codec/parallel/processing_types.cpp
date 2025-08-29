#include "codec/parallel/processing_types.hpp"
#include "codec/parallel/processing_config.hpp"
#include <chrono>
#include <thread>
#include <filesystem>
#include <cstdlib>
#include <iostream>

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

// MultiBufferState implementation
MultiBufferState::MultiBufferState(const std::string& temp_base, uint32_t num_buffers) {
    buffer_dirs_.reserve(num_buffers);
    
    // Create buffer directories
    for (uint32_t i = 0; i < num_buffers; ++i) {
        std::string buffer_dir = temp_base + "/buffer_" + std::to_string(i);
        buffer_dirs_.push_back(buffer_dir);
        available_buffers_.insert(i);
        std::filesystem::create_directories(buffer_dir);
    }
}

MultiBufferState::~MultiBufferState() {
    // Clean up buffer directories
    for (const auto& dir : buffer_dirs_) {
        if (std::filesystem::exists(dir)) {
            std::filesystem::remove_all(dir);
        }
    }
}

std::string MultiBufferState::get_available_decomp_dir() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return !available_buffers_.empty(); });
    
    size_t index = *available_buffers_.begin();
    available_buffers_.erase(index);
    return buffer_dirs_[index];
}

void MultiBufferState::signal_ready(const std::string& dir) {
    size_t index = find_buffer_index(dir);
    if (index < buffer_dirs_.size()) {
        std::lock_guard<std::mutex> lock(mutex_);
        ready_buffers_.insert(index);
        cv_.notify_all();
    }
}

std::string MultiBufferState::wait_for_ready_dir() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { 
        return !ready_buffers_.empty() || decompression_finished_.load(); 
    });
    
    if (!ready_buffers_.empty()) {
        size_t index = *ready_buffers_.begin();
        ready_buffers_.erase(index);
        in_use_buffers_.insert(index);
        return buffer_dirs_[index];
    }
    
    return "";  // Decompression finished and no more work
}

void MultiBufferState::finish_with_dir(const std::string& dir) {
    size_t index = find_buffer_index(dir);
    if (index < buffer_dirs_.size()) {
        std::lock_guard<std::mutex> lock(mutex_);
        in_use_buffers_.erase(index);
        available_buffers_.insert(index);
        cv_.notify_all();
    }
}

void MultiBufferState::signal_decompression_finished() {
    decompression_finished_.store(true);
    cv_.notify_all();
}

size_t MultiBufferState::find_buffer_index(const std::string& dir) const {
    for (size_t i = 0; i < buffer_dirs_.size(); ++i) {
        if (buffer_dirs_[i] == dir) {
            return i;
        }
    }
    return buffer_dirs_.size(); // Invalid index
}

} // namespace Parallel
} // namespace L2
