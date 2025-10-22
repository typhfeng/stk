#pragma once

#include <atomic>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace misc {

// Forward declaration
class ParallelProgress;

// Worker handle: lightweight, auto-releasing (RAII)
// Each worker thread gets one handle and simply calls update()
// Handle automatically releases slot when destroyed
class ProgressHandle {
public:
  ProgressHandle() : progress_(nullptr), worker_id_(-1), auto_release_(false) {}

  // Move constructor (transfer ownership)
  ProgressHandle(ProgressHandle &&other) noexcept
      : progress_(other.progress_), worker_id_(other.worker_id_), auto_release_(other.auto_release_) {
    other.progress_ = nullptr;
    other.worker_id_ = -1;
    other.auto_release_ = false;
  }

  // Move assignment
  ProgressHandle &operator=(ProgressHandle &&other) noexcept {
    if (this != &other) {
      release();
      progress_ = other.progress_;
      worker_id_ = other.worker_id_;
      auto_release_ = other.auto_release_;
      other.progress_ = nullptr;
      other.worker_id_ = -1;
      other.auto_release_ = false;
    }
    return *this;
  }

  // Destructor: auto-release slot
  ~ProgressHandle() {
    release();
  }

  // Delete copy (move-only)
  ProgressHandle(const ProgressHandle &) = delete;
  ProgressHandle &operator=(const ProgressHandle &) = delete;

  // Update progress (fast, lock-free) - defined after ParallelProgress
  void update(size_t current, size_t total, const std::string &msg = "") const;

  // Set label (e.g., asset code) - defined after ParallelProgress
  void set_label(const std::string &label) const;

  // Check if handle is valid
  bool valid() const { return progress_ != nullptr && worker_id_ >= 0; }

private:
  friend class ParallelProgress;
  ProgressHandle(ParallelProgress *progress, int worker_id, bool auto_release)
      : progress_(progress), worker_id_(worker_id), auto_release_(auto_release) {}

  void release();  // Defined after ParallelProgress

  ParallelProgress *progress_;
  int worker_id_;
  bool auto_release_;
};

// Parallel progress tracker: manages all worker progress displays
// Usage:
//   auto tracker = std::make_shared<ParallelProgress>(num_workers);
//   auto handle = tracker->acquire_slot();  // Get handle for worker
//   handle.update(i, total, "processing...");  // Worker updates progress
class ParallelProgress : public std::enable_shared_from_this<ParallelProgress> {
private:
  // Cache-line aligned worker slot (prevents false sharing)
  struct alignas(64) WorkerSlot {
    std::atomic<size_t> current{0};
    std::atomic<size_t> total{0};
    std::atomic<bool> dirty{false};
    std::atomic<bool> active{false};  // Slot is in use
    char label[32] = {0};
    char message[96] = {0};
  };

public:
  explicit ParallelProgress(int num_workers, int refresh_interval_ms = 100)
      : num_workers_(num_workers),
        refresh_interval_ms_(refresh_interval_ms),
        slots_(num_workers),
        running_(true),
        initialized_(false) {

    // Print initial empty progress bars
    for (int i = 0; i < num_workers_; ++i) {
      std::cout << std::string(bar_width_ + 60, ' ') << "\n";
    }
    std::cout << std::flush;

    initialized_ = true;

    // Start refresh thread
    refresh_thread_ = std::thread(&ParallelProgress::refresh_loop, this);
  }

  ~ParallelProgress() {
    stop();
  }

  // Acquire a worker slot (returns RAII handle that auto-releases)
  ProgressHandle acquire_slot() {
    std::lock_guard<std::mutex> lock(slot_mutex_);

    for (int i = 0; i < num_workers_; ++i) {
      bool expected = false;
      if (slots_[i].active.compare_exchange_strong(expected, true)) {
        return ProgressHandle(this, i, true);  // auto_release = true
      }
    }

    // No available slot (shouldn't happen if used correctly)
    return ProgressHandle();
  }

  // Release a worker slot (optional, handle can be dropped)
  void release_slot(int worker_id) {
    if (worker_id >= 0 && worker_id < num_workers_) {
      slots_[worker_id].active.store(false, std::memory_order_release);
      slots_[worker_id].dirty.store(true, std::memory_order_release);
    }
  }

  // Stop refresh thread and finalize display
  void stop() {
    if (running_.exchange(false, std::memory_order_release)) {
      if (refresh_thread_.joinable()) {
        refresh_thread_.join();
      }

      if (initialized_) {
        refresh_all_lines(true);
        std::cout << "\n" << std::flush;
      }
    }
  }

private:
  friend class ProgressHandle;

  // Internal update (called by handle)
  void update_internal(int worker_id, size_t current, size_t total, const std::string &msg) {
    WorkerSlot &slot = slots_[worker_id];
    slot.current.store(current, std::memory_order_relaxed);
    slot.total.store(total, std::memory_order_relaxed);

    if (!msg.empty()) {
      size_t len = std::min(msg.size(), sizeof(slot.message) - 1);
      std::memcpy(slot.message, msg.c_str(), len);
      slot.message[len] = '\0';
    }

    slot.dirty.store(true, std::memory_order_release);
  }

  // Internal set label (called by handle)
  void set_label_internal(int worker_id, const std::string &label) {
    WorkerSlot &slot = slots_[worker_id];
    size_t len = std::min(label.size(), sizeof(slot.label) - 1);
    std::memcpy(slot.label, label.c_str(), len);
    slot.label[len] = '\0';
  }

  void refresh_loop() {
    while (running_.load(std::memory_order_acquire)) {
      refresh_all_lines(false);
      std::this_thread::sleep_for(std::chrono::milliseconds(refresh_interval_ms_));
    }
  }

  void refresh_all_lines(bool force) {
    std::ostringstream buffer;

    for (int i = 0; i < num_workers_; ++i) {
      WorkerSlot &slot = slots_[i];

      bool is_dirty = slot.dirty.exchange(false, std::memory_order_acquire);
      if (!is_dirty && !force) continue;

      size_t current = slot.current.load(std::memory_order_relaxed);
      size_t total = slot.total.load(std::memory_order_relaxed);
      bool active = slot.active.load(std::memory_order_relaxed);

      // Move cursor to target line
      int lines_up = num_workers_ - i;
      buffer << "\033[" << lines_up << "A\r";

      // Render progress bar
      float progress = (total > 0) ? static_cast<float>(current) / total : 0.0f;
      int filled = static_cast<int>(bar_width_ * progress);

      buffer << "[";
      for (int j = 0; j < bar_width_; ++j) {
        if (j < filled)
          buffer << "=";
        else if (j == filled && active && current < total)
          buffer << ">";
        else
          buffer << " ";
      }
      buffer << "] " << std::setw(3) << static_cast<int>(progress * 100) << "% "
             << "(" << current << "/" << total << ")";

      if (slot.label[0] != '\0') {
        buffer << " " << slot.label;
      }
      if (slot.message[0] != '\0') {
        buffer << " - " << slot.message;
      }

      buffer << "\033[K";
      buffer << "\033[" << lines_up << "B";
    }

    std::cout << buffer.str() << std::flush;
  }

  int num_workers_;
  int bar_width_ = 40;
  int refresh_interval_ms_;

  std::vector<WorkerSlot> slots_;
  std::atomic<bool> running_;
  bool initialized_;
  std::mutex slot_mutex_;
  std::thread refresh_thread_;
};

// ProgressHandle member function implementations (after ParallelProgress is defined)
inline void ProgressHandle::update(size_t current, size_t total, const std::string &msg) const {
  if (progress_ && worker_id_ >= 0) {
    progress_->update_internal(worker_id_, current, total, msg);
  }
}

inline void ProgressHandle::set_label(const std::string &label) const {
  if (progress_ && worker_id_ >= 0) {
    progress_->set_label_internal(worker_id_, label);
  }
}

inline void ProgressHandle::release() {
  if (auto_release_ && progress_ && worker_id_ >= 0) {
    progress_->release_slot(worker_id_);
  }
}

} // namespace misc
