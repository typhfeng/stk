#pragma once

#include "FeatureStoreConfig.hpp"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

// ============================================================================
// FEATURE STORE IMPLEMENTATION - HIGH-PERFORMANCE TIME-SERIES STORAGE
// ============================================================================
// Design principles:
// - Time-major layout: [time × asset × feature] for sequential access
// - Pre-allocated arrays: zero dynamic allocation in hot path
// - Aligned memory: 64-byte alignment for SIMD/cache optimization
// - Parent linking: hierarchical pointer chain for multi-level aggregation
// - Macro-generated: fully automatic code generation for all levels
//
// Memory efficiency:
// - Uniform float storage: 4 bytes per feature
// - Contiguous layout: cache-friendly sequential writes
// - Pre-allocated capacity: eliminates reallocation overhead
// ============================================================================

// ============================================================================
// LEVEL STORAGE - SINGLE-LEVEL MULTI-ASSET BUFFER
// ============================================================================
// Memory layout: [time₀...timeₜ] × [asset₀...assetₙ] × [feature₀...featureₖ]
// Parent indices: [time₀...timeₜ] × [asset₀...assetₙ] → parent_time_idx

class LevelStorage {
private:
  // Core storage
  float *data_ = nullptr;            // Feature data: time × asset × feature
  size_t *parent_indices_ = nullptr; // Parent time indices: time × asset

  // Dimension metadata
  size_t num_features_;       // Number of features per row
  size_t num_assets_;         // Number of assets
  size_t capacity_per_asset_; // Pre-allocated time steps per asset

  // Per-asset write tracking (tracks max written index, -1 means no data written)
  std::vector<int64_t> per_asset_max_written_idx_;

public:
  // ============================================================================
  // LIFECYCLE MANAGEMENT
  // ============================================================================

  LevelStorage() = default;

  ~LevelStorage() {
    if (data_)
      std::free(data_);
    if (parent_indices_)
      std::free(parent_indices_);
  }

  LevelStorage(const LevelStorage &) = delete;
  LevelStorage &operator=(const LevelStorage &) = delete;

  // Initialize with pre-allocation (called once per date-level pair)
  void initialize(size_t num_features, size_t num_assets, size_t capacity_per_asset) {
    // Prevent double initialization
    if (data_) {
      std::free(data_);
      data_ = nullptr;
    }
    if (parent_indices_) {
      std::free(parent_indices_);
      parent_indices_ = nullptr;
    }

    num_features_ = num_features;
    num_assets_ = num_assets;
    capacity_per_asset_ = capacity_per_asset;

    // Initialize per-asset max written indices (-1 means no data written yet)
    per_asset_max_written_idx_.assign(num_assets, -1);

    // Allocate feature data: time × asset × feature
    const size_t total_floats = capacity_per_asset * num_assets * num_features;
    data_ = static_cast<float *>(std::aligned_alloc(64, total_floats * sizeof(float)));
    assert(data_ && "aligned_alloc failed for data_");
    std::memset(data_, 0, total_floats * sizeof(float));

    // Allocate parent linkage: time × asset
    const size_t total_parent_slots = capacity_per_asset * num_assets;
    parent_indices_ = static_cast<size_t *>(std::aligned_alloc(64, total_parent_slots * sizeof(size_t)));
    assert(parent_indices_ && "aligned_alloc failed for parent_indices_");
    std::memset(parent_indices_, 0, total_parent_slots * sizeof(size_t));
  }

  // ============================================================================
  // WRITE OPERATIONS
  // ============================================================================

  // Push complete row for specific asset at explicit time index
  // - Write-protected: ignores attempts to overwrite existing indices
  // - Gap-filling: fills all indices from last written to current with same data
  template <typename LevelData>
  void push_row(size_t asset_id, size_t time_idx, const LevelData &data, size_t parent_row_idx);

  // ============================================================================
  // PARENT LINKAGE ACCESS
  // ============================================================================

  void set_parent_index(size_t asset_id, size_t time_idx, size_t parent_row_idx) {
    const size_t flat_idx = time_idx * num_assets_ + asset_id;
    parent_indices_[flat_idx] = parent_row_idx;
  }

  size_t get_parent_index(size_t asset_id, size_t time_idx) const {
    const size_t flat_idx = time_idx * num_assets_ + asset_id;
    return parent_indices_[flat_idx];
  }

  // ============================================================================
  // TIME INDEX MANAGEMENT (Per-asset)
  // ============================================================================

  // Get max written index for asset (-1 if no data written)
  int64_t get_max_written_idx(size_t asset_id) const {
    return per_asset_max_written_idx_[asset_id];
  }

  // Get next available index for asset (for compatibility)
  size_t get_time_idx(size_t asset_id) const {
    return static_cast<size_t>(per_asset_max_written_idx_[asset_id] + 1);
  }

  // Get maximum time index across all assets
  size_t get_max_time_idx() const {
    if (per_asset_max_written_idx_.empty())
      return 0;
    auto max_it = std::max_element(per_asset_max_written_idx_.begin(),
                                   per_asset_max_written_idx_.end());
    return *max_it < 0 ? 0 : static_cast<size_t>(*max_it + 1);
  }

  // ============================================================================
  // METADATA ACCESSORS
  // ============================================================================

  size_t get_num_assets() const { return num_assets_; }
  size_t get_num_features() const { return num_features_; }
  size_t get_capacity() const { return capacity_per_asset_; }

  // ============================================================================
  // DATA EXPORT
  // ============================================================================

  // Get raw pointer for zero-copy export
  const float *get_data_ptr() const { return data_; }
  const size_t *get_parent_indices_ptr() const { return parent_indices_; }

  // Get specific row (asset × time)
  const float *get_row(size_t asset_id, size_t time_idx) const {
    const size_t row_offset = (time_idx * num_assets_ + asset_id) * num_features_;
    return data_ + row_offset;
  }
};

// ============================================================================
// DAILY FEATURE TENSOR - MULTI-LEVEL STORAGE FOR ONE DATE
// ============================================================================
// Contains all levels (L0, L1, L2, ...) for a single trading date
// Each level stores data for all assets with shared time indices

class DailyFeatureTensor {
private:
  LevelStorage levels_[LEVEL_COUNT]; // Storage for each level
  std::string date_;                 // Trading date identifier
  size_t num_assets_;                // Number of assets in this tensor

public:
  // ============================================================================
  // INITIALIZATION
  // ============================================================================

  DailyFeatureTensor(const std::string &date, size_t num_assets)
      : date_(date), num_assets_(num_assets) {
    // Initialize all levels with their respective capacities
    for (size_t level_idx = 0; level_idx < LEVEL_COUNT; ++level_idx) {
      levels_[level_idx].initialize(
          FIELDS_PER_LEVEL[level_idx],
          num_assets,
          MAX_ROWS_PER_LEVEL[level_idx]);
    }
  }

  // ============================================================================
  // LEVEL ACCESS
  // ============================================================================

  LevelStorage &get_level(size_t level_idx) { return levels_[level_idx]; }
  const LevelStorage &get_level(size_t level_idx) const { return levels_[level_idx]; }

  // ============================================================================
  // METADATA ACCESS
  // ============================================================================

  const std::string &get_date() const { return date_; }
  void set_date(const std::string &date) { date_ = date; }
  size_t get_num_assets() const { return num_assets_; }
};

// ============================================================================
// GLOBAL FEATURE STORE - DATE-SHARDED MULTI-ASSET MANAGER
// ============================================================================
// Top-level container managing all dates and assets
// Optimized for hot path: cached pointers, minimal lookups
// Thread-safe: Each asset has independent thread, no cross-asset contention

class GlobalFeatureStore {
private:
  // Pre-allocated tensor pool
  std::vector<std::unique_ptr<DailyFeatureTensor>> tensor_pool_;
  std::atomic<size_t> next_tensor_idx_{0};

  // Date to tensor mapping
  std::map<std::string, DailyFeatureTensor *> date_to_tensor_;
  mutable std::shared_mutex map_mutex_;

  size_t num_assets_;

  // Per-asset cached pointers (HOT PATH optimization)
  std::vector<DailyFeatureTensor *> current_tensors_;
  std::vector<std::string> current_dates_;

public:
  // ============================================================================
  // INITIALIZATION
  // ============================================================================

  explicit GlobalFeatureStore(size_t num_assets, size_t preallocated_blocks = 50)
      : num_assets_(num_assets),
        current_tensors_(num_assets, nullptr),
        current_dates_(num_assets) {

    // Calculate memory per level
    size_t total_features = 0;
    size_t level_sizes[LEVEL_COUNT];
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
      level_sizes[lvl] = MAX_ROWS_PER_LEVEL[lvl] * num_assets * FIELDS_PER_LEVEL[lvl] * sizeof(float);
      total_features += FIELDS_PER_LEVEL[lvl];
    }
    size_t bytes_per_day = 0;
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
      bytes_per_day += level_sizes[lvl];
    }

    // Print allocation info
    std::cout << "Preallocating Feature Store:\n";
    std::cout << "  Daily: ";
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
      std::cout << "L" << lvl << "(" << (level_sizes[lvl] / (1024.0 * 1024.0)) << "MB)";
      if (lvl + 1 < LEVEL_COUNT)
        std::cout << " + ";
    }
    std::cout << " × " << preallocated_blocks << " days = "
              << (bytes_per_day * preallocated_blocks / (1024.0 * 1024.0 * 1024.0)) << " GB\n";
    std::cout << "  Features: " << total_features << "\n\n";

    // Preallocate tensor blocks
    tensor_pool_.reserve(preallocated_blocks);
    for (size_t i = 0; i < preallocated_blocks; ++i) {
      tensor_pool_.emplace_back(std::make_unique<DailyFeatureTensor>("", num_assets));
    }
  }

  // ============================================================================
  // DATE SWITCHING (Called by main.cpp when asset moves to next date)
  // ============================================================================

  // Switch asset to new date - updates cached pointer
  void switch_date(size_t asset_id, const std::string &date) {
    assert(asset_id < num_assets_ && "Invalid asset_id");
    current_dates_[asset_id] = date;

    // Fast path: check if date already bound
    {
      std::shared_lock lock(map_mutex_);
      auto it = date_to_tensor_.find(date);
      if (it != date_to_tensor_.end()) {
        current_tensors_[asset_id] = it->second;
        return;
      }
    }

    // Slow path: allocate tensor from pool (lock-free for pool access)
    size_t idx = next_tensor_idx_.fetch_add(1, std::memory_order_relaxed);
    DailyFeatureTensor *tensor;

    if (idx < tensor_pool_.size()) {
      // Use preallocated block
      tensor = tensor_pool_[idx].get();
    } else {
      // Pool exhausted, allocate new block on demand
      std::unique_lock lock(map_mutex_);
      tensor_pool_.emplace_back(std::make_unique<DailyFeatureTensor>("", num_assets_));
      tensor = tensor_pool_.back().get();
    }

    // Bind date to tensor
    tensor->set_date(date);

    // Register mapping (write lock)
    {
      std::unique_lock lock(map_mutex_);
      date_to_tensor_[date] = tensor;
    }

    current_tensors_[asset_id] = tensor;
  }

  // Legacy compatibility (redirects to switch_date)
  void set_current_date(size_t asset_id, const std::string &date,
                        [[maybe_unused]] const size_t *level_reserve_sizes = nullptr) {
    switch_date(asset_id, date);
  }

  // ============================================================================
  // HOT PATH: ULTRA-FAST PUSH (Called billions of times)
  // ============================================================================

  // Push with explicit time index - caller manages time granularity
  // No map lookup, no string comparison - just cached pointer dereference
  template <typename LevelData>
  inline void push(size_t level_idx, size_t asset_id, size_t time_idx, const LevelData *data) {
    DailyFeatureTensor *tensor = current_tensors_[asset_id];
    assert(tensor && "Call switch_date before push");

    LevelStorage &level = tensor->get_level(level_idx);

    // Auto-determine parent index from parent level
    size_t parent_idx = 0;
    if (level_idx > 0) {
      parent_idx = tensor->get_level(level_idx - 1).get_time_idx(asset_id);
    }

    level.push_row(asset_id, time_idx, *data, parent_idx);
  }

  // ============================================================================
  // TENSOR ACCESS (For advanced use)
  // ============================================================================

  DailyFeatureTensor *get_tensor(const std::string &date) const {
    std::shared_lock lock(map_mutex_);
    auto it = date_to_tensor_.find(date);
    return it != date_to_tensor_.end() ? it->second : nullptr;
  }

  const std::string &get_current_date(size_t asset_id) const {
    return current_dates_[asset_id];
  }

  // ============================================================================
  // EXPORT: HIERARCHICAL TENSOR WITH PARENT LINKING
  // ============================================================================

  // Export single date as [time × asset × all_level_features]
  // Features arranged as: [L0_features | L1_features | L2_features | ...]
  // Each row follows parent chain to link multi-level features
  std::vector<float> export_date_tensor(const std::string &date) const {
    DailyFeatureTensor *tensor = get_tensor(date);
    if (!tensor)
      return {};

    // Calculate dimensions
    const size_t max_time = tensor->get_level(L0_INDEX).get_max_time_idx();
    if (max_time == 0)
      return {};

    size_t total_features = 0;
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
      total_features += FIELDS_PER_LEVEL[lvl];
    }

    // Allocate output: [time × asset × total_features]
    std::vector<float> output(max_time * num_assets_ * total_features, 0.0f);

    // Export each asset independently
    for (size_t asset_id = 0; asset_id < num_assets_; ++asset_id) {
      const size_t asset_time = tensor->get_level(L0_INDEX).get_time_idx(asset_id);

      for (size_t t = 0; t < asset_time; ++t) {
        float *row_out = output.data() + (t * num_assets_ + asset_id) * total_features;
        size_t feature_offset = 0;

        // Follow parent chain across levels
        size_t parent_time_idx = t;
        for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
          const LevelStorage &level = tensor->get_level(lvl);
          const float *src = level.get_row(asset_id, parent_time_idx);
          const size_t num_features = FIELDS_PER_LEVEL[lvl];

          std::memcpy(row_out + feature_offset, src, num_features * sizeof(float));
          feature_offset += num_features;

          // Get parent index for next level
          if (lvl + 1 < LEVEL_COUNT) {
            parent_time_idx = level.get_parent_index(asset_id, parent_time_idx);
          }
        }
      }
    }

    return output;
  }

  // ============================================================================
  // METADATA ACCESS
  // ============================================================================

  size_t get_num_assets() const { return num_assets_; }

  size_t get_num_dates() const {
    std::shared_lock lock(map_mutex_);
    return date_to_tensor_.size();
  }
};

// ============================================================================
// AUTO-GENERATED: push_row TEMPLATE SPECIALIZATIONS
// ============================================================================
// Fully macro-driven generation - automatically scales to all levels
// Each specialization performs:
// 1. Write protection: return if time_idx already written
// 2. Gap filling: fill all indices from (max_written + 1) to time_idx
// 3. Compute flat array offset: (time × num_assets + asset) × num_features
// 4. Memcpy struct fields to destination for all filled indices
// 5. Record parent row index for hierarchical linking
// 6. Update max_written_idx for this asset

#define GENERATE_PUSH_ROW_SPECIALIZATION(level_name, level_num, fields)                \
  template <>                                                                          \
  inline void LevelStorage::push_row<Level##level_num##Data>(                          \
      size_t asset_id,                                                                 \
      size_t time_idx,                                                                 \
      const Level##level_num##Data &data,                                              \
      size_t parent_row_idx) {                                                         \
    assert(time_idx < capacity_per_asset_ && "time_idx exceeds capacity");             \
    /* Write protection: ignore if already written */                                  \
    const int64_t max_written = per_asset_max_written_idx_[asset_id];                  \
    if (static_cast<int64_t>(time_idx) <= max_written)                                 \
      return;                                                                          \
    /* Gap filling: write same data to all indices from (max_written+1) to time_idx */ \
    const size_t start_idx = static_cast<size_t>(max_written + 1);                     \
    const float *src = reinterpret_cast<const float *>(&data);                         \
    for (size_t idx = start_idx; idx <= time_idx; ++idx) {                             \
      const size_t row_offset = (idx * num_assets_ + asset_id) * num_features_;        \
      float *dest = data_ + row_offset;                                                \
      std::memcpy(dest, src, num_features_ * sizeof(float));                           \
      parent_indices_[idx * num_assets_ + asset_id] = parent_row_idx;                  \
    }                                                                                  \
    /* Update max written index */                                                     \
    per_asset_max_written_idx_[asset_id] = static_cast<int64_t>(time_idx);             \
  }

// Generate specializations for all levels
ALL_LEVELS(GENERATE_PUSH_ROW_SPECIALIZATION)
