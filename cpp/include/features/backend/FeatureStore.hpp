#pragma once

#include "FeatureStoreConfig.hpp"
#include <atomic>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <string>

// ============================================================================
// FEATURE STORE CONFIGURATION
// ============================================================================
// Control tensor flush strategy:
// - true:  Flush unified daily tensor [T_L0, F_total, A] (GPU-friendly, single file)
// - false: Flush separate level tensors [T_L0, F0, A], [T_L1, F1, A], [T_L2, F2, A]
#define STORE_UNIFIED_DAILY_TENSOR false

// ============================================================================
// FEATURE STORE - Single class interface
// ============================================================================
// Design: [T][F][A] layout for optimal CS operations
// Lockfree sync: per-TS-core progress tracking
// ============================================================================

class GlobalFeatureStore {
private:
  // Per-date storage
  struct DayData {
    std::string date;                                           // Current date stored
    bool in_use = false;                                        // Is this slot in use
    std::atomic<bool> cs_done{false};                          // CS processing complete
    
    feature_storage_t* data[LEVEL_COUNT] = {nullptr};          // [level][T][F][A] stored as _Float16
    std::atomic<size_t>* ts_progress[LEVEL_COUNT] = {nullptr}; // [level][core]
    
    // Disable copy (atomic cannot be copied)
    DayData() = default;
    DayData(const DayData&) = delete;
    DayData& operator=(const DayData&) = delete;
    DayData(DayData&&) = delete;
    DayData& operator=(DayData&&) = delete;
    
    void allocate(size_t num_assets, size_t num_cores) {
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
        const size_t T = MAX_ROWS_PER_LEVEL[lvl];
        const size_t F = FIELDS_PER_LEVEL[lvl];
        const size_t A = num_assets;
        
        if (!data[lvl]) {
          const size_t total_elements = T * F * A;
          data[lvl] = static_cast<feature_storage_t*>(std::aligned_alloc(64, total_elements * sizeof(feature_storage_t)));
          assert(data[lvl] && "aligned_alloc failed");
        }
        if (!ts_progress[lvl]) {
          ts_progress[lvl] = new std::atomic<size_t>[num_cores]();
        }
      }
    }
    
    void reset() {
      in_use = false;
      cs_done.store(false);
      date.clear();
      
      // Zero out all data
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
        if (data[lvl]) {
          const size_t T = MAX_ROWS_PER_LEVEL[lvl];
          const size_t F = FIELDS_PER_LEVEL[lvl];
          const size_t A = num_assets_;
          std::memset(data[lvl], 0, T * F * A * sizeof(feature_storage_t));
        }
        if (ts_progress[lvl]) {
          for (size_t c = 0; c < num_cores_; ++c) {
            ts_progress[lvl][c].store(0);
          }
        }
      }
    }
    
    ~DayData() {
      for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
        if (data[lvl]) std::free(data[lvl]);
        if (ts_progress[lvl]) delete[] ts_progress[lvl];
      }
    }
    
  private:
    size_t num_assets_ = 0;
    size_t num_cores_ = 0;
    friend class GlobalFeatureStore;
  };
  
  DayData** tensor_pool_ = nullptr;
  std::map<std::string, DayData*> date_map_;
  mutable std::shared_mutex map_mutex_;
  
  const size_t num_assets_;
  const size_t num_cores_;
  const size_t pool_size_;
  std::string output_dir_ = "./output/features";
  
  // Export tensor to file (unified or separate based on STORE_UNIFIED_DAILY_TENSOR)
  void flush_tensor(DayData* day) {
    if (!day || !day->in_use) return;
    
    const std::string& date_str = day->date;
    if (date_str.size() != 8) return;
    
    // Create directory: output/features/YYYY/MM/DD
    std::string year = date_str.substr(0, 4);
    std::string month = date_str.substr(4, 2);
    std::string day_str = date_str.substr(6, 2);
    std::string out_dir = output_dir_ + "/" + year + "/" + month + "/" + day_str;
    std::filesystem::create_directories(out_dir);
    
    const size_t T0 = MAX_ROWS_PER_LEVEL[0];
    const size_t T1 = MAX_ROWS_PER_LEVEL[1];
    const size_t T2 = MAX_ROWS_PER_LEVEL[2];
    const size_t F0 = FIELDS_PER_LEVEL[0];
    const size_t F1 = FIELDS_PER_LEVEL[1];
    const size_t F2 = FIELDS_PER_LEVEL[2];
    const size_t A = num_assets_;
    
#if STORE_UNIFIED_DAILY_TENSOR
    // Unified mode: [T_L0, F_total, A] single file
    const size_t F_total = F0 + F1 + F2;
    std::string tensor_file = out_dir + "/features.bin";
    std::ofstream ofs(tensor_file, std::ios::binary);
    if (!ofs) {
      std::cerr << "Failed to open unified tensor file: " << tensor_file << std::endl;
      return;
    }
    
    // Write header: [T, F, A]
    ofs.write(reinterpret_cast<const char*>(&T0), sizeof(size_t));
    ofs.write(reinterpret_cast<const char*>(&F_total), sizeof(size_t));
    ofs.write(reinterpret_cast<const char*>(&A), sizeof(size_t));
    
    // Get link feature offsets from L0
    const size_t link_to_L1_offset = L0_FieldOffset::_link_to_L1;
    const size_t link_to_L2_offset = L0_FieldOffset::_link_to_L2;
    
    // Write data: for each L0 time t0
    for (size_t t0 = 0; t0 < T0; ++t0) {
      // Read L1/L2 indices from L0 link features (stored as uint16_t, reinterpret as index)
      // Use first asset's link (all assets share same time mapping)
      const size_t t1 = static_cast<size_t>(day->data[0][t0 * F0 * A + link_to_L1_offset * A]);
      const size_t t2 = static_cast<size_t>(day->data[0][t0 * F0 * A + link_to_L2_offset * A]);
      
      // L0 features
      for (size_t f = 0; f < F0; ++f) {
        ofs.write(reinterpret_cast<const char*>(day->data[0] + t0 * F0 * A + f * A), A * sizeof(feature_storage_t));
      }
      
      // L1 features (upsampled via link)
      for (size_t f = 0; f < F1; ++f) {
        ofs.write(reinterpret_cast<const char*>(day->data[1] + t1 * F1 * A + f * A), A * sizeof(feature_storage_t));
      }
      
      // L2 features (upsampled via link)
      for (size_t f = 0; f < F2; ++f) {
        ofs.write(reinterpret_cast<const char*>(day->data[2] + t2 * F2 * A + f * A), A * sizeof(feature_storage_t));
      }
    }
    
    if (ofs) {
      std::cout << "Flushed unified tensor: " << date_str << " [" << T0 << "×" << F_total << "×" << A << "] (float16)" << std::endl;
    } else {
      std::cerr << "Error writing unified tensor for date " << date_str << std::endl;
    }
#else
    // Separate mode: 3 level files (link already in L0 features, no separate metadata file needed)
    // L0 tensor: [T0, F0, A] (includes _link_to_L1 and _link_to_L2 features)
    {
      std::string l0_file = out_dir + "/features_L0.bin";
      std::ofstream ofs(l0_file, std::ios::binary);
      if (!ofs) {
        std::cerr << "Failed to open L0 file: " << l0_file << std::endl;
        return;
      }
      ofs.write(reinterpret_cast<const char*>(&T0), sizeof(size_t));
      ofs.write(reinterpret_cast<const char*>(&F0), sizeof(size_t));
      ofs.write(reinterpret_cast<const char*>(&A), sizeof(size_t));
      for (size_t t = 0; t < T0; ++t) {
        for (size_t f = 0; f < F0; ++f) {
          ofs.write(reinterpret_cast<const char*>(day->data[0] + t * F0 * A + f * A), A * sizeof(feature_storage_t));
        }
      }
    }
    
    // L1 tensor: [T1, F1, A]
    {
      std::string l1_file = out_dir + "/features_L1.bin";
      std::ofstream ofs(l1_file, std::ios::binary);
      if (!ofs) {
        std::cerr << "Failed to open L1 file: " << l1_file << std::endl;
        return;
      }
      ofs.write(reinterpret_cast<const char*>(&T1), sizeof(size_t));
      ofs.write(reinterpret_cast<const char*>(&F1), sizeof(size_t));
      ofs.write(reinterpret_cast<const char*>(&A), sizeof(size_t));
      for (size_t t = 0; t < T1; ++t) {
        for (size_t f = 0; f < F1; ++f) {
          ofs.write(reinterpret_cast<const char*>(day->data[1] + t * F1 * A + f * A), A * sizeof(feature_storage_t));
        }
      }
    }
    
    // L2 tensor: [T2, F2, A]
    {
      std::string l2_file = out_dir + "/features_L2.bin";
      std::ofstream ofs(l2_file, std::ios::binary);
      if (!ofs) {
        std::cerr << "Failed to open L2 file: " << l2_file << std::endl;
        return;
      }
      ofs.write(reinterpret_cast<const char*>(&T2), sizeof(size_t));
      ofs.write(reinterpret_cast<const char*>(&F2), sizeof(size_t));
      ofs.write(reinterpret_cast<const char*>(&A), sizeof(size_t));
      for (size_t t = 0; t < T2; ++t) {
        for (size_t f = 0; f < F2; ++f) {
          ofs.write(reinterpret_cast<const char*>(day->data[2] + t * F2 * A + f * A), A * sizeof(feature_storage_t));
        }
      }
    }
    
    std::cout << "Flushed separate tensors: " << date_str 
              << " [L0:" << T0 << "×" << F0 << "×" << A 
              << ", L1:" << T1 << "×" << F1 << "×" << A 
              << ", L2:" << T2 << "×" << F2 << "×" << A << "] (float16, link in L0)" << std::endl;
#endif
  }
  
  // Find a finished tensor to recycle
  DayData* find_finished_tensor() {
    for (size_t i = 0; i < pool_size_; ++i) {
      if (tensor_pool_[i]->in_use && tensor_pool_[i]->cs_done.load(std::memory_order_acquire)) {
        return tensor_pool_[i];
      }
    }
    return nullptr;
  }
  
  // Get or create day data with tensor pool recycling
  DayData* get_day_data(const std::string& date) {
    // Fast path: already allocated
    {
      std::shared_lock lock(map_mutex_);
      auto it = date_map_.find(date);
      if (it != date_map_.end()) return it->second;
    }
    
    // Slow path: allocate new tensor
    std::unique_lock lock(map_mutex_);
    
    // Double check
    auto it = date_map_.find(date);
    if (it != date_map_.end()) return it->second;
    
    // Find free slot
    DayData* free_slot = nullptr;
    for (size_t i = 0; i < pool_size_; ++i) {
      if (!tensor_pool_[i]->in_use) {
        free_slot = tensor_pool_[i];
        break;
      }
    }
    
    // Pool full: find finished tensor to flush and recycle
    if (!free_slot) {
      free_slot = find_finished_tensor();
      if (free_slot) {
        flush_tensor(free_slot);
        date_map_.erase(free_slot->date);
        free_slot->reset();
      } else {
        // All tensors still in use, can't recycle (should not happen if pool sized correctly)
        std::cerr << "Warning: Tensor pool exhausted, cannot allocate for " << date << "\n";
        return nullptr;
      }
    }
    
    // Allocate and initialize
    free_slot->num_assets_ = num_assets_;
    free_slot->num_cores_ = num_cores_;
    free_slot->allocate(num_assets_, num_cores_);
    free_slot->in_use = true;
    free_slot->date = date;
    date_map_[date] = free_slot;
    
    return free_slot;
  }

public:
  GlobalFeatureStore(size_t num_assets, size_t num_cores, size_t pool_size = 10, const std::string& output_dir = "")
      : num_assets_(num_assets), num_cores_(num_cores), pool_size_(pool_size) {
    
    if (!output_dir.empty()) {
        output_dir_ = output_dir;
        // Auto wipe and create
        if (std::filesystem::exists(output_dir_)) {
            std::cout << "Wiping and creating output directory: " << output_dir_ << std::endl;
            std::filesystem::remove_all(output_dir_);
        }
        std::filesystem::create_directories(output_dir_);
    }

    size_t bytes_per_day = 0;
    size_t bytes_per_level[LEVEL_COUNT] = {0};
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
      bytes_per_level[lvl] = MAX_ROWS_PER_LEVEL[lvl] * num_assets * FIELDS_PER_LEVEL[lvl] * sizeof(feature_storage_t);
      bytes_per_day += bytes_per_level[lvl];
    }
    
    std::cout << "Feature Store Tensor Pool:\n";
    std::cout << "  Pool size: " << pool_size << " tensors\n";
#if STORE_UNIFIED_DAILY_TENSOR
    std::cout << "  Storage mode: Unified daily tensor\n";
#else
    std::cout << "  Storage mode: Separate-level daily tensors\n";
#endif
    for (size_t lvl = 0; lvl < LEVEL_COUNT; ++lvl) {
      std::cout << "  L" << lvl << ": "
                << MAX_ROWS_PER_LEVEL[lvl] << "×"
                << FIELDS_PER_LEVEL[lvl] << "×"
                << num_assets << " -> "
                << (bytes_per_level[lvl] / (1024.0 * 1024.0)) << " MB\n";
    }
    std::cout << "  Per tensor: " << (bytes_per_day / (1024.0 * 1024.0)) << " MB\n";
    std::cout << "  Total memory: " << (bytes_per_day * pool_size / (1024.0 * 1024.0 * 1024.0)) << " GB\n";
    
    // Pre-allocate tensor pool
    tensor_pool_ = new DayData*[pool_size];
    for (size_t i = 0; i < pool_size; ++i) {
      tensor_pool_[i] = new DayData();
      tensor_pool_[i]->num_assets_ = num_assets;
      tensor_pool_[i]->num_cores_ = num_cores;
      tensor_pool_[i]->allocate(num_assets, num_cores);
      tensor_pool_[i]->reset();
    }
  }
  
  ~GlobalFeatureStore() {
    if (tensor_pool_) {
      for (size_t i = 0; i < pool_size_; ++i) {
        delete tensor_pool_[i];
      }
      delete[] tensor_pool_;
    }
  }
  
  // ===== TS Worker Interface =====
  void mark_ts_core_done(const std::string& date, size_t level_idx, 
                         size_t core_id, size_t l0_time_index) {
    auto* day = get_day_data(date);
    if (!day) {
      std::cerr << "FATAL: Failed to allocate tensor for " << date << "\n";
      std::exit(1);
    }
    day->ts_progress[level_idx][core_id].store(l0_time_index + 1, std::memory_order_release);
  }
  
  // ===== CS Worker Interface =====
  bool is_timeslot_ready(const std::string& date, size_t level_idx, 
                         size_t l0_time_index) const {
    std::shared_lock lock(map_mutex_);
    auto it = date_map_.find(date);
    if (it == date_map_.end()) return false;
    
    auto* day = it->second;
    for (size_t core_id = 0; core_id < num_cores_; ++core_id) {
      if (day->ts_progress[level_idx][core_id].load(std::memory_order_acquire) <= l0_time_index) {
        return false;
      }
    }
    return true;
  }
  
  // ===== Data Access (for macros) =====
  feature_storage_t* get_data_ptr(const std::string& date, size_t level_idx) {
    return get_day_data(date)->data[level_idx];
  }
  
  size_t get_F(size_t level_idx) const { 
    return FIELDS_PER_LEVEL[level_idx]; 
  }
  
  size_t get_A() const { 
    return num_assets_; 
  }
  
  size_t get_T(size_t level_idx) const { 
    return MAX_ROWS_PER_LEVEL[level_idx]; 
  }
  
  size_t get_num_assets() const {
    return num_assets_;
  }
  
  size_t get_num_dates() const {
    std::shared_lock lock(map_mutex_);
    return date_map_.size();
  }
  
  // ===== Link Management (L0 features store L1/L2 time indices) =====
  // Write link to L1/L2 for a specific L0 time and asset
  // link_value: L1 or L2 time index (stored as _Float16)
  void write_link(const std::string& date, size_t l0_t, size_t asset_idx, 
                  size_t link_feature_offset, _Float16 link_value) {
    auto* day = get_day_data(date);
    if (!day) {
      std::cerr << "FATAL: Failed to write link for " << date << "\n";
      std::exit(1);
    }
    const size_t F0 = FIELDS_PER_LEVEL[0];
    const size_t A = num_assets_;
    day->data[0][l0_t * F0 * A + link_feature_offset * A + asset_idx] = link_value;
  }
  
  // ===== CS Worker Interface - Mark date complete =====
  void mark_date_complete(const std::string& date) {
    std::shared_lock lock(map_mutex_);
    auto it = date_map_.find(date);
    if (it != date_map_.end()) {
      auto* day = it->second;
      day->cs_done.store(true, std::memory_order_release);
    }
  }
  
  // ===== Export Interface =====
  void set_output_dir(const std::string& dir) {
    output_dir_ = dir;
  }
  
  // Flush all remaining tensors (called at end)
  void flush_all() {
    std::unique_lock lock(map_mutex_);
    size_t flushed_count = 0;
    for (size_t i = 0; i < pool_size_; ++i) {
      if (tensor_pool_[i]->in_use) {
        flush_tensor(tensor_pool_[i]);
        ++flushed_count;
      }
    }
    date_map_.clear();
    std::cout << "Flushed total " << flushed_count << " tensors to disk" << std::endl;
  }
};

// ============================================================================
// DATA ACCESS MACROS
// ============================================================================
// Note: All macros work with feature_storage_t (_Float16)
// Automatic conversion between float and _Float16 (like float <-> double)

// TS worker: write features for asset a at time t (src is feature_storage_t*)
#define TS_WRITE_FEATURES(store, date, level_idx, t, a, f_start, f_end, src) \
  do { \
    feature_storage_t* base = (store)->get_data_ptr(date, level_idx); \
    const size_t F = (store)->get_F(level_idx); \
    const size_t A = (store)->get_A(); \
    const size_t base_offset = (t) * F * A + (a); \
    for (size_t f = (f_start); f < (f_end); ++f) { \
      base[base_offset + f * A] = (src)[f]; \
    } \
  } while(0)

// CS worker: read all assets for feature f at time t (returns feature_storage_t*)
#define CS_READ_ALL_ASSETS(store, date, level_idx, t, f) \
  ((store)->get_data_ptr(date, level_idx) + (t) * (store)->get_F(level_idx) * (store)->get_A() + (f) * (store)->get_A())

// CS worker: write all assets for feature f at time t (src is feature_storage_t*)
#define CS_WRITE_ALL_ASSETS(store, date, level_idx, t, f, src, count) \
  std::memcpy((store)->get_data_ptr(date, level_idx) + (t) * (store)->get_F(level_idx) * (store)->get_A() + (f) * (store)->get_A(), \
              (src), (count) * sizeof(feature_storage_t))

// Read single value (returns feature_storage_t)
#define READ_FEATURE(store, date, level_idx, t, f, a) \
  ((store)->get_data_ptr(date, level_idx)[(t) * (store)->get_F(level_idx) * (store)->get_A() + (f) * (store)->get_A() + (a)])

// Write single value (value is feature_storage_t)
#define WRITE_FEATURE(store, date, level_idx, t, f, a, value) \
  do { (store)->get_data_ptr(date, level_idx)[(t) * (store)->get_F(level_idx) * (store)->get_A() + (f) * (store)->get_A() + (a)] = (value); } while(0)

// TS worker: write features with sync (no parent linkage, link features managed separately)
#define TS_WRITE_FEATURES_WITH_SYNC(store, date, level_idx, t, a, f_start, f_end, src, \
                                    sys_done_idx, sys_valid_idx, sys_ts_idx, is_valid) \
  do { \
    TS_WRITE_FEATURES(store, date, level_idx, t, a, f_start, f_end, src); \
    WRITE_FEATURE(store, date, level_idx, t, sys_done_idx, a, 1.0f); \
    WRITE_FEATURE(store, date, level_idx, t, sys_valid_idx, a, (is_valid) ? 1.0f : 0.0f); \
    WRITE_FEATURE(store, date, level_idx, t, sys_ts_idx, a, \
      static_cast<float>(std::chrono::duration_cast<std::chrono::milliseconds>( \
        std::chrono::system_clock::now().time_since_epoch()).count() & 0xFFFF)); \
  } while(0)

// Write link feature (L0 only): map L0 time to L1/L2 time
// link_feature_offset: L0_FieldOffset::_link_to_L1 or _link_to_L2
// link_value: L1 or L2 time index (stored as _Float16, auto-converted from size_t)
#define WRITE_LINK_FEATURE(store, date, l0_t, asset_idx, link_feature_offset, link_value) \
  do { \
    (store)->write_link(date, l0_t, asset_idx, link_feature_offset, static_cast<_Float16>(link_value)); \
  } while(0)

