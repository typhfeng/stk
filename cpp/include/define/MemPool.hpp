#pragma once

#include <algorithm>
#include <cassert>
#include <iostream>
#include <type_traits>
#include <vector>

/*
通用内存池 + 多级哈希核心逻辑
(预期为单线程使用, cacheline之间的false sharing不会造成一致性协议的overhead)

1. 根据 Entry 大小和 CPU cache line(计算机体系结构) 确定每个桶的大小(每个桶多少entry)

2. 已知总元素数 N：
   - 计算一级桶数 B1 = N / 目标桶内平均元素数
   - 计算一级 load factor α1 = N / B1

3. 判断一级桶平均元素数：
   - 如果 > 目标桶内元素数 → 增加二级哈希
   - 否则保持单级哈希

4. 二级哈希：
   - 桶数 B2 = 一级桶平均元素数 / 二级目标 load factor α2
   - 判断二级桶平均元素数：
       * 如果 > 目标 → 继续增加下一级
       * 否则结束

5. 内存池管理：
   - 分配固定大小池
   - 池耗尽 → 按增长因子扩容

6. 查找流程：
   - hash1(key) → 一级桶
   - hash2(key) → 二级桶（可选）
   - 桶内线性查找
*/

namespace MemPool {

// ============================================================================
// Performance Configuration Parameters
// ============================================================================

namespace Config {
// CPU cache line optimization (hardware specific)
static constexpr size_t CACHE_LINE_SIZE = 64;

// Memory pool configuration (user defined)
static constexpr size_t DEFAULT_POOL_SIZE = 100000; // User defined pool size

// Hash algorithm parameters (manually tuned)
static constexpr float LEVEL1_LOAD_FACTOR = 0.75f; // First level load factor
static constexpr float LEVEL2_LOAD_FACTOR = 0.8f;  // Second level load factor
static constexpr size_t MAX_HASH_LEVELS = 3;       // Maximum hash levels

static constexpr size_t TARGET_BUCKET_ELEMENTS = CACHE_LINE_SIZE / sizeof(void *); // Pointer elements per cache line
static constexpr size_t MIN_BUCKET_COUNT = TARGET_BUCKET_ELEMENTS * 2;             // Minimum buckets per level
static constexpr size_t MIN_POOL_SIZE = CACHE_LINE_SIZE * 16;                      // Minimum pool size (16 cache lines)
// Growth factor for single continuous memory expansion
static constexpr size_t MEMORY_GROWTH_FACTOR = 2;
} // namespace Config

// ============================================================================
// High-Performance Memory Pool Template
// ============================================================================

template <typename T>
class alignas(Config::CACHE_LINE_SIZE) MemoryPool {
private:
  // Calculate optimal elements per cache line for type T
  static constexpr size_t elements_per_cache_line() {
    return std::max(size_t{1}, Config::CACHE_LINE_SIZE / sizeof(T));
  }

  // Raw memory allocation
  T *data_;
  size_t capacity_;
  size_t used_;

public:
  // ========================================================================
  // Constructor
  // ========================================================================

  explicit MemoryPool(size_t initial_size = Config::DEFAULT_POOL_SIZE)
      : capacity_(initial_size), used_(0) {
    // Allocate raw aligned memory - no object construction
    size_t bytes = capacity_ * sizeof(T);
    void *raw = operator new[](bytes, std::align_val_t{Config::CACHE_LINE_SIZE});
    data_ = static_cast<T *>(raw);
  }

  // ========================================================================
  // Core Allocation Interface
  // ========================================================================

  // Ultra-fast O(1) allocation
  [[gnu::hot]] T *allocate() {
    if (used_ >= capacity_) [[unlikely]] {
      std::cerr << std::endl;
      std::cerr << "[MemoryPool] Capacity exceeded! Initial capacity: " << capacity_ 
                << ", used: " << used_ << std::endl;
      exit(1);
    }

    return &data_[used_++];
  }

  // Batch allocation for improved cache efficiency
  [[gnu::hot]] std::vector<T *> allocate_batch(size_t count) {
    std::vector<T *> ptrs;
    ptrs.reserve(count);

    for (size_t i = 0; i < count; ++i) {
      T *ptr = allocate();
      if (!ptr)
        break;
      ptrs.push_back(ptr);
    }

    return ptrs;
  }

  // Construct object in-place with perfect forwarding
  template <typename... Args>
  [[gnu::hot]] T *construct(Args &&...args) {
    T *ptr = allocate();
    if (ptr) {
      new (ptr) T(std::forward<Args>(args)...);
    }
    return ptr;
  }

  // ========================================================================
  // Memory Management
  // ========================================================================

  // Reset pool for reuse (doesn't deallocate memory)
  void reset() {
    // Call destructors for constructed objects if needed
    if constexpr (!std::is_trivially_destructible_v<T>) {
      for (size_t i = 0; i < used_; ++i) {
        data_[i].~T();
      }
    }
    used_ = 0;
  }

  // Destructor to handle raw memory
  ~MemoryPool() {
    if (data_) {
      operator delete[](data_, std::align_val_t{Config::CACHE_LINE_SIZE});
    }
  }

  // Disable copy/move to prevent memory management issues
  MemoryPool(const MemoryPool &) = delete;
  MemoryPool &operator=(const MemoryPool &) = delete;
  MemoryPool(MemoryPool &&) = delete;
  MemoryPool &operator=(MemoryPool &&) = delete;

  // Clear all memory
  void clear() {
    reset();
    if (data_) {
      operator delete[](data_, std::align_val_t{Config::CACHE_LINE_SIZE});
    }
    capacity_ = Config::DEFAULT_POOL_SIZE;
    size_t bytes = capacity_ * sizeof(T);
    void *raw = operator new[](bytes, std::align_val_t{Config::CACHE_LINE_SIZE});
    data_ = static_cast<T *>(raw);
  }

  // ========================================================================
  // Performance Monitoring
  // ========================================================================

  struct MemoryStats {
    size_t total_allocated;
    size_t total_used;
    double utilization_rate;
    size_t memory_overhead_bytes;
  };

  MemoryStats get_memory_stats() const {
    const double utilization = capacity_ > 0 ? static_cast<double>(used_) / capacity_ : 0.0;
    const size_t overhead = sizeof(*this);

    return {
        capacity_,
        used_,
        utilization,
        overhead};
  }

  // Check if pointer belongs to this pool
  bool owns(const T *ptr) const {
    const T *start = data_;
    const T *end = start + capacity_;
    return ptr >= start && ptr < end;
  }

  // Get current usage
  size_t size() const { return used_; }
  size_t capacity() const { return capacity_; }

private:
  // ========================================================================
  // Internal Implementation
  // ========================================================================

  bool try_expand() {
    // CRITICAL FIX: Disable memory pool expansion to prevent pointer invalidation
    // The LOB stores raw pointers to pool-allocated objects. When the pool expands,
    // objects are moved to new memory locations, invalidating all stored pointers.
    // This causes double-free errors when the LOB tries to access the old pointers.
    // Solution: Return false to prevent expansion - pool should be pre-allocated large enough.
    return false;
  }
};

// ============================================================================
// Multi-Level Hash Table with Memory Pool Integration
// ============================================================================

template <typename Key, typename Value, typename Hash = std::hash<Key>>
class HighPerformanceHashMap {
private:
  // Entry stored in memory pool
  struct alignas(Config::CACHE_LINE_SIZE) Entry {
    Key key;
    Value value;
    Entry *next; // For collision handling

    template <typename K, typename V>
    Entry(K &&k, V &&v) : key(std::forward<K>(k)), value(std::forward<V>(v)), next(nullptr) {}
  };

  // Hash level configuration
  struct HashLevel {
    size_t bucket_count;
    size_t hash_shift;
    std::vector<Entry *> buckets;

    explicit HashLevel(size_t bucket_count_param) : bucket_count(bucket_count_param), hash_shift(0) {
      buckets.resize(bucket_count, nullptr);
      // Calculate hash shift for this level
      hash_shift = 64 - __builtin_clzll(bucket_count - 1);
    }
  };

  std::vector<HashLevel> levels_;
  MemoryPool<Entry> entry_pool_;
  Hash hasher_;
  size_t size_ = 0;

public:
  // ========================================================================
  // Constructor
  // ========================================================================

  explicit HighPerformanceHashMap(size_t expected_elements = Config::DEFAULT_POOL_SIZE)
      : entry_pool_(expected_elements) {
    initialize_hash_levels(expected_elements);
  }

  // ========================================================================
  // Core Operations
  // ========================================================================

  // Ultra-fast insertion with multi-level hashing
  [[gnu::hot]] bool insert(const Key &key, const Value &value) {
    const size_t hash_value = hasher_(key);

    // Multi-level hash lookup_
    for (auto &level : levels_) {
      const size_t bucket_idx = (hash_value >> level.hash_shift) % level.bucket_count;
      Entry *entry = level.buckets[bucket_idx];

      // Check for existing key in this bucket
      size_t chain_length = 0;
      while (entry && chain_length < Config::TARGET_BUCKET_ELEMENTS) {
        if (entry->key == key) {
          entry->value = value; // Update existing
          return true;
        }
        entry = entry->next;
        ++chain_length;
      }

      // If chain is short enough, insert here
      if (chain_length < Config::TARGET_BUCKET_ELEMENTS) {
        Entry *new_entry = entry_pool_.construct(key, value);
        if (!new_entry)
          return false;

        new_entry->next = level.buckets[bucket_idx];
        level.buckets[bucket_idx] = new_entry;
        ++size_;
        return true;
      }
    }

    // All levels exceeded target chain length, need to add level or fail
    return false;
  }

  // Ultra-fast lookup_ with multi-level hashing
  [[gnu::hot]] Value *find(const Key &key) {
    const size_t hash_value = hasher_(key);

    for (auto &level : levels_) {
      const size_t bucket_idx = (hash_value >> level.hash_shift) % level.bucket_count;
      Entry *entry = level.buckets[bucket_idx];

      while (entry) {
        if (entry->key == key) {
          return &entry->value;
        }
        entry = entry->next;
      }
    }

    return nullptr;
  }

  // ========================================================================
  // Management Interface
  // ========================================================================

  size_t size() const { return size_; }

  void clear() {
    for (auto &level : levels_) {
      std::fill(level.buckets.begin(), level.buckets.end(), nullptr);
    }
    entry_pool_.reset();
    size_ = 0;
  }

  // Performance statistics
  struct HashStats {
    size_t total_elements;
    size_t hash_levels;
    size_t total_buckets;
    double average_chain_length;
    MemoryPool<Entry>::MemoryStats pool_stats;
  };

  HashStats get_hash_stats() const {
    size_t total_buckets = 0;
    size_t total_chains = 0;

    for (const auto &level : levels_) {
      total_buckets += level.bucket_count;
      for (Entry *entry : level.buckets) {
        if (entry) {
          size_t chain_len = 0;
          while (entry) {
            ++chain_len;
            entry = entry->next;
          }
          total_chains += chain_len;
        }
      }
    }

    const double avg_chain_length = size_ > 0 ? static_cast<double>(total_chains) / size_ : 0.0;

    return {
        size_,
        levels_.size(),
        total_buckets,
        avg_chain_length,
        entry_pool_.get_memory_stats()};
  }

private:
  // ========================================================================
  // Internal Implementation
  // ========================================================================

  void initialize_hash_levels(size_t expected_elements) {
    size_t remaining_elements = expected_elements;

    for (size_t level = 0; level < Config::MAX_HASH_LEVELS && remaining_elements > 0; ++level) {
      const size_t bucket_count = std::max(
          Config::MIN_BUCKET_COUNT,
          remaining_elements / Config::TARGET_BUCKET_ELEMENTS);

      levels_.emplace_back(bucket_count);

      const float load_factor = (level == 0) ? Config::LEVEL1_LOAD_FACTOR : Config::LEVEL2_LOAD_FACTOR;
      remaining_elements = static_cast<size_t>(remaining_elements * (1.0f - load_factor));
    }

    // Ensure at least one level exists
    if (levels_.empty()) {
      levels_.emplace_back(Config::MIN_BUCKET_COUNT);
    }
  }
};

} // namespace MemPool
