#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <vector>

// 控制扩容时是否打印警告（取消注释以启用）
// #define MEMPOOL_WARN_ON_EXPANSION

#ifdef MEMPOOL_WARN_ON_EXPANSION
#include "misc/logging.hpp"
#include <sstream>
#endif

/*
═══════════════════════════════════════════════════════════════════════════════
高性能内存管理系统（分块扩容版）
═══════════════════════════════════════════════════════════════════════════════

架构设计：
  底层：BumpPool<T>, BitmapPool<T>  - 分块内存分配器（自动扩容）
  上层：HashMap<K, V, Pool>          - 通用字典（Pool 可配置）
  便利：BumpDict<K,V>, BitmapDict<K,V> - 类型别名

性能特点：
  BumpPool:   3-6 cycles 分配，不支持回收，自动扩容
  BitmapPool: 3-7 cycles 分配，3-7 cycles 回收，自动扩容
  HashMap:    + 5-15 cycles 哈希开销
  扩容机制：  每 ~1MB 块扩容一次（位运算优化），指针永不失效

内存布局：
  - 分块存储：每块 2^N 个对象（根据 sizeof(T) 自动计算，接近 1MB）
  - 块内连续：cache 友好
  - 块间独立：扩容时旧指针不受影响

使用指南：
  - 只增不删的数据 → BumpPool / BumpDict
  - 频繁增删的数据 → BitmapPool / BitmapDict
  - 调试扩容：取消注释 MEMPOOL_WARN_ON_EXPANSION 查看扩容日志
═══════════════════════════════════════════════════════════════════════════════
*/

namespace MemPool {

// ═════════════════════════════════════════════════════════════════════════════
//  Configuration
// ═════════════════════════════════════════════════════════════════════════════

namespace Config {
static constexpr size_t CACHE_LINE_SIZE = 64;
static constexpr size_t DEFAULT_POOL_SIZE = 10000;
static constexpr size_t BITS_PER_WORD = 64; // uint64_t

// 分块配置：根据对象大小自动选择块大小（接近 1MB）
template <typename T>
struct ChunkConfig {
  // 根据对象大小选择合适的 shift（块大小 = 2^shift 个对象）
  static constexpr size_t CHUNK_SHIFT =
      sizeof(T) <= 16 ? 16 : // 16B × 65536 = 1MB
          sizeof(T) <= 32 ? 15
                          : // 32B × 32768 = 1MB
          sizeof(T) <= 64 ? 14
                          : // 64B × 16384 = 1MB
          sizeof(T) <= 128 ? 13
                           : // 128B × 8192 = 1MB
          12;                // 256B+ × 4096 = 1MB+

  static constexpr size_t CHUNK_SIZE = size_t{1} << CHUNK_SHIFT;
  static constexpr size_t CHUNK_MASK = CHUNK_SIZE - 1;
};
} // namespace Config

// ═════════════════════════════════════════════════════════════════════════════
//  1. BumpPool - 极致速度，只增不删
// ═════════════════════════════════════════════════════════════════════════════

template <typename T>
class alignas(Config::CACHE_LINE_SIZE) BumpPool {
public:
  using value_type = T;
  using ChunkCfg = Config::ChunkConfig<T>;

  explicit BumpPool(size_t initial_capacity = Config::DEFAULT_POOL_SIZE)
      : used_(0) {
    // 计算初始需要的 chunk 数量（至少 1 个）
    initial_chunks_ = std::max(size_t{1}, (initial_capacity + ChunkCfg::CHUNK_SIZE - 1) / ChunkCfg::CHUNK_SIZE);

    // 预分配初始 chunks
    for (size_t i = 0; i < initial_chunks_; ++i) {
      allocate_new_chunk();
    }
  }

  ~BumpPool() {
    reset();
    for (T *chunk : chunks_) {
      operator delete[](chunk, std::align_val_t{Config::CACHE_LINE_SIZE});
    }
  }

  // 禁用拷贝/移动
  BumpPool(const BumpPool &) = delete;
  BumpPool &operator=(const BumpPool &) = delete;
  BumpPool(BumpPool &&) = delete;
  BumpPool &operator=(BumpPool &&) = delete;

  // ═══════════════════════════════════════════════════════════════════════
  // 核心 API
  // ═══════════════════════════════════════════════════════════════════════

  // 分配：3-6 cycles（位运算优化）
  [[gnu::hot, gnu::always_inline]] inline T *allocate() {
    size_t chunk_idx = used_ >> ChunkCfg::CHUNK_SHIFT;
    size_t local_idx = used_ & ChunkCfg::CHUNK_MASK;

    // 需要新 chunk 时扩容
    if (chunk_idx >= chunks_.size()) [[unlikely]] {
      allocate_new_chunk();
    }

    ++used_;
    return &chunks_[chunk_idx][local_idx];
  }

  // 回收：不支持（Bump 语义）
  [[gnu::always_inline]] inline void deallocate(T *ptr) {
    // Bump Pool 不回收单个对象
    (void)ptr;
  }

  // 构造对象
  template <typename... Args>
  [[gnu::hot, gnu::always_inline]] inline T *construct(Args &&...args) {
    T *ptr = allocate();
    new (ptr) T(std::forward<Args>(args)...);
    return ptr;
  }

  // 销毁对象（不回收内存）
  void destroy(T *ptr) {
    if (ptr && !std::is_trivially_destructible_v<T>) {
      ptr->~T();
    }
  }

  // 重置池（调用析构函数，重置分配指针）
  void reset(bool shrink_to_fit = false) {
    if constexpr (!std::is_trivially_destructible_v<T>) {
      for (size_t i = 0; i < used_; ++i) {
        size_t chunk_idx = i >> ChunkCfg::CHUNK_SHIFT;
        size_t local_idx = i & ChunkCfg::CHUNK_MASK;
        chunks_[chunk_idx][local_idx].~T();
      }
    }
    used_ = 0;

    // 可选：释放超过初始容量的 chunks
    if (shrink_to_fit && chunks_.size() > initial_chunks_) {
      for (size_t i = initial_chunks_; i < chunks_.size(); ++i) {
        operator delete[](chunks_[i], std::align_val_t{Config::CACHE_LINE_SIZE});
      }
      chunks_.resize(initial_chunks_);
    }
  }

  // ═══════════════════════════════════════════════════════════════════════
  // 查询 API
  // ═══════════════════════════════════════════════════════════════════════

  size_t size() const { return used_; }
  size_t capacity() const { return chunks_.size() * ChunkCfg::CHUNK_SIZE; }
  bool owns(const T *ptr) const {
    for (const T *chunk : chunks_) {
      if (ptr >= chunk && ptr < chunk + ChunkCfg::CHUNK_SIZE) {
        return true;
      }
    }
    return false;
  }
  double utilization() const {
    size_t cap = capacity();
    return cap > 0 ? static_cast<double>(used_) / cap : 0.0;
  }

private:
  std::vector<T *> chunks_;
  size_t used_;
  size_t initial_chunks_; // 初始预留的 chunk 数量

  void allocate_new_chunk() {
    size_t bytes = ChunkCfg::CHUNK_SIZE * sizeof(T);
    void *raw = operator new[](bytes, std::align_val_t{Config::CACHE_LINE_SIZE});
    chunks_.push_back(static_cast<T *>(raw));

#ifdef MEMPOOL_WARN_ON_EXPANSION
    // 记录扩容到日志（只在超过初始容量时）
    if (chunks_.size() > initial_chunks_ && Logger::is_initialized()) {
      std::ostringstream oss;
      oss << "[BumpPool] EXPANSION #" << chunks_.size()
          << " (" << (ChunkCfg::CHUNK_SIZE * sizeof(T) / 1024.0 / 1024.0) << "MB)";
      Logger::log_analyze(oss.str());
    }
#endif
  }
};

// ═════════════════════════════════════════════════════════════════════════════
//  2. BitmapPool - 支持回收，仍然极快
// ═════════════════════════════════════════════════════════════════════════════

template <typename T>
class alignas(Config::CACHE_LINE_SIZE) BitmapPool {
public:
  using value_type = T;
  using ChunkCfg = Config::ChunkConfig<T>;

  explicit BitmapPool(size_t initial_capacity = Config::DEFAULT_POOL_SIZE)
      : used_(0), allocated_(0) {
    // 计算初始需要的 chunk 数量（至少 1 个）
    initial_chunks_ = std::max(size_t{1}, (initial_capacity + ChunkCfg::CHUNK_SIZE - 1) / ChunkCfg::CHUNK_SIZE);

    // 预分配初始 chunks
    for (size_t i = 0; i < initial_chunks_; ++i) {
      allocate_new_chunk();
    }
  }

  ~BitmapPool() {
    reset();
    for (T *chunk : chunks_) {
      operator delete[](chunk, std::align_val_t{Config::CACHE_LINE_SIZE});
    }
  }

  BitmapPool(const BitmapPool &) = delete;
  BitmapPool &operator=(const BitmapPool &) = delete;
  BitmapPool(BitmapPool &&) = delete;
  BitmapPool &operator=(BitmapPool &&) = delete;

  // ═══════════════════════════════════════════════════════════════════════
  // 核心 API
  // ═══════════════════════════════════════════════════════════════════════

  // 分配：3-7 cycles（位运算优化 + 分块）
  [[gnu::hot]] T *allocate() {
    // 1. 尝试从已使用区域找空闲槽位（cache 热）
    size_t search_words = (used_ + Config::BITS_PER_WORD - 1) / Config::BITS_PER_WORD;

    for (size_t word_idx = 0; word_idx < search_words; ++word_idx) {
      uint64_t word = bitmap_[word_idx];
      if (word != 0) [[likely]] {
        // 硬件指令：找到第一个空闲 bit (1-2 cycles)
        size_t bit_idx = __builtin_ctzll(word);
        size_t slot_idx = word_idx * Config::BITS_PER_WORD + bit_idx;

        // 标记为占用
        bitmap_[word_idx] &= ~(1ULL << bit_idx);
        ++allocated_;

        // 分块寻址（位运算）
        size_t chunk_idx = slot_idx >> ChunkCfg::CHUNK_SHIFT;
        size_t local_idx = slot_idx & ChunkCfg::CHUNK_MASK;
        return &chunks_[chunk_idx][local_idx];
      }
    }

    // 2. 已使用区域都满了，扩展到新区域
    size_t slot_idx = used_++;
    size_t chunk_idx = slot_idx >> ChunkCfg::CHUNK_SHIFT;

    // 需要新 chunk 时扩容
    if (chunk_idx >= chunks_.size()) [[unlikely]] {
      allocate_new_chunk();
    }

    size_t word_idx = slot_idx / Config::BITS_PER_WORD;
    size_t bit_idx = slot_idx % Config::BITS_PER_WORD;

    bitmap_[word_idx] &= ~(1ULL << bit_idx);
    ++allocated_;

    size_t local_idx = slot_idx & ChunkCfg::CHUNK_MASK;
    return &chunks_[chunk_idx][local_idx];
  }

  // 回收：3-7 cycles × chunks_.size()
  // 注意：如果扩容后有很多 chunks，性能会下降！
  // 建议：频繁 deallocate 时，使用 reset(true) 收缩到初始容量
  [[gnu::hot, gnu::always_inline]] inline void deallocate(T *ptr) {
    if (!ptr) [[unlikely]]
      return;

    // 调用析构函数
    if constexpr (!std::is_trivially_destructible_v<T>) {
      ptr->~T();
    }

    // 找到所属的 chunk 并计算全局索引（O(chunks_.size())）
    size_t slot_idx = 0;
    for (size_t chunk_idx = 0; chunk_idx < chunks_.size(); ++chunk_idx) {
      T *chunk = chunks_[chunk_idx];
      if (ptr >= chunk && ptr < chunk + ChunkCfg::CHUNK_SIZE) {
        slot_idx = (chunk_idx << ChunkCfg::CHUNK_SHIFT) + (ptr - chunk);
        break;
      }
    }

    size_t word_idx = slot_idx / Config::BITS_PER_WORD;
    size_t bit_idx = slot_idx % Config::BITS_PER_WORD;

    // 标记为空闲 (1 cycle)
    bitmap_[word_idx] |= (1ULL << bit_idx);
    --allocated_;
  }

  // 构造对象
  template <typename... Args>
  [[gnu::hot]] T *construct(Args &&...args) {
    T *ptr = allocate();
    new (ptr) T(std::forward<Args>(args)...);
    return ptr;
  }

  // 销毁对象（回收内存）
  void destroy(T *ptr) {
    deallocate(ptr);
  }

  // 重置池
  void reset(bool shrink_to_fit = false) {
    if constexpr (!std::is_trivially_destructible_v<T>) {
      for (size_t i = 0; i < used_; ++i) {
        size_t word_idx = i / Config::BITS_PER_WORD;
        size_t bit_idx = i % Config::BITS_PER_WORD;
        // 只析构被占用的对象
        if ((bitmap_[word_idx] & (1ULL << bit_idx)) == 0) {
          size_t chunk_idx = i >> ChunkCfg::CHUNK_SHIFT;
          size_t local_idx = i & ChunkCfg::CHUNK_MASK;
          chunks_[chunk_idx][local_idx].~T();
        }
      }
    }

    // 重置 bitmap（全部标记为空闲）
    std::fill(bitmap_.begin(), bitmap_.end(), ~0ULL);
    used_ = 0;
    allocated_ = 0;

    // 可选：释放超过初始容量的 chunks（减少 deallocate 遍历开销）
    if (shrink_to_fit && chunks_.size() > initial_chunks_) {
      for (size_t i = initial_chunks_; i < chunks_.size(); ++i) {
        operator delete[](chunks_[i], std::align_val_t{Config::CACHE_LINE_SIZE});
      }
      chunks_.resize(initial_chunks_);

      // 同步缩减 bitmap
      size_t initial_bitmap_words = initial_chunks_ * ChunkCfg::CHUNK_SIZE / Config::BITS_PER_WORD;
      bitmap_.resize(initial_bitmap_words);
      std::fill(bitmap_.begin(), bitmap_.end(), ~0ULL);
    }
  }

  // ═══════════════════════════════════════════════════════════════════════
  // 查询 API
  // ═══════════════════════════════════════════════════════════════════════

  size_t size() const { return allocated_; }
  size_t capacity() const { return chunks_.size() * ChunkCfg::CHUNK_SIZE; }
  bool owns(const T *ptr) const {
    for (const T *chunk : chunks_) {
      if (ptr >= chunk && ptr < chunk + ChunkCfg::CHUNK_SIZE) {
        return true;
      }
    }
    return false;
  }
  double utilization() const {
    return used_ > 0 ? static_cast<double>(allocated_) / used_ : 1.0;
  }

private:
  std::vector<T *> chunks_;
  std::vector<uint64_t> bitmap_;
  size_t used_;           // 曾经使用到的最大索引
  size_t allocated_;      // 当前存活对象数
  size_t initial_chunks_; // 初始预留的 chunk 数量

  void allocate_new_chunk() {
    size_t bytes = ChunkCfg::CHUNK_SIZE * sizeof(T);
    void *raw = operator new[](bytes, std::align_val_t{Config::CACHE_LINE_SIZE});
    chunks_.push_back(static_cast<T *>(raw));

    // 扩展 bitmap（一个 chunk 需要的 bitmap words）
    size_t new_bitmap_words = ChunkCfg::CHUNK_SIZE / Config::BITS_PER_WORD;
    bitmap_.insert(bitmap_.end(), new_bitmap_words, ~0ULL);

#ifdef MEMPOOL_WARN_ON_EXPANSION
    // 记录扩容到日志（只在超过初始容量时）
    if (chunks_.size() > initial_chunks_ && Logger::is_initialized()) {
      std::ostringstream oss;
      oss << "[BitmapPool] EXPANSION #" << chunks_.size()
          << " (" << (ChunkCfg::CHUNK_SIZE * sizeof(T) / 1024.0 / 1024.0) << "MB)";
      Logger::log_analyze(oss.str());
    }
#endif
  }
};

// ═════════════════════════════════════════════════════════════════════════════
//  3. HashMap - 通用字典（Pool 可配置）
// ═════════════════════════════════════════════════════════════════════════════

template <typename Key, typename Value, template <typename> class Pool = BitmapPool, typename Hash = std::hash<Key>>
class HashMap {
private:
  // Entry 节点
  struct Entry {
    Key key;
    Value value;
    Entry *next;

    Entry(const Key &k, const Value &v) : key(k), value(v), next(nullptr) {}
    Entry(Key &&k, Value &&v) : key(std::move(k)), value(std::move(v)), next(nullptr) {}
    ~Entry() = default; // 显式声明为 trivial destructor
  };

  Pool<Entry> entry_pool_;
  std::vector<Entry *> buckets_;
  Hash hasher_;
  size_t size_;
  size_t bucket_count_;

public:
  // ═══════════════════════════════════════════════════════════════════════
  // 构造函数
  // ═══════════════════════════════════════════════════════════════════════

  explicit HashMap(size_t expected_size = Config::DEFAULT_POOL_SIZE)
      : entry_pool_(expected_size), size_(0) {
    // 桶数量为预期元素数的 1.5 倍（load factor ~0.67）
    bucket_count_ = std::max(size_t{16}, expected_size * 3 / 2);
    buckets_.resize(bucket_count_, nullptr);
  }

  ~HashMap() { clear(); }

  HashMap(const HashMap &) = delete;
  HashMap &operator=(const HashMap &) = delete;
  HashMap(HashMap &&) = delete;
  HashMap &operator=(HashMap &&) = delete;

  // ═══════════════════════════════════════════════════════════════════════
  // 核心 API
  // ═══════════════════════════════════════════════════════════════════════

  // 查找：5-15 cycles
  [[gnu::hot]] Value *find(const Key &key) {
    size_t bucket_idx = hasher_(key) % bucket_count_;
    Entry *entry = buckets_[bucket_idx];

    while (entry) {
      if (entry->key == key) {
        return &entry->value;
      }
      entry = entry->next;
    }

    return nullptr;
  }

  // 查找（const 版本）
  [[gnu::hot]] const Value *find(const Key &key) const {
    return const_cast<HashMap *>(this)->find(key);
  }

  // 插入/更新：7-20 cycles
  [[gnu::hot]] bool insert(const Key &key, const Value &value) {
    size_t bucket_idx = hasher_(key) % bucket_count_;
    Entry *entry = buckets_[bucket_idx];

    // 检查是否已存在
    while (entry) {
      if (entry->key == key) {
        entry->value = value; // 更新
        return true;
      }
      entry = entry->next;
    }

    // 创建新节点（自动扩容）
    Entry *new_entry = entry_pool_.construct(key, value);

    // 头插法
    new_entry->next = buckets_[bucket_idx];
    buckets_[bucket_idx] = new_entry;
    ++size_;
    return true;
  }

  // try_emplace 语义（不存在才插入）
  [[gnu::hot]] std::pair<Value *, bool> try_emplace(const Key &key, const Value &value) {
    size_t bucket_idx = hasher_(key) % bucket_count_;
    Entry *entry = buckets_[bucket_idx];

    // 检查是否已存在
    while (entry) {
      if (entry->key == key) {
        return {&entry->value, false}; // 已存在
      }
      entry = entry->next;
    }

    // 创建新节点（自动扩容）
    Entry *new_entry = entry_pool_.construct(key, value);

    new_entry->next = buckets_[bucket_idx];
    buckets_[bucket_idx] = new_entry;
    ++size_;
    return {&new_entry->value, true};
  }

  // 删除：7-20 cycles
  [[gnu::hot]] bool erase(const Key &key) {
    size_t bucket_idx = hasher_(key) % bucket_count_;
    Entry **prev_ptr = &buckets_[bucket_idx];
    Entry *entry = *prev_ptr;

    while (entry) {
      if (entry->key == key) {
        // 从链表中移除
        *prev_ptr = entry->next;

        // 回收内存（只有 BitmapPool 才真正回收）
        entry_pool_.deallocate(entry);

        --size_;
        return true;
      }
      prev_ptr = &entry->next;
      entry = entry->next;
    }

    return false;
  }

  // 清空
  void clear() {
    std::fill(buckets_.begin(), buckets_.end(), nullptr);
    entry_pool_.reset();
    size_ = 0;
  }

  // ═══════════════════════════════════════════════════════════════════════
  // 查询 API
  // ═══════════════════════════════════════════════════════════════════════

  size_t size() const { return size_; }
  bool empty() const { return size_ == 0; }

  // 遍历（for debug）
  template <typename Func>
  void for_each(Func &&func) const {
    for (Entry *head : buckets_) {
      Entry *entry = head;
      while (entry) {
        func(entry->key, entry->value);
        entry = entry->next;
      }
    }
  }

  // 统计信息
  struct Stats {
    size_t element_count;
    size_t bucket_count;
    size_t longest_chain;
    double avg_chain_length;
    double load_factor;
    double pool_utilization;
  };

  Stats get_stats() const {
    size_t longest_chain = 0;
    size_t total_chain_length = 0;
    size_t non_empty_buckets = 0;

    for (Entry *head : buckets_) {
      if (head) {
        size_t chain_length = 0;
        Entry *entry = head;
        while (entry) {
          ++chain_length;
          entry = entry->next;
        }
        longest_chain = std::max(longest_chain, chain_length);
        total_chain_length += chain_length;
        ++non_empty_buckets;
      }
    }

    return {size_,
            bucket_count_,
            longest_chain,
            non_empty_buckets > 0 ? static_cast<double>(total_chain_length) / non_empty_buckets : 0.0,
            static_cast<double>(size_) / bucket_count_,
            entry_pool_.utilization()};
  }
};

// ═════════════════════════════════════════════════════════════════════════════
//  4. 类型别名（便利层）
// ═════════════════════════════════════════════════════════════════════════════

// 只增不删的字典（极致速度）
template <typename Key, typename Value, typename Hash = std::hash<Key>>
using BumpDict = HashMap<Key, Value, BumpPool, Hash>;

// 支持删除的字典（高性能 + 内存回收）
template <typename Key, typename Value, typename Hash = std::hash<Key>>
using BitmapDict = HashMap<Key, Value, BitmapPool, Hash>;

} // namespace MemPool
