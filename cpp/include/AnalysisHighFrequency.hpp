#pragma once

#include <algorithm>
#include <bitset>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <iomanip>
#include <iostream>
#include <memory_resource>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "codec/L2_DataType.hpp"
#include "define/MemPool.hpp"
// #include "features/backend/FeaturesConfig.hpp"
#include "math/sample/ResampleRunBar.hpp"

#define DEBUG_ORDER_PRINT 0
#define DEBUG_BOOK_PRINT 1
#define DEBUG_BOOK_BY_SECOND 1 // 0: by tick, 1: every 1 second, 2: every 2 seconds, ...
#define DEBUG_BOOK_AS_AMOUNT 1 // 0: 手, 1: 1万元, 2: 2万元, 3: 3万元, ...
#define DEBUG_ANOMALY_PRINT 1 // Print max unmatched order with creation timestamp
#define DEBUG_SINGLE_DAY 1

//========================================================================================
// ORDER TYPE PROCESSING COMPARISON TABLE
//========================================================================================
//
// ┌─────────────────────────┬──────────────────────────────┬──────────────────────────────┬──────────────────────────────┐
// │ Dimension               │ MAKER (Creator·Consumer)     │ TAKER (Counterparty·TOB)     │ CANCEL (Self·Shenzhen no px) │
// ├─────────────────────────┼──────────────────────────────┼──────────────────────────────┼──────────────────────────────┤
// │ target_id               │ Self ID                      │ Counterparty ID (reversed!)  │ Self ID                      │
// │ signed_volume           │ BID: +  ASK: -               │ BID: +  ASK: -               │ BID: -  ASK: +               │
// ├─────────────────────────┼──────────────────────────────┼──────────────────────────────┼──────────────────────────────┤
// │ order_lookup_ access    │ [Write] Create/Update Loc    │ [R/W] Update/Erase Loc       │ [R/W] Update/Erase Loc       │
// │ pending_deductions_     │ [Read+Del] Check & flush     │ [Write] If not found         │ [Write] If not found         │
// │ order_memory_pool_      │ [Alloc] Common               │ [-] Rare (out-of-order)      │ [-] Rare (out-of-order)      │
// │ level_storage_          │ [Create] May create Level    │ [-] Never                    │ [-] Never                    │
// │ visible_price_bitmap_   │ add/remove                   │ May remove                   │ May remove                   │
// │ best_bid_/best_ask_     │ [-]                          │ [YES] Update TOB             │ [-]                          │
// ├─────────────────────────┼──────────────────────────────┼──────────────────────────────┼──────────────────────────────┤
// │ Level::net_quantity     │ Increase (+/-)               │ Decrease                     │ Decrease                     │
// │ When order found        │ qty += signed_vol (merge)    │ qty += signed_vol (deduct C) │ qty += signed_vol (deduct S) │
// │ When order NOT found    │ Flush pending + Create Order │ Enqueue to pending           │ Enqueue to pending           │
// │ price=0 handling        │ Never happens                │ Use level->price             │ Common (SZ), use level->price│
// ├─────────────────────────┼──────────────────────────────┼──────────────────────────────┼──────────────────────────────┤
// │ Hash lookups            │ 2x (lookup + pending)        │ 1x (lookup)                  │ 1x (lookup)                  │
// │ Execution steps         │ 1→2→4→5→6                    │ 1→2→3→5→6→7                  │ 1→2→3→5→6                    │
// │ Typical probability     │ 95% create, 5% flush         │ 95% found, 5% pending        │ 85% found, 15% pending/no-px │
// ├─────────────────────────┼──────────────────────────────┼──────────────────────────────┼──────────────────────────────┤
// │ Core characteristics    │ Create order, consume pend,  │ Operate counterparty, prod   │ Operate self, produce pend,  │
// │                         │ allocate memory (creator)    │ pend, update TOB (reversed!) │ no price (Shenzhen special)  │
// └─────────────────────────┴──────────────────────────────┴──────────────────────────────┴──────────────────────────────┘
//
//========================================================================================

// 我们采用方案2: 抵扣模型 (simple & robust)
//
// 数据问题:
// 1. 同一ms内(甚至不同时刻间), order之间可能为乱序
// 2. order信息可能丢失
// 3. snapshot数据为异步 (对于沪深两市, 快照的时间点不确定, 并不是0ms对齐的, 这意味着快照只能作为大规模偏移后的模糊矫正)
// 4. 深交所2025年起撤单(U类型)无价格信息 (price=0)
//
//========================================================================================
// 方案-1(reorder-queue-based):
// - MAKER订单(挂单)可以立即执行当且仅当:
//   1. (假设已满足) 对应的maker_id 在lob中不存在
//   2. (需要检查)   目标订单价确认在当前lob本方价格中 (交易所会自动将对手方maker单拆分为taker+maker, 但是不保证行情顺序)
//   3. (假设已满足) timestamp >= last_processed_timestamp
//   4. (假设已满足) volume > 0
//
// - TAKER订单(吃单)可以立即执行当且仅当:
//   1. (需要检查)   对应的maker_id 在lob中存在
//   2. (需要检查)   目标订单价确认在当前lob对手价格中
//   3. (假设已满足) timestamp >= 对应maker order的timestamp
//   4. (假设已满足) 吃单数量 <= 当前lob的剩余挂单数量
//   5. (假设已满足) 没有pending的相同maker_id的cancel order
//
// - CANCEL订单(撤单)可以立即执行当且仅当:
//   1. (需要检查)   对应的maker_id 在lob中存在
//   2. (需要检查)   目标订单价确认在当前lob本方价格中
//   3. (假设已满足) timestamp >= 对应maker order的timestamp
//   4. (假设已满足) 撤单数量 >= 当前lob的剩余挂单数量 (保证成交已经完成)
//
//========================================================================================
// 方案-2(deduction-based with pending queue):
// - MAKER订单(挂单)执行:
//   1. 检查 pending_deductions_ 中是否有待抵扣的CANCEL/TAKER
//   2. 如果有, 先抵扣 pending 的量, 如果完全抵消则不创建订单
//   3. 创建对应PriceLevel和挂单, 或抵扣后仍有余量则创建部分挂单
//   4. 维护 PriceLevel 的总剩余订单量
// - TAKER订单(吃单)执行:
//   1. 大概率maker单已经存在, 正常吃单, 如果执行后挂单量为0, 则直接清除此订单
//   2. 因为乱序, 小概率maker单不存在, 此时将抵扣量放入 pending_deductions_, 等待MAKER到达后flush
//   3. 维护 PriceLevel 的总剩余订单量
// - CANCEL订单(撤单)执行:
//   1. 大概率maker单已经存在, 正常撤单, 如果执行后挂单量为0, 则直接清除此订单
//   2. 因为乱序或深交所price=0, maker单不存在, 此时将抵扣量放入 pending_deductions_, 等待MAKER到达后flush
//   3. 维护 PriceLevel 的总剩余订单量
// - 动态跟踪bid ask的top of book:
//   1. 因为有乱序问题, bid ask level交界处, 可能出现正负挂单量交替的混乱地带
//   2. 我们不尝试找到当前真正的TOB, 但是知道(不需要维护, 只是定义)TOB的上下界, 并认为真实TOB在上下界之间
//   3. ask TOB(上界)被定义为从高价到低价, 第一个卖方(负)挂单前的level(只是定义, 不需要实现)
//   4. bid TOB(下界)被定义为从低价到高价, 第一个买方(正)挂单前的level(只是定义, 不需要实现)
//   5. 真实ask TOB被定义为最近的主动成交的买方taker单对手价格(维护简单, 如果吃空对手maker, 则自动向远离mid price的price level顺延)
//   6. 真实bid TOB被定义为最近的主动成交的卖方taker单对手价格(维护简单, 如果吃空对手maker, 则自动向远离mid price的price level顺延)
//   7. 我们不假设ask TOB和bid TOB的高低关系, 这样易于维护, 并且99%的时间TOB都是准确的(乱序时间很短)

// 对于价格档位, 用位图 + 缓存向量的数据结构
// 对于订单, 用(内存连续)向量 + 哈希表的数据结构

//========================================================================================
// CONSTANTS AND TYPES
//========================================================================================

// LOB reconstruction engine configuration
static constexpr size_t EXPECTED_QUEUE_SIZE = 128;
static constexpr size_t CACHE_LINE_SIZE = 64;
static constexpr float HASH_LOAD_FACTOR = 0.4f; // Ultra-low load factor

// Core types
using Price = uint16_t;
using Quantity = int32_t; // Supports negative quantities for deduction model
using OrderId = uint32_t;

static constexpr uint32_t PRICE_RANGE_SIZE = static_cast<uint32_t>(UINT16_MAX) + 1; // 0-65535

// Ultra-compact order entry - cache-optimized
#if DEBUG_ANOMALY_PRINT
struct alignas(16) Order {
  Quantity qty;
  OrderId id;
  uint32_t timestamp; // Creation timestamp for debug only
  Order(Quantity q, OrderId i, uint32_t ts) : qty(q), id(i), timestamp(ts) {}
#else
struct alignas(8) Order {
  Quantity qty;
  OrderId id;
  Order(Quantity q, OrderId i) : qty(q), id(i) {}
#endif
  // Fast operations - always inline
  bool is_positive() const { return qty > 0; }
  bool is_depleted() const { return qty <= 0; }
  void subtract(Quantity amount) { qty -= amount; }
  void add(Quantity amount) { qty += amount; }
};

// Simple unified price level - no side field needed
struct alignas(CACHE_LINE_SIZE) Level {
  Price price;                    // Price level identifier
  Quantity net_quantity = 0;      // Cached sum of all quantities (can be negative)
  uint16_t order_count = 0;       // Fast size tracking
  uint16_t alignment_padding = 0; // Explicit padding for cache line alignment
  std::vector<Order *> orders;    // Pointers to orders at this price level

  explicit Level(Price p) : price(p) {
    orders.reserve(EXPECTED_QUEUE_SIZE);
  }

  // High-performance order management
  [[gnu::hot]] void add(Order *order) {
    orders.push_back(order);
    ++order_count;
    net_quantity += order->qty;
  }

  [[gnu::hot]] void remove(size_t order_index) {
    assert(order_index < orders.size());
    Order *removed_order = orders[order_index];

    // Update cached total before removal
    net_quantity -= removed_order->qty;

    // Swap-and-pop for O(1) removal
    if (order_index != orders.size() - 1) {
      orders[order_index] = orders.back();
    }
    orders.pop_back();
    --order_count;
  }

  // Fast level state queries
  bool empty() const { return order_count == 0; }
  bool has_visible_quantity() const { return net_quantity != 0; }

  // Recalculate cached total from scratch
  void refresh_total() {
    net_quantity = 0;
    for (const Order *current_order : orders) {
      net_quantity += current_order->qty;
    }
  }
};

// Order location tracking
struct Location {
  Level *level;
  size_t index;

  Location(Level *l, size_t i) : level(l), index(i) {}
};

//========================================================================================
// MAIN CLASS
//========================================================================================

class AnalysisHighFrequency {

public:
  // ========================================================================================
  // CONSTRUCTOR
  // ========================================================================================

  explicit AnalysisHighFrequency(size_t ORDER_SIZE = L2::DEFAULT_ENCODER_ORDER_SIZE)
      : order_lookup_(&order_lookup_memory_pool_), order_memory_pool_(ORDER_SIZE) {
    // Configure hash table for minimal collisions based on custom order count
    const size_t HASH_BUCKETS = (1ULL << static_cast<size_t>(std::ceil(std::log2(ORDER_SIZE / HASH_LOAD_FACTOR))));
    order_lookup_.reserve(ORDER_SIZE);
    order_lookup_.rehash(HASH_BUCKETS);
    order_lookup_.max_load_factor(HASH_LOAD_FACTOR);
  }

  // ========================================================================================
  // PUBLIC INTERFACE - MAIN ENTRY POINTS
  // ========================================================================================

  // Main order processing entry point - with zero price handling and pending order logic
  [[gnu::hot]] bool process(const L2::Order &order) {
    curr_tick_ = (order.hour << 24) | (order.minute << 16) | (order.second << 8) | order.millisecond;
    new_tick_ = curr_tick_ != prev_tick_;
    print_book(); // print before updating prev_tick (such that current snapshot is a valid sample)
    prev_tick_ = curr_tick_;

    // Process resampling
    if (resampler_.process(order)) {
      // std::cout << "[RESAMPLE] Bar formed at " << format_time() << std::endl;
    }

    bool result = update_lob(order);
    return result;
  };

  [[gnu::hot, gnu::always_inline]] bool update_lob(const L2::Order &order) {
    // 1. Get signed volume and target ID using simple lookup functions
    signed_volume_ = get_signed_volume(order);
    target_id_ = get_target_id(order);
    if (signed_volume_ == 0 || target_id_ == 0) [[unlikely]]
      return false;
    
#if DEBUG_ANOMALY_PRINT
    debug_.last_order = order;
#endif

    // 2. Perform lookup for the incoming order
    auto order_lookup_iterator = order_lookup_.find(target_id_);
    effective_price_ = order.price;

    // 3. Handle CANCEL/TAKER that arrive before MAKER (out-of-order)
    if (order.order_type == L2::OrderType::CANCEL || order.order_type == L2::OrderType::TAKER) {
      if (order_lookup_iterator == order_lookup_.end()) {
        // Order not found - this is out-of-order (CANCEL/TAKER arrived before MAKER)
        // Add to pending deductions queue, will be flushed when MAKER arrives
        pending_deductions_[target_id_] += signed_volume_;
        return true; // Deferred processing
      }
      
      // Order found - use counterparty MAKER's price from level
      effective_price_ = order_lookup_iterator->second.level->price;
    }

    // 4. Handle MAKER: check if there are pending deductions waiting
    if (order.order_type == L2::OrderType::MAKER) {
      auto pending_it = pending_deductions_.find(target_id_);
      if (pending_it != pending_deductions_.end()) {
        // Found pending deductions - flush them now
        signed_volume_ += pending_it->second;
        pending_deductions_.erase(pending_it);
        
        // If fully offset by pending deductions, don't create the order
        if (signed_volume_ == 0) {
          return true; // Fully offset, no order created
        }
      }
    }

    // 5. Process order normally (either non-zero price or resolved zero price)
    bool was_fully_consumed = apply_volume_change(target_id_, effective_price_, signed_volume_, order_lookup_iterator);

    // 6. Order-type specific post-processing
    if (order.order_type == L2::OrderType::TAKER) {
      update_tob_after_trade(order, was_fully_consumed, effective_price_);
    }
    return true;
  };

  // ========================================================================================
  // PUBLIC INTERFACE - DATA ACCESS
  // ========================================================================================

  // Get best bid price
  [[gnu::hot]] Price best_bid() const {
    update_tob();
    return best_bid_;
  }

  // Get best ask price
  [[gnu::hot]] Price best_ask() const {
    update_tob();
    return best_ask_;
  }

  // Book statistics - optimized for performance
  size_t total_orders() const { return order_lookup_.size(); }
  size_t total_levels() const { return price_levels_.size(); }
  size_t total_pending() const { return pending_deductions_.size(); }  // Number of pending out-of-order deductions

  // ========================================================================================
  // PUBLIC INTERFACE - BATCH PROCESSING
  // ========================================================================================

  // Simple batch processing
  template <typename OrderRange>
  [[gnu::hot]] size_t process_batch(const OrderRange &order_range) {
    size_t successfully_processed = 0;

    for (const auto &current_order : order_range) {
      if (process(current_order))
        ++successfully_processed;
    }

    return successfully_processed;
  }

  // ========================================================================================
  // PUBLIC INTERFACE - MARKET DEPTH ITERATION
  // ========================================================================================

  // Iterate through bid levels (price >= best_bid_) - optimized with cache
  template <typename Func>
  void for_each_visible_bid(Func &&callback_function, size_t max_levels = 5) const {
    update_tob();
    refresh_cache_if_dirty();

    if (best_bid_ == 0 || cached_visible_prices_.empty())
      return;

    // Find position of best_bid_ in sorted cache
    auto it = std::upper_bound(cached_visible_prices_.begin(), cached_visible_prices_.end(), best_bid_);

    size_t levels_processed = 0;
    // Iterate backwards from best_bid position for descending price order
    while (it != cached_visible_prices_.begin() && levels_processed < max_levels) {
      --it;
      Price price = *it;
      Level *level = find_level(price);
      if (level && level->has_visible_quantity()) {
        callback_function(price, level->net_quantity);
        ++levels_processed;
      }
    }
  }

  // Iterate through ask levels (price <= best_ask_) - optimized with cache
  template <typename Func>
  void for_each_visible_ask(Func &&callback_function, size_t max_levels = 5) const {
    update_tob();
    refresh_cache_if_dirty();

    if (best_ask_ == 0 || cached_visible_prices_.empty())
      return;

    // Find position of best_ask_ in sorted cache
    auto it = std::lower_bound(cached_visible_prices_.begin(), cached_visible_prices_.end(), best_ask_);

    size_t levels_processed = 0;
    // Iterate forward from best_ask position for ascending price order
    for (; it != cached_visible_prices_.end() && levels_processed < max_levels; ++it) {
      Price price = *it;
      Level *level = find_level(price);
      if (level && level->has_visible_quantity()) {
        callback_function(price, level->net_quantity);
        ++levels_processed;
      }
    }
  }

  // ========================================================================================
  // PUBLIC INTERFACE - UTILITIES
  // ========================================================================================

  // Complete reset
  void clear() {
    price_levels_.clear();
    level_storage_.clear();
    order_lookup_.clear();
    order_memory_pool_.reset();
    pending_deductions_.clear();
    visible_price_bitmap_.reset(); // O(1) clear all bits
    cached_visible_prices_.clear();
    cache_dirty_ = false;
    best_bid_ = 0;
    best_ask_ = 0;
    tob_dirty_ = true;
    prev_tick_ = 0;
    curr_tick_ = 0;
    new_tick_ = false;
    signed_volume_ = 0;
    target_id_ = 0;
    effective_price_ = 0;
#if DEBUG_ANOMALY_PRINT
    debug_.printed_anomalies.clear();
#endif

    if (DEBUG_SINGLE_DAY) {
      exit(1);
    }
  }

private:
  // ========================================================================================
  // CORE DATA STRUCTURES
  // ========================================================================================

  // Price level storage (stable memory addresses via deque)
  std::deque<Level> level_storage_;                 // All price levels (deque guarantees stable pointers)
  std::unordered_map<Price, Level *> price_levels_; // Price -> Level* mapping for O(1) lookup

  // Visible price tracking (prices with non-zero net_quantity)
  std::bitset<PRICE_RANGE_SIZE> visible_price_bitmap_; // Bitmap for O(1) visibility check
  mutable std::vector<Price> cached_visible_prices_;   // Sorted cache for fast iteration
  mutable bool cache_dirty_ = false;                   // Cache needs refresh flag

  // Top of book tracking
  mutable Price best_bid_ = 0;   // Best bid price (highest buy price with visible quantity)
  mutable Price best_ask_ = 0;   // Best ask price (lowest sell price with visible quantity)
  mutable bool tob_dirty_ = true; // TOB needs recalculation flag

  // Order tracking infrastructure
  std::pmr::unsynchronized_pool_resource order_lookup_memory_pool_;  // PMR memory pool for hash map
  std::pmr::unordered_map<OrderId, Location> order_lookup_;          // OrderId -> Location(Level*, index) for O(1) order lookup
  MemPool::MemoryPool<Order> order_memory_pool_;                     // Memory pool for Order object allocation

  // Out-of-order handling: pending deductions queue
  // When CANCEL/TAKER arrives before MAKER (especially Shenzhen's price=0 cancels):
  // - Store signed_volume in this map
  // - When MAKER arrives, flush by deducting accumulated volume
  // - Prevents anomaly accumulation in LOB
  std::unordered_map<OrderId, Quantity> pending_deductions_;  // OrderId -> accumulated signed_volume

  // Market timestamp tracking (hour|minute|second|millisecond)
  uint32_t prev_tick_ = 0;  // Previous tick timestamp
  uint32_t curr_tick_ = 0;  // Current tick timestamp
  bool new_tick_ = false;   // Flag: entered new tick

  // Hot path temporary variable cache to reduce allocation overhead
  mutable Quantity signed_volume_;
  mutable OrderId target_id_;
  mutable Price effective_price_;

  // Resampling components
  ResampleRunBar resampler_;

  // ========================================================================================
  // CORE LOB MANAGEMENT - LEVEL OPERATIONS
  // ========================================================================================

  // Simple price level lookup
  [[gnu::hot, gnu::always_inline]] inline Level *find_level(Price price) const {
    auto level_iterator = price_levels_.find(price);
    return (level_iterator != price_levels_.end()) ? level_iterator->second : nullptr;
  }

  // Create new price level
  [[gnu::hot, gnu::always_inline]] Level *create_level(Price price) {
    level_storage_.emplace_back(price);
    Level *new_level = &level_storage_.back();
    price_levels_[price] = new_level;
    return new_level;
  }

  // Remove empty level
  [[gnu::hot, gnu::always_inline]] void remove_level(Level *level_to_remove, bool erase_visible = true) {
    price_levels_.erase(level_to_remove->price);
    if (erase_visible) {
      remove_visible_price(level_to_remove->price);
    }
  }

  // ========================================================================================
  // CORE LOB MANAGEMENT - VISIBLE PRICE TRACKING
  // ========================================================================================

  // High-performance visible price cache management
  [[gnu::hot, gnu::always_inline]] inline void refresh_cache_if_dirty() const {
    if (!cache_dirty_)
      return;

    cached_visible_prices_.clear();
    for (uint32_t price_u32 = 0; price_u32 < PRICE_RANGE_SIZE; ++price_u32) {
      Price price = static_cast<Price>(price_u32);
      if (visible_price_bitmap_[price]) {
        cached_visible_prices_.push_back(price);
      }
    }
    cache_dirty_ = false;
  }

  // O(1) visible price addition
  [[gnu::hot, gnu::always_inline]] inline void add_visible_price(Price price) {
    if (!visible_price_bitmap_[price]) {
      visible_price_bitmap_.set(price);
      cache_dirty_ = true;
    }
  }

  // O(1) visible price removal
  [[gnu::hot, gnu::always_inline]] inline void remove_visible_price(Price price) {
    if (visible_price_bitmap_[price]) {
      visible_price_bitmap_.reset(price);
      cache_dirty_ = true;
    }
  }

  // Maintain visible price ordering after any level total change - now O(1)!
  [[gnu::hot, gnu::always_inline]] inline void update_visible_price(Level *level) {
    if (level->has_visible_quantity()) {
      add_visible_price(level->price);
    } else {
      remove_visible_price(level->price);
    }
  }

  // Find next ask level strictly above a given price with visible quantity - O(n) worst case, but typically very fast
  Price next_ask_above(Price from_price) const {
    for (uint32_t price_u32 = static_cast<uint32_t>(from_price) + 1; price_u32 < PRICE_RANGE_SIZE; ++price_u32) {
      Price price = static_cast<Price>(price_u32);
      if (visible_price_bitmap_[price]) {
        return price;
      }
    }
    return 0;
  }

  // Find next bid level strictly below a given price with visible quantity - O(n) worst case, but typically very fast
  Price next_bid_below(Price from_price) const {
    if (from_price == 0)
      return 0;
    for (Price price = from_price - 1; price != UINT16_MAX; --price) {
      if (visible_price_bitmap_[price]) {
        return price;
      }
    }
    return 0;
  }

  // Find minimum price with visible quantity - O(n) worst case, but cache-friendly
  Price min_visible_price() const {
    refresh_cache_if_dirty();
    return cached_visible_prices_.empty() ? 0 : cached_visible_prices_.front();
  }

  // Find maximum price with visible quantity - O(n) worst case, but cache-friendly
  Price max_visible_price() const {
    refresh_cache_if_dirty();
    return cached_visible_prices_.empty() ? 0 : cached_visible_prices_.back();
  }

  // ========================================================================================
  // CORE LOB MANAGEMENT - ORDER PROCESSING
  // ========================================================================================

  // Simplified unified volume calculation
  [[gnu::hot, gnu::always_inline]] inline Quantity get_signed_volume(const L2::Order &order) const {
    const bool is_bid = (order.order_dir == L2::OrderDirection::BID);

    switch (order.order_type) {
    case L2::OrderType::MAKER:
      return is_bid ? +order.volume : -order.volume;
    case L2::OrderType::CANCEL:
      return is_bid ? -order.volume : +order.volume;
    case L2::OrderType::TAKER:
      return is_bid ? +order.volume : -order.volume;
    default:
      return 0;
    }
  }

  // Simplified unified target ID lookup
  [[gnu::hot, gnu::always_inline]] inline OrderId get_target_id(const L2::Order &order) const {
    const bool is_bid = (order.order_dir == L2::OrderDirection::BID);

    switch (order.order_type) {
    case L2::OrderType::MAKER:
      return is_bid ? order.bid_order_id : order.ask_order_id;
    case L2::OrderType::CANCEL:
      return is_bid ? order.bid_order_id : order.ask_order_id;
    case L2::OrderType::TAKER:
      return is_bid ? order.ask_order_id : order.bid_order_id;
    default:
      return 0;
    }
  }

  // Unified order processing core logic - now accepts lookup iterator from caller
  [[gnu::hot, gnu::always_inline]] bool apply_volume_change(
      OrderId target_id,
      Price price,
      Quantity signed_volume,
      decltype(order_lookup_.find(target_id)) order_lookup_iterator) {

    if (order_lookup_iterator != order_lookup_.end()) {
      // Order exists - modify it
      Level *target_level = order_lookup_iterator->second.level;
      size_t order_index = order_lookup_iterator->second.index;
      Order *target_order = target_level->orders[order_index];

      // Apply signed volume change
      const Quantity old_qty = target_order->qty;
      const Quantity new_qty = old_qty + signed_volume;

      if (new_qty == 0) {
        // std::cout << "Order fully consumed: " << target_id << " at price: " << price << " with volume: " << signed_volume << std::endl;

        // Order fully consumed - remove completely
        target_level->remove(order_index);
        order_lookup_.erase(order_lookup_iterator);

        // Update lookup index for any moved order (swap-and-pop side effect)
        if (order_index < target_level->orders.size()) {
          auto moved_order_lookup = order_lookup_.find(target_level->orders[order_index]->id);
          if (moved_order_lookup != order_lookup_.end()) {
            moved_order_lookup->second.index = order_index;
          }
        }

        // Handle level cleanup and TOB updates
        if (target_level->empty()) {
          remove_level(target_level);
        } else {
          update_visible_price(target_level);
        }

        return true; // Fully consumed
      } else {
        // Partial update
        target_level->net_quantity += signed_volume;
        target_order->qty = new_qty;
        update_visible_price(target_level);
        
#if DEBUG_ANOMALY_PRINT
        check_anomaly(target_level);
#endif
        return false; // Partially consumed
      }

    } else {
      // Order doesn't exist - create placeholder
#if DEBUG_ANOMALY_PRINT
      Order *new_order = order_memory_pool_.construct(signed_volume, target_id, curr_tick_);
#else
      Order *new_order = order_memory_pool_.construct(signed_volume, target_id);
#endif
      if (!new_order)
        return false;

      Level *target_level = find_level(price);
      if (!target_level) {
        target_level = create_level(price);
      }

      size_t new_order_index = target_level->orders.size();
      target_level->add(new_order);
      order_lookup_.emplace(target_id, Location(target_level, new_order_index));
      update_visible_price(target_level);
      
#if DEBUG_ANOMALY_PRINT
      check_anomaly(target_level);
#endif
      return false; // New order created
    }
  }

  // ========================================================================================
  // CORE LOB MANAGEMENT - TOP OF BOOK (TOB)
  // ========================================================================================

  // Simple TOB update when needed (bootstrap only)
  void update_tob() const {
    if (!tob_dirty_)
      return;

    if (best_bid_ == 0 && best_ask_ == 0) {
      best_bid_ = max_visible_price();
      best_ask_ = min_visible_price();
    }

    tob_dirty_ = false;
  }

  // TOB update logic for taker orders
  [[gnu::hot, gnu::always_inline]] inline void update_tob_after_trade(const L2::Order &order, bool was_fully_consumed, Price trade_price) {
    const bool is_bid = (order.order_dir == L2::OrderDirection::BID);

    if (was_fully_consumed) {
      // Level was emptied - advance TOB
      if (is_bid) {
        // Buy taker consumed ask - advance ask to higher price
        best_ask_ = next_ask_above(trade_price);
      } else {
        // Sell taker consumed bid - advance bid to lower price
        best_bid_ = next_bid_below(trade_price);
      }
    } else {
      // Partial fill - TOB stays at this price
      if (is_bid) {
        best_ask_ = trade_price;
      } else {
        best_bid_ = trade_price;
      }
    }

    tob_dirty_ = false;
  }

  // ========================================================================================
  // HELPER UTILITIES - TIME FORMATTING
  // ========================================================================================

  // Convert packed timestamp to human-readable format
  std::string format_time() const {
    uint8_t hours = (curr_tick_ >> 24) & 0xFF;
    uint8_t minutes = (curr_tick_ >> 16) & 0xFF;
    uint8_t seconds = (curr_tick_ >> 8) & 0xFF;
    uint8_t milliseconds = curr_tick_ & 0xFF;

    std::ostringstream time_formatter;
    time_formatter << std::setfill('0')
                   << std::setw(2) << int(hours) << ":"
                   << std::setw(2) << int(minutes) << ":"
                   << std::setw(2) << int(seconds) << "."
                   << std::setw(3) << int(milliseconds * 10);
    return time_formatter.str();
  }

  // ========================================================================================
  // DEBUG INFRASTRUCTURE AND FUNCTIONS
  // ========================================================================================
#if DEBUG_ANOMALY_PRINT
  
  // Debug state storage
  struct DebugState {
    L2::Order last_order;
    std::unordered_set<Price> printed_anomalies;
  };
  mutable DebugState debug_;

  // Format timestamp as HH:MM:SS.mmm
  inline std::string format_timestamp(uint32_t ts) const {
    std::ostringstream oss;
    oss << std::setfill('0')
        << std::setw(2) << ((ts >> 24) & 0xFF) << ":"
        << std::setw(2) << ((ts >> 16) & 0xFF) << ":"
        << std::setw(2) << ((ts >> 8) & 0xFF) << "."
        << std::setw(3) << ((ts & 0xFF) * 10);
    return oss.str();
  }

  // Calculate order age in milliseconds
  inline int calc_age_ms(uint32_t order_ts) const {
    constexpr auto to_ms = [](uint32_t ts) constexpr -> int {
      return ((ts >> 24) & 0xFF) * 3600000 + ((ts >> 16) & 0xFF) * 60000 +
             ((ts >> 8) & 0xFF) * 1000 + (ts & 0xFF) * 10;
    };
    return to_ms(curr_tick_) - to_ms(order_ts);
  }

  // Check for sign anomaly in level (strict: only print far anomalies 5+ ticks from TOB)
  void check_anomaly(Level *level) const {
    update_tob();
    
    // Step 1: Distance filter - only check far levels (5+ ticks from TOB)
    constexpr Price MIN_DISTANCE = 5;
    const bool is_far_below_bid = (best_bid_ > 0 && level->price < best_bid_ - MIN_DISTANCE);
    const bool is_far_above_ask = (best_ask_ > 0 && level->price > best_ask_ + MIN_DISTANCE);
    if (!is_far_below_bid && !is_far_above_ask) return;
    
    // Step 2: Classify by distance to TOB (closer to bid = BID side, closer to ask = ASK side)
    const Price dist_to_bid = (level->price < best_bid_) ? (best_bid_ - level->price) : (level->price - best_bid_);
    const Price dist_to_ask = (level->price < best_ask_) ? (best_ask_ - level->price) : (level->price - best_ask_);
    const bool is_bid_side = (dist_to_bid < dist_to_ask);
    
    const bool has_anomaly = (is_bid_side && level->net_quantity < 0) || (!is_bid_side && level->net_quantity > 0);
    
    // Skip if no anomaly, small quantity, or already printed
    if (!has_anomaly || std::abs(level->net_quantity) <= 10) return;
    if (debug_.printed_anomalies.count(level->price)) return;
    
    // Step 3: Time filter - only print during continuous trading (09:30-15:00)
    const uint8_t hour = (curr_tick_ >> 24) & 0xFF;
    const uint8_t minute = (curr_tick_ >> 16) & 0xFF;
    if (!((hour == 9 && minute >= 30) || (hour >= 10 && hour < 15))) {
      return; // Anomaly exists but not printed (call auction period)
    }
    
    // Print and mark as printed
    debug_.printed_anomalies.insert(level->price);
    print_anomaly_level(level, is_bid_side);
  }
  
  // Print detailed anomaly information for a level
  void print_anomaly_level(Level *level, bool is_bid_side) const {
    // Collect all reverse-sign orders (unmatched orders)
    std::vector<Order *> anomaly_orders;
    anomaly_orders.reserve(level->order_count); // Pre-allocate
    
    for (Order *order : level->orders) {
      const bool is_reverse = (is_bid_side && order->qty < 0) || (!is_bid_side && order->qty > 0);
      if (is_reverse) anomaly_orders.push_back(order);
    }
    if (anomaly_orders.empty()) return;
    
    // Sort by absolute size (largest first)
    std::sort(anomaly_orders.begin(), anomaly_orders.end(), 
              [](const Order *a, const Order *b) { return std::abs(a->qty) > std::abs(b->qty); });
    
    // Print level summary header
    std::cout << "\033[35m[ANOMALY_LEVEL] " << format_time() 
              << " Level=" << level->price << " ExpectedSide=" << (is_bid_side ? "BID" : "ASK")
              << " NetQty=" << level->net_quantity << " TotalOrders=" << level->order_count
              << " UnmatchedOrders=" << anomaly_orders.size()
              << " | TOB: Bid=" << best_bid_ << " Ask=" << best_ask_ << "\033[0m\n";
    
    // Print all unmatched orders sorted by size
    for (size_t i = 0; i < anomaly_orders.size(); ++i) {
      const Order *order = anomaly_orders[i];
      std::cout << "\033[35m  [" << (i + 1) << "] ID=" << order->id 
                << " Qty=" << order->qty
                << " Created=" << format_timestamp(order->timestamp)
                << " Age=" << calc_age_ms(order->timestamp) << "ms\033[0m\n";
    }
  }

#endif // DEBUG_ANOMALY_PRINT

  // ========================================================================================
  // DEBUG UTILITIES - DISPLAY BOOK
  // ========================================================================================

  // Display current market depth
  void inline print_book() const {
#if DEBUG_BOOK_BY_SECOND == 0
    // Print by tick
    if (new_tick_ && DEBUG_BOOK_PRINT) {
#else
    // Print every N seconds (extract second timestamp by removing millisecond)
    const uint32_t curr_second = (curr_tick_ >> 8);
    const uint32_t prev_second = (prev_tick_ >> 8);
    const bool should_print = (curr_second / DEBUG_BOOK_BY_SECOND) != (prev_second / DEBUG_BOOK_BY_SECOND);
    if (should_print && DEBUG_BOOK_PRINT) {
#endif
      std::ostringstream book_output;
      book_output << "[" << format_time() << "] [" << std::setfill('0') << std::setw(3) << total_pending() << std::setfill(' ') << "] ";

      constexpr size_t MAX_DISPLAY_LEVELS = 10;
      constexpr size_t LEVEL_WIDTH = 12;

      update_tob();
      
#if DEBUG_ANOMALY_PRINT
      // At continuous trading start (09:30:00), scan all existing levels
      // This ensures anomalies that existed during call auction are detected
      static uint32_t last_check_second = 0;
      const uint32_t curr_second = (curr_tick_ >> 8);  // Remove milliseconds
      if (curr_second != last_check_second) {
        last_check_second = curr_second;
        const uint8_t hour = (curr_tick_ >> 24) & 0xFF;
        const uint8_t minute = (curr_tick_ >> 16) & 0xFF;
        const uint8_t second = (curr_tick_ >> 8) & 0xFF;
        
        if (hour == 9 && minute == 30 && second == 0) {
          debug_.printed_anomalies.clear();
          refresh_cache_if_dirty();
          for (const Price price : cached_visible_prices_) {
            Level *level = find_level(price);
            if (level && level->has_visible_quantity()) {
              check_anomaly(level);
            }
          }
        }
      }
#endif

      // Collect ask levels
      std::vector<std::pair<Price, Quantity>> ask_data;
      for_each_visible_ask([&](Price price, Quantity quantity) {
        ask_data.emplace_back(price, quantity);
      },
                           MAX_DISPLAY_LEVELS);

      // Reverse ask data for display
      std::reverse(ask_data.begin(), ask_data.end());

      // Display ask levels (left side, negate for display)
      book_output << "ASK: ";
      size_t ask_empty_spaces = MAX_DISPLAY_LEVELS - ask_data.size();
      for (size_t i = 0; i < ask_empty_spaces; ++i) {
        book_output << std::setw(LEVEL_WIDTH) << " ";
      }
      for (size_t i = 0; i < ask_data.size(); ++i) {
        const Price price = ask_data[i].first;
        const Quantity qty = ask_data[i].second;
        const Quantity display_qty = -qty; // Negate: normal negative -> positive, anomaly positive -> negative
        const bool is_anomaly = (qty > 0); // Ask should be negative, positive is anomaly
        
#if DEBUG_BOOK_AS_AMOUNT == 0
        // Display as 手 (lots)
        const std::string qty_str = std::to_string(display_qty);
#else
        // Display as N万元 (N * 10000 yuan): 手 * 100 * 股价 / (N * 10000)
        const double amount = std::abs(display_qty) * 100.0 * price / (DEBUG_BOOK_AS_AMOUNT * 10000.0);
        const std::string qty_str = (display_qty < 0 ? "-" : "") + std::to_string(static_cast<int>(amount + 0.5));
#endif
        const std::string level_str = std::to_string(price) + "x" + qty_str;
        
        if (is_anomaly) {
          book_output << "\033[31m" << std::setw(LEVEL_WIDTH) << std::left << level_str << "\033[0m";
        } else {
          book_output << std::setw(LEVEL_WIDTH) << std::left << level_str;
        }
      }

      book_output << "| BID: ";

      // Collect bid levels
      std::vector<std::pair<Price, Quantity>> bid_data;
      for_each_visible_bid([&](Price price, Quantity quantity) {
        bid_data.emplace_back(price, quantity);
      },
                           MAX_DISPLAY_LEVELS);

      // Display bid levels (right side, display as-is)
      for (size_t i = 0; i < MAX_DISPLAY_LEVELS; ++i) {
        if (i < bid_data.size()) {
          const Price price = bid_data[i].first;
          const Quantity qty = bid_data[i].second;
          const bool is_anomaly = (qty < 0); // Bid should be positive, negative is anomaly
          
#if DEBUG_BOOK_AS_AMOUNT == 0
          // Display as 手 (lots)
          const std::string qty_str = std::to_string(qty);
#else
          // Display as N万元 (N * 10000 yuan): 手 * 100 * 股价 / (N * 10000)
          const double amount = std::abs(qty) * 100.0 * price / (DEBUG_BOOK_AS_AMOUNT * 10000.0);
          const std::string qty_str = (qty < 0 ? "-" : "") + std::to_string(static_cast<int>(amount + 0.5));
#endif
          const std::string level_str = std::to_string(price) + "x" + qty_str;
          
          if (is_anomaly) {
            book_output << "\033[31m" << std::setw(LEVEL_WIDTH) << std::left << level_str << "\033[0m";
          } else {
            book_output << std::setw(LEVEL_WIDTH) << std::left << level_str;
          }
        } else {
          book_output << std::setw(LEVEL_WIDTH) << " ";
        }
      }

      // Count anomalies across ALL visible levels (not limited to displayed levels)
      size_t anomaly_count = 0;
      refresh_cache_if_dirty();
      for (const Price price : cached_visible_prices_) {
        const Level *level = find_level(price);
        if (!level || !level->has_visible_quantity()) continue;
        
        // Classify by distance to TOB (closer to bid = BID side, closer to ask = ASK side)
        const Price dist_to_bid = (price < best_bid_) ? (best_bid_ - price) : (price - best_bid_);
        const Price dist_to_ask = (price < best_ask_) ? (best_ask_ - price) : (price - best_ask_);
        const bool is_bid_side = (dist_to_bid < dist_to_ask);
        
        // Check for sign anomaly: BID should be positive, ASK should be negative
        if ((is_bid_side && level->net_quantity < 0) || (!is_bid_side && level->net_quantity > 0)) {
          ++anomaly_count;
        }
      }
      
      if (anomaly_count > 0) {
        book_output << " \033[31m[" << anomaly_count << " anomalies]\033[0m";
      }
      
      std::cout << book_output.str() << "\n";
    }
#if DEBUG_ORDER_PRINT
    char order_type_char = (order.order_type == L2::OrderType::MAKER) ? 'M' : (order.order_type == L2::OrderType::CANCEL) ? 'C'
                                                                                                                          : 'T';
    char order_dir_char = (order.order_dir == L2::OrderDirection::BID) ? 'B' : 'S';
    std::cout << "[" << format_time() << "] " << " ID: " << get_target_id(order) << " Type: " << order_type_char << " Direction: " << order_dir_char << " Price: " << order.price << " Volume: " << order.volume << std::endl;
#endif
  }
};
