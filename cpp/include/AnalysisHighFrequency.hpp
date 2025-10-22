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
#include <vector>

#include "codec/L2_DataType.hpp"
#include "define/MemPool.hpp"
// #include "features/backend/FeaturesConfig.hpp"
#include "math/sample/ResampleRunBar.hpp"

//========================================================================================
// PROFILING CONTROL MACROS
//========================================================================================

// Control inlining based on PROFILE_MODE (set by CMake)
#ifdef PROFILE_MODE
#define HOT_INLINE [[gnu::noinline]]   // Disable inlining for profiler visibility
#define HOT_NOINLINE [[gnu::noinline]] // Keep function boundary visible
#else
#define HOT_INLINE [[gnu::hot, gnu::always_inline]] inline // Aggressive inlining
#define HOT_NOINLINE [[gnu::hot]]                          // Hot but not inlined
#endif

//========================================================================================
// CONFIGURATION PARAMETERS
//========================================================================================

// Debug switches
#define DEBUG_ORDER_PRINT 0         // Print every order processing
#define DEBUG_ORDER_FLAGS_CREATE 0  // Print when order with special flags is created
#define DEBUG_ORDER_FLAGS_RESOLVE 0 // Print when order with special flags is resolved/migrated
#define DEBUG_BOOK_PRINT 0          // Print order book snapshot
#define DEBUG_BOOK_BY_SECOND 1      // 0: by tick, 1: every 1 second, 2: every 2 seconds, ...
#define DEBUG_BOOK_AS_AMOUNT 1      // 0: 手, 1: 1万元, 2: 2万元, 3: 3万元, ...
#define DEBUG_ANOMALY_PRINT 1       // Print max unmatched order with creation timestamp
#define DEBUG_SINGLE_DAY 0          // Exit after processing one day

// Auto-disable dependent switches based on logical relationships
#if DEBUG_BOOK_PRINT == 0
#undef DEBUG_BOOK_BY_SECOND
#define DEBUG_BOOK_BY_SECOND 0
#undef DEBUG_BOOK_AS_AMOUNT
#define DEBUG_BOOK_AS_AMOUNT 0
#undef DEBUG_ANOMALY_PRINT
#define DEBUG_ANOMALY_PRINT 0
#endif

#if DEBUG_ANOMALY_PRINT
#include <unordered_set>
#endif

// Trading session time points (China A-share market)
namespace TradingSession {
// Morning call auction (集合竞价)
constexpr uint8_t MORNING_CALL_AUCTION_START_HOUR = 9;
constexpr uint8_t MORNING_CALL_AUCTION_START_MINUTE = 15;
constexpr uint8_t MORNING_CALL_AUCTION_END_MINUTE = 25;

// Morning matching period (集合竞价撮合期)
constexpr uint8_t MORNING_MATCHING_START_MINUTE = 25;
constexpr uint8_t MORNING_MATCHING_END_MINUTE = 30;

// Continuous auction (连续竞价)
constexpr uint8_t CONTINUOUS_TRADING_START_HOUR = 9;
constexpr uint8_t CONTINUOUS_TRADING_START_MINUTE = 30;
constexpr uint8_t CONTINUOUS_TRADING_END_HOUR = 15;
constexpr uint8_t CONTINUOUS_TRADING_END_MINUTE = 0;

// Closing call auction (收盘集合竞价 - Shenzhen only)
constexpr uint8_t CLOSING_CALL_AUCTION_START_HOUR = 14;
constexpr uint8_t CLOSING_CALL_AUCTION_START_MINUTE = 57;
constexpr uint8_t CLOSING_CALL_AUCTION_END_HOUR = 15;
constexpr uint8_t CLOSING_CALL_AUCTION_END_MINUTE = 0;
} // namespace TradingSession

// LOB visualization parameters
namespace BookDisplay {
constexpr size_t MAX_DISPLAY_LEVELS = 10; // Number of price levels to display
constexpr size_t LEVEL_WIDTH = 12;        // Width for each price level display
} // namespace BookDisplay

// Anomaly detection parameters
namespace AnomalyDetection {
constexpr uint16_t MIN_DISTANCE_FROM_TOB = 5; // Minimum distance from TOB to check anomalies
}

//========================================================================================
// CORE ARCHITECTURE: ORDER-CENTRIC LOB WITH ADAPTIVE PLACEMENT
//========================================================================================
//
// 设计理念: 抵扣模型 + Order迁移机制
// --------------------------------------------------------------------------------
// - TAKER成交价是最终真相, MAKER挂价可能不准确
// - 所有Order都存在于某个Level中 (包括Level[0]特殊档位)
// - Order携带flags标记状态, 支持在Level间迁移
// - 通过order_lookup_全局索引实现O(1)定位和迁移
//
//========================================================================================
// CORE DATA STRUCTURES
//========================================================================================
//
// 1. order_lookup_: unordered_map<OrderId, Location>
//    - O(1)全局索引, 任何Order都能快速定位到(Level*, index)
//    - 支持Order在不同Level间迁移后更新Location
//
// 2. price_levels_: unordered_map<Price, Level*>
//    - 价格到Level的映射, 快速找到任意价格档位
//    - Level[0]是特殊档位, 存放price=0的订单
//
// 3. Level: 价格档位, 内部维护Order队列
//    - vector<Order*> orders: FIFO队列 (时间优先)
//    - Quantity net_quantity: 缓存的总量 (正=买压, 负=卖压)
//    - operations: add(), remove(), refresh_total()
//
// 4. Order: 订单对象 (在order_memory_pool_中分配)
//    - Quantity qty: 有符号数量 (正=买方增加/卖方减少, 负=买方减少/卖方增加)
//    - OrderId id: 订单唯一标识
//    - OrderFlags flags: 订单状态标记
//
// Order Flags (订单状态标记):
// --------------------------------------------------------------------------------
// enum class OrderFlags : uint8_t {
//   NORMAL,          // 正常订单 (价格方向都确定且匹配)
//   UNKNOWN,         // 未知类型 (信息不足, 暂时标记)
//   OUT_OF_ORDER,    // 乱序到达 (TAKER/CANCEL先于MAKER)
//   CALL_AUCTION,    // 集合竞价订单 (价格可能不是最终成交价)
//   SPECIAL_MAKER,   // 特殊挂单 (price=0的市价单等)
//   ZERO_PRICE,      // 零价格订单 (CANCEL等, price=0)
//   ANOMALY_MATCH    // 异常撮合 (连续竞价时价格/方向不匹配)
// };
//
// Level[0] 特殊档位:
// --------------------------------------------------------------------------------
// - 存放price=0的订单: SPECIAL_MAKER, ZERO_PRICE, UNKNOWN等
// - 数量极少(<0.1%总订单数), 通常<10个订单
// - O(n)遍历开销可忽略
// - 统一接口: Level[0]和其他Level使用相同的操作(add/remove/move)
//
//========================================================================================
// CORNER CASES SUMMARY
//========================================================================================
//
// 交易所逐笔数据存在多种corner cases (总计~5% orders):
//
// 1. OUT_OF_ORDER (乱序到达) - ~2-5%
//    - TAKER/CANCEL先于对应MAKER到达
//    - 预创建Order, MAKER到达后抵扣
//
// 2. CALL_AUCTION (集合竞价) - 9:15-9:30, 14:57-15:00
//    - MAKER挂价 ≠ 最终撮合价 (统一竞价撮合价)
//    - TAKER到达时按成交价迁移Order
//
// 3. SPECIAL_MAKER (特殊挂单) - ~1-2%
//    - 市价单('1'), 本方最优('U')等price=0的订单
//    - 存放于Level[0], TAKER到达时迁移到成交价
//
// 4. ZERO_PRICE_CANCEL (深交所零价格撤单) - ~5-10% of cancels
//    - 深交所撤单无价格信息(price=0)
//    - 存放于Level[0], MAKER到达时迁移到挂单价
//
// 5. ANOMALY_MATCH (异常撮合) - rare
//    - 连续竞价时, MAKER挂价 ≠ TAKER成交价 (非集合竞价)
//    - 可能是盘中算法单等特殊撮合规则
//    - 按TAKER成交价迁移Order, 标记flags=ANOMALY_MATCH
//
// 6. UNKNOWN (信息不足) - temporary
//    - 无法确定订单类型时的暂时状态
//    - 等待后续数据澄清
//
//========================================================================================
// ORDER PROCESSING FLOW (3 ORDER TYPES)
//========================================================================================
//
// ┌─────────────────────────────────────────────────────────────────────────────────┐
// │ MAKER FLOW (创建挂单)                                                            │
// └─────────────────────────────────────────────────────────────────────────────────┘
//
// 1. 确定初始放置位置和flags:
//    ┌─ price=0?           → Level[0],     flags=SPECIAL_MAKER
//    ├─ 集合竞价期?        → Level[price], flags=CALL_AUCTION
//    └─ 连续竞价正常       → Level[price], flags=NORMAL
//
// 2. 检查order_lookup_[id]:
//    ├─ 已存在 (OUT_OF_ORDER: TAKER/CANCEL先到)
//    │  ├─ Order在Level[X], MAKER要求Level[Y], X≠Y?
//    │  │  └─ move_order_by_id(id, Y)
//    │  └─ order_upsert(抵扣qty, 更新flags)
//    │
//    └─ 不存在 (正常情况)
//       └─ order_upsert(创建新Order, 设置flags)
//
// ┌─────────────────────────────────────────────────────────────────────────────────┐
// │ TAKER FLOW (成交)                                                               │
// └─────────────────────────────────────────────────────────────────────────────────┘
//
// 交易所撮合机制差异 (SSE分时段, SZSE全天双边):
// --------------------------------------------------------------------------------
// SSE: 集合竞价撮合期 (9:25-9:30, 14:57-15:00) 双边, 连续竞价 (9:30-14:57) 单边
// SZSE: 全天双边
//
// 1. 判断撮合类型 (条件: order.order_type == TAKER):
//    ├─ 双边撮合?  need_bilateral_matching && bid_id != 0 && ask_id != 0
//    │  └─ 执行 handle_bilateral_matching() → 返回
//    │
//    └─ 单边撮合 (默认路径)
//       └─ 继续下面流程
//
// 2. 单边撮合 - 获取target_id (被抵扣的maker):
//    ├─ bid_id != 0 && ask_id != 0?  → target_id = min(bid_id, ask_id)  (更早的maker)
//    └─ 只有一个ID != 0?             → target_id = 存在的那个ID
//
// 3. 通过order_lookup_查找target_id:
//    ├─ 找到Order (正常情况)
//    │  ├─ Order在Level[X], TAKER成交价=Y, X≠Y?
//    │  │  ├─ move_order_by_id(target_id, Y)
//    │  │  └─ 连续竞价 && 价格不匹配? → flags=ANOMALY_MATCH
//    │  │
//    │  ├─ order_upsert(抵扣qty)
//    │  └─ update_tob_after_trade(order, 是否完全消耗, 成交价)
//    │
//    └─ 未找到Order (OUT_OF_ORDER: TAKER先于MAKER到达)
//       └─ order_upsert(预创建Order, flags=OUT_OF_ORDER)
//
// 4. 处理self_order (仅市价单场景: !both_ids_present):
//    └─ 己方MAKER可能在Level[0] → 查找并迁移 → 抵扣
//
// ┌─────────────────────────────────────────────────────────────────────────────────┐
// │ CANCEL FLOW (撤单)                                                              │
// └─────────────────────────────────────────────────────────────────────────────────┘
//
// 1. 通过order_lookup_查找self_id:
//    ├─ 找到Order
//    │  ├─ Order在Level[0] && CANCEL有价格?
//    │  │  └─ move_order_by_id(id, cancel_price)
//    │  │
//    │  └─ order_upsert(抵扣qty, CANCEL的signed_volume为负)
//    │
//    └─ 未找到Order (OUT_OF_ORDER或ZERO_PRICE)
//       ├─ price=0? → Level[0], flags=ZERO_PRICE
//       └─ price≠0? → Level[price], flags=OUT_OF_ORDER
//
//========================================================================================
// SPECIAL HANDLING: CALL AUCTION (集合竞价)
//========================================================================================
//
// 9:15-9:25 集合竞价期 (in_call_auction_ = true, in_matching_period_ = false):
//   - MAKER: 创建到Level[报价], flags=CALL_AUCTION
//   - TAKER: 暂无 (集合竞价期不产生成交)
//
// 9:25-9:30 集合竞价撮合期 (in_call_auction_ = true, in_matching_period_ = true):
//   - MAKER: 继续创建到Level[报价], flags=CALL_AUCTION
//   - TAKER: 按统一撮合价成交
//     → SSE: 双边撮合 (handle_bilateral_matching)
//     → 找到对手MAKER, 若价格不匹配则迁移
//     → 抵扣qty, 更新flags=NORMAL
//
// 9:30:00 连续竞价开始 (in_call_auction_ = false, in_matching_period_ = false):
//   - flush_call_auction_flags(): 遍历所有Order, 清除flags=CALL_AUCTION
//   - 剩余MAKER按原挂单价继续挂牌 (已在Level[挂单价], 无需搬运)
//
// 14:57-15:00 收盘集合竞价 (in_call_auction_ = true, in_matching_period_ = true):
//   - SSE: 双边撮合 (handle_bilateral_matching), 与开盘集合竞价处理相同
//   - SZSE: 全天双边撮合, 收盘集合竞价与其他时段处理相同
//
//========================================================================================
// PERFORMANCE CHARACTERISTICS
//========================================================================================
//
// Hot Path (95%+ orders): 正常连续竞价, 价格匹配
// --------------------------------------------------------------------------------
// MAKER:  1x hash lookup (order_lookup_) + create Order     → O(1)
// TAKER:  1x hash lookup (order_lookup_) + deduct Order     → O(1)
// CANCEL: 1x hash lookup (order_lookup_) + deduct Order     → O(1)
//
// Cold Path (5%- orders): Corner cases, 需要迁移或特殊处理
// --------------------------------------------------------------------------------
// OUT_OF_ORDER:    1x hash + possible move                  → O(1)
// CALL_AUCTION:    1x hash + possible move                  → O(1)
// SPECIAL_MAKER:   1x hash + move from Level[0]             → O(1)
// ZERO_PRICE:      1x hash + move from Level[0]             → O(1)
// ANOMALY_MATCH:   1x hash + move + flag update             → O(1)
// Level[0] scan:   O(n) where n < 10 typically              → negligible
//
// Memory:
// --------------------------------------------------------------------------------
// - 所有Order统一存储于Levels, 无额外队列
// - order_lookup_提供O(1)全局索引
// - 内存占用: ~16 bytes/Order (compact design)
//
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

// Use ExchangeType from L2 namespace (defined in L2_DataType.hpp)
using ExchangeType = L2::ExchangeType;

// Order flags enumeration - track order state and corner cases
enum class OrderFlags : uint8_t {
  NORMAL,        // Normal order (price and direction match, fully confirmed)
  UNKNOWN,       // Unknown type (insufficient information, temporary state)
  OUT_OF_ORDER,  // Out-of-order arrival (TAKER/CANCEL arrived before MAKER)
  CALL_AUCTION,  // Call auction order (price may not be final trade price)
  SPECIAL_MAKER, // Special MAKER (price=0, market order, etc.)
  ZERO_PRICE,    // Zero-price order (CANCEL with price=0, Shenzhen exchange)
  ANOMALY_MATCH  // Anomaly match (continuous trading, price/direction mismatch)
};

// Ultra-compact order entry - cache-optimized with flags
#if DEBUG_ANOMALY_PRINT
struct alignas(16) Order {
  Quantity qty;
  OrderId id;
  uint32_t timestamp; // Creation timestamp for debug only
  OrderFlags flags;   // Order state/type flags
  uint8_t padding[3]; // Explicit padding for alignment

  Order(Quantity q, OrderId i, uint32_t ts, OrderFlags f = OrderFlags::NORMAL)
      : qty(q), id(i), timestamp(ts), flags(f) {}
#else
struct alignas(16) Order {
  Quantity qty;
  OrderId id;
  OrderFlags flags;   // Order state/type flags
  uint8_t padding[7]; // Explicit padding for alignment

  Order(Quantity q, OrderId i, OrderFlags f = OrderFlags::NORMAL)
      : qty(q), id(i), flags(f) {}
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
  HOT_INLINE void add(Order *order) {
    orders.push_back(order);
    ++order_count;
    net_quantity += order->qty;
  }

  HOT_INLINE void remove(size_t order_index) {
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
  HOT_INLINE bool empty() const { return order_count == 0; }
  HOT_INLINE bool has_visible_quantity() const { return net_quantity != 0; }

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

// Fast identity hash for OrderId (uint32_t) - avoids std::hash overhead
struct OrderIdHash {
  HOT_INLINE size_t operator()(OrderId id) const noexcept {
    // Direct identity hash - OrderIds are already well-distributed
    return static_cast<size_t>(id);
  }
};

//========================================================================================
// MAIN CLASS
//========================================================================================

class AnalysisHighFrequency {

public:
  //======================================================================================
  // CONSTRUCTOR & CONFIGURATION
  //======================================================================================

  explicit AnalysisHighFrequency(size_t ORDER_SIZE = L2::DEFAULT_ENCODER_ORDER_SIZE, ExchangeType exchange_type = ExchangeType::SSE)
      : order_lookup_(&order_lookup_memory_pool_), order_memory_pool_(ORDER_SIZE), exchange_type_(exchange_type) {
    // Configure hash table for minimal collisions based on custom order count
    const size_t HASH_BUCKETS = (1ULL << static_cast<size_t>(std::ceil(std::log2(ORDER_SIZE / HASH_LOAD_FACTOR))));
    order_lookup_.max_load_factor(HASH_LOAD_FACTOR);
    order_lookup_.rehash(HASH_BUCKETS);
    order_lookup_.reserve(ORDER_SIZE);

    // Pre-allocate price levels map to reduce rehashing
    price_levels_.reserve(1024); // Typical max price levels in a trading day
    price_levels_.max_load_factor(0.5f);
  }

  // Set exchange type (useful if not set in constructor)
  void set_exchange_type(ExchangeType exchange_type) {
    exchange_type_ = exchange_type;
  }

  //======================================================================================
  // PUBLIC API: Order Processing
  //======================================================================================

  // Process single order and update LOB state
  HOT_NOINLINE bool process(const L2::Order &order) {
    curr_tick_ = (order.hour << 24) | (order.minute << 16) | (order.second << 8) | order.millisecond;
    new_tick_ = curr_tick_ != prev_tick_;

    // Update session state on tick change
    if (new_tick_) [[unlikely]] {
      update_trading_session_state();
    }

    // Detect transition from matching period to continuous trading
    static bool was_in_matching_period = false;
    if (was_in_matching_period && !in_matching_period_ && !in_call_auction_) [[unlikely]] {
      flush_call_auction_flags();
    }
    was_in_matching_period = in_matching_period_;

#if DEBUG_BOOK_PRINT
    print_book(order);
#endif
    prev_tick_ = curr_tick_;

    // Process resampling
    if (resampler_.resample(order)) {
      // std::cout << "[RESAMPLE] Bar formed at " << format_time() << std::endl;
    }

    bool result = update_lob(order);
    return result;
  };

  // Handle bilateral matching (both bid and ask deducted)
  HOT_NOINLINE bool handle_bilateral_matching(const L2::Order &order) {
    const Price trade_price = order.price;
    const Quantity volume = order.volume;

    // Determine active side: smaller ID = maker (earlier), larger ID = taker (newer)
    const bool is_active_bid = (order.bid_order_id > order.ask_order_id);

    // Perform both lookups back-to-back for better cache locality
    auto bid_it = order_lookup_.find(order.bid_order_id);
    auto ask_it = order_lookup_.find(order.ask_order_id);

    bool maker_fully_consumed = false;
    
    // Get or create trade_price level once (shared by both sides)
    Level *trade_level = level_find(trade_price);

    // Process bid order (apply -volume) - hot path assumes order exists
    if (bid_it != order_lookup_.end()) [[likely]] {
      if (bid_it->second.level->price != trade_price) [[unlikely]] {
        order_move_to_price(bid_it, trade_price);
      }
      bool consumed = order_upsert(order.bid_order_id, trade_price, -volume, bid_it, OrderFlags::NORMAL, trade_level);
      maker_fully_consumed = !is_active_bid && consumed;
    }

    // Process ask order (apply +volume) - hot path assumes order exists
    if (ask_it != order_lookup_.end()) [[likely]] {
      if (ask_it->second.level->price != trade_price) [[unlikely]] {
        order_move_to_price(ask_it, trade_price);
      }
      bool consumed = order_upsert(order.ask_order_id, trade_price, +volume, ask_it, OrderFlags::NORMAL, trade_level);
      if (is_active_bid)
        maker_fully_consumed = consumed;
    }

    // Update TOB: only the maker side (consumed side) needs update
    actual_price_ = trade_price;
    update_tob_one_side(is_active_bid, maker_fully_consumed, trade_price);
    return true;
  }

  HOT_NOINLINE bool update_lob(const L2::Order &order) {
    // Check for bilateral matching (SSE matching period or SZSE all day)
    const bool need_bilateral_matching = (exchange_type_ == ExchangeType::SZSE) ||
                                         (exchange_type_ == ExchangeType::SSE && in_matching_period_);

    if (order.order_type == L2::OrderType::TAKER && need_bilateral_matching &&
        order.bid_order_id != 0 && order.ask_order_id != 0) [[unlikely]] {
      return handle_bilateral_matching(order);
    }

    // Extract operation parameters
    delta_qty_ = order_get_delta_qty(order);
    target_id_ = order_get_target_id(order);
    if (delta_qty_ == 0 || target_id_ == 0) [[unlikely]]
      return false;

#if DEBUG_ANOMALY_PRINT
    debug_.last_order = order;
#endif

    // Lookup target order (single hash operation)
    auto it = order_lookup_.find(target_id_);
    const bool found = (it != order_lookup_.end());

    //====================================================================================
    // FAST PATH: TAKER/CANCEL with existing order
    //====================================================================================
    if ((order.order_type == L2::OrderType::TAKER || order.order_type == L2::OrderType::CANCEL) &&
        found && !in_call_auction_) [[likely]] {
      Level *existing_level = it->second.level;
      actual_price_ = existing_level->price;

      if (actual_price_ == order.price) [[likely]] {
        bool was_fully_consumed = order_upsert(target_id_, actual_price_, delta_qty_, it, OrderFlags::NORMAL, existing_level);
        if (order.order_type == L2::OrderType::TAKER) {
          update_tob_after_trade(order, was_fully_consumed, actual_price_);
        }
        return true;
      }
      // Price mismatch: fall through to slow path
    }

    //====================================================================================
    // FAST PATH: MAKER (most common case)
    //====================================================================================
    if (order.order_type == L2::OrderType::MAKER && !in_call_auction_ && order.price != 0 && !found) {
      actual_price_ = order.price;
      // For MAKER, we may need to create level, so don't pass hint
      order_upsert(target_id_, actual_price_, delta_qty_, it, OrderFlags::NORMAL);
      return true;
    }

    //====================================================================================
    // SLOW PATH: Corner cases
    //====================================================================================
    return update_lob_deferred(order, it, found, in_call_auction_, in_matching_period_);
  };

  // Slow path: handle corner cases (out-of-order, call auction, special prices)
  [[gnu::cold]] [[gnu::noinline]] bool update_lob_deferred(
      const L2::Order &order,
      std::pmr::unordered_map<OrderId, Location, OrderIdHash>::iterator it,
      bool found,
      bool in_call_auction,
      bool in_matching_period) {

    //====================================================================================
    // MAKER ORDER
    //====================================================================================
    if (order.order_type == L2::OrderType::MAKER) {
      // Determine placement and flags
      Price placement_price;
      OrderFlags flags;

      if (order.price == 0) {
        // SPECIAL_MAKER: price=0 (market order, best-for-us, etc.)
        placement_price = 0; // Level[0]
        flags = OrderFlags::SPECIAL_MAKER;
      } else if (in_call_auction || in_matching_period) {
        // CALL_AUCTION: 9:15-9:30, price may not be final trade price
        placement_price = order.price;
        flags = OrderFlags::CALL_AUCTION;
      } else {
        // NORMAL: continuous auction with known price
        placement_price = order.price;
        flags = OrderFlags::NORMAL;
      }

      if (found) {
        // OUT_OF_ORDER: order exists (created by earlier TAKER/CANCEL)
        Price existing_price = it->second.level->price;
        if (existing_price != placement_price) {
          order_move_to_price(it, placement_price);
        }
        order_upsert(target_id_, placement_price, delta_qty_, it, flags);
      } else {
        // Create new order
        order_upsert(target_id_, placement_price, delta_qty_, it, flags);
      }

      return true;
    }

    //====================================================================================
    // TAKER ORDER
    //====================================================================================
    if (order.order_type == L2::OrderType::TAKER) {
      const bool is_bid = (order.order_dir == L2::OrderDirection::BID);
      const OrderId self_id = is_bid ? order.bid_order_id : order.ask_order_id;
      const bool both_ids_present = (order.bid_order_id != 0 && order.ask_order_id != 0);

      // Handle target order (counterparty)
      if (found) {
        Price target_price = it->second.level->price;

        if (target_price != order.price) {
          OrderFlags anomaly_flag = OrderFlags::NORMAL;
          if (!in_call_auction && !in_matching_period) {
            anomaly_flag = OrderFlags::ANOMALY_MATCH;
          }

          order_move_to_price(it, order.price);

          if (anomaly_flag == OrderFlags::ANOMALY_MATCH) {
            Order *target_order = it->second.level->orders[it->second.index];
            target_order->flags = anomaly_flag;
          }
        }

        actual_price_ = order.price;
        bool was_fully_consumed = order_upsert(target_id_, actual_price_, delta_qty_, it);
        update_tob_after_trade(order, was_fully_consumed, actual_price_);
      } else {
        // OUT_OF_ORDER: create placeholder
        actual_price_ = order.price;
        order_upsert(target_id_, actual_price_, delta_qty_, it, OrderFlags::OUT_OF_ORDER);
      }

      // Handle self order (market orders from Level[0])
      const bool need_self_order = (self_id != 0 && self_id != target_id_) && !both_ids_present;

      if (need_self_order) {
        auto self_it = order_lookup_.find(self_id);
        if (self_it != order_lookup_.end()) {
          Order *self_order = self_it->second.level->orders[self_it->second.index];

          if (self_order->flags == OrderFlags::SPECIAL_MAKER) {
            Price self_price = self_it->second.level->price;
            if (self_price != order.price) {
              order_move_to_price(self_it, order.price);
            }
          }

          order_upsert(self_id, order.price, -delta_qty_, self_it);
        }
      }

      return true;
    }

    //====================================================================================
    // CANCEL ORDER
    //====================================================================================
    if (order.order_type == L2::OrderType::CANCEL) {
      if (found) {
        Price self_price = it->second.level->price;

        // Migrate from Level[0] if CANCEL has price
        if (self_price == 0 && order.price != 0) {
          order_move_to_price(it, order.price);
          self_price = order.price;
        }

        order_upsert(target_id_, self_price, delta_qty_, it);
      } else {
        // OUT_OF_ORDER or ZERO_PRICE: create placeholder
        Price placement_price = (order.price == 0) ? 0 : order.price;
        OrderFlags flags = (order.price == 0) ? OrderFlags::ZERO_PRICE : OrderFlags::OUT_OF_ORDER;
        order_upsert(target_id_, placement_price, delta_qty_, it, flags);
      }

      return true;
    }

    return false;
  };

  //======================================================================================
  // PUBLIC API: Market Data Access
  //======================================================================================

  // Get best bid (highest buy price)
  HOT_NOINLINE Price best_bid() const {
    update_tob();
    return best_bid_;
  }

  // Get best ask (lowest sell price)
  HOT_NOINLINE Price best_ask() const {
    update_tob();
    return best_ask_;
  }

  // Statistics
  size_t total_orders() const { return order_lookup_.size(); }
  size_t total_levels() const { return price_levels_.size(); }
  size_t total_level_zero_orders() const {
    Level *level0 = level_find(0);
    return level0 ? level0->order_count : 0;
  }

  //======================================================================================
  // PUBLIC API: Batch Processing
  //======================================================================================

  // Process multiple orders in batch
  template <typename OrderRange>
  HOT_NOINLINE size_t process_batch(const OrderRange &order_range) {
    size_t count = 0;
    for (const auto &order : order_range) {
      if (process(order))
        ++count;
    }
    return count;
  }

  //======================================================================================
  // PUBLIC API: Market Depth Iteration
  //======================================================================================

  // Iterate through visible bid levels (descending price order)
  template <typename Func>
  void for_each_visible_bid(Func &&callback, size_t max_levels = 5) const {
    update_tob();
    visibility_refresh_cache();

    if (best_bid_ == 0 || cached_visible_prices_.empty())
      return;

    // Find position of best_bid_ in sorted cache
    auto it = std::upper_bound(cached_visible_prices_.begin(), cached_visible_prices_.end(), best_bid_);

    size_t levels_processed = 0;
    // Iterate backwards from best_bid position for descending price order
    while (it != cached_visible_prices_.begin() && levels_processed < max_levels) {
      --it;
      Price price = *it;
      Level *level = level_find(price);
      if (level && level->has_visible_quantity()) {
        callback(price, level->net_quantity);
        ++levels_processed;
      }
    }
  }

  // Iterate through visible ask levels (ascending price order)
  template <typename Func>
  void for_each_visible_ask(Func &&callback, size_t max_levels = 5) const {
    update_tob();
    visibility_refresh_cache();

    if (best_ask_ == 0 || cached_visible_prices_.empty())
      return;

    // Find position of best_ask_ in sorted cache
    auto it = std::lower_bound(cached_visible_prices_.begin(), cached_visible_prices_.end(), best_ask_);

    size_t levels_processed = 0;
    // Iterate forward from best_ask position for ascending price order
    for (; it != cached_visible_prices_.end() && levels_processed < max_levels; ++it) {
      Price price = *it;
      Level *level = level_find(price);
      if (level && level->has_visible_quantity()) {
        callback(price, level->net_quantity);
        ++levels_processed;
      }
    }
  }

  //======================================================================================
  // PUBLIC API: Utilities
  //======================================================================================

  // Complete reset
  HOT_NOINLINE void clear() {
    price_levels_.clear();
    level_storage_.clear();
    order_lookup_.clear();
    order_memory_pool_.reset();
    visible_price_bitmap_.reset(); // O(1) clear all bits
    cached_visible_prices_.clear();
    cache_dirty_ = false;
    best_bid_ = 0;
    best_ask_ = 0;
    tob_dirty_ = true;
    prev_tick_ = 0;
    curr_tick_ = 0;
    new_tick_ = false;
    in_call_auction_ = false;
    in_matching_period_ = false;
    in_continuous_trading_ = false;
    delta_qty_ = 0;
    target_id_ = 0;
    actual_price_ = 0;
#if DEBUG_ANOMALY_PRINT
    debug_.printed_anomalies.clear();
#endif

    if (DEBUG_SINGLE_DAY) {
      exit(1);
    }
  }

private:
  //======================================================================================
  // DATA STRUCTURES
  //======================================================================================

  // Price level storage (stable memory addresses via deque)
  std::deque<Level> level_storage_;                 // All price levels (deque guarantees stable pointers)
  std::unordered_map<Price, Level *> price_levels_; // Price -> Level* mapping for O(1) lookup

  // Visible price tracking (prices with non-zero net_quantity)
  std::bitset<PRICE_RANGE_SIZE> visible_price_bitmap_; // Bitmap for O(1) visibility check
  mutable std::vector<Price> cached_visible_prices_;   // Sorted cache for fast iteration
  mutable bool cache_dirty_ = false;                   // Cache needs refresh flag

  // Top of book tracking
  mutable Price best_bid_ = 0;    // Best bid price (highest buy price with visible quantity)
  mutable Price best_ask_ = 0;    // Best ask price (lowest sell price with visible quantity)
  mutable bool tob_dirty_ = true; // TOB needs recalculation flag

  // Order tracking infrastructure
  std::pmr::unsynchronized_pool_resource order_lookup_memory_pool_;      // PMR memory pool for hash map (uses default configuration)
  std::pmr::unordered_map<OrderId, Location, OrderIdHash> order_lookup_; // OrderId -> Location(Level*, index) for O(1) order lookup with identity hash
  MemPool::MemoryPool<Order> order_memory_pool_;                         // Memory pool for Order object allocation

  // Market timestamp tracking (hour|minute|second|millisecond)
  uint32_t prev_tick_ = 0; // Previous tick timestamp
  uint32_t curr_tick_ = 0; // Current tick timestamp
  bool new_tick_ = false;  // Flag: entered new tick

  // Trading session state cache (computed once per order)
  bool in_call_auction_ = false;
  bool in_matching_period_ = false;
  bool in_continuous_trading_ = false;

  // Exchange type - determines matching mechanism (SSE vs SZSE)
  ExchangeType exchange_type_ = ExchangeType::SSE;

  // Hot path temporary variable cache to reduce allocation overhead
  mutable Quantity delta_qty_; // Signed quantity change (+add/-deduct)
  mutable OrderId target_id_;  // Target order ID for current operation
  mutable Price actual_price_; // Actual effective price for current operation

  // Resampling components
  ResampleRunBar resampler_;

  //======================================================================================
  // LEVEL MANAGEMENT (Price Level Operations)
  //======================================================================================
  // Symmetric API for managing price levels in the order book
  
  // Query: Find existing level by price (returns nullptr if not found)
  HOT_INLINE Level *level_find(Price price) const {
    auto it = price_levels_.find(price);
    return (it != price_levels_.end()) ? it->second : nullptr;
  }
  
  // Optimized: Get level or create atomically (single hash lookup)
  HOT_INLINE Level *level_get_or_create(Price price) {
    auto [it, inserted] = price_levels_.try_emplace(price, nullptr);
    if (inserted) [[unlikely]] {
      level_storage_.emplace_back(price);
      it->second = &level_storage_.back();
    }
    return it->second;
  }

  // Create: Create new level (assumes level doesn't exist)
  HOT_INLINE Level *level_create(Price price) {
    level_storage_.emplace_back(price);
    Level *level = &level_storage_.back();
    price_levels_[price] = level;
    return level;
  }

  // Remove: Delete empty level from book
  HOT_INLINE void level_remove(Level *level, bool update_visibility = true) {
    price_levels_.erase(level->price);
    if (update_visibility) {
      visibility_mark_invisible_safe(level->price);
    }
  }

  //======================================================================================
  // VISIBILITY TRACKING (Price Visibility Operations)
  //======================================================================================
  // Symmetric API for managing which prices are visible (non-zero quantity) in the book
  // Uses bitmap for O(1) check and sorted cache for iteration
  
  // Cache: Rebuild sorted price cache from bitmap (called lazily when dirty)
  HOT_INLINE void visibility_refresh_cache() const {
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

  // Mark: Set price as visible (unchecked, assumes not already visible)
  HOT_INLINE void visibility_mark_visible(Price price) {
    visible_price_bitmap_.set(price);
    cache_dirty_ = true;
  }

  // Mark: Set price as invisible (unchecked, assumes currently visible)
  HOT_INLINE void visibility_mark_invisible(Price price) {
    visible_price_bitmap_.reset(price);
    cache_dirty_ = true;
  }
  
  // Safe: Set visibility with duplicate check
  HOT_INLINE void visibility_mark_visible_safe(Price price) {
    if (!visible_price_bitmap_[price]) {
      visibility_mark_visible(price);
    }
  }

  HOT_INLINE void visibility_mark_invisible_safe(Price price) {
    if (visible_price_bitmap_[price]) {
      visibility_mark_invisible(price);
    }
  }

  // Update: Sync visibility based on level's quantity state
  HOT_INLINE void visibility_update_from_level(Level *level) {
    if (level->has_visible_quantity()) {
      visibility_mark_visible_safe(level->price);
    } else {
      visibility_mark_invisible_safe(level->price);
    }
  }

  // Find next visible ask price above given price (ascending scan)
  HOT_INLINE Price next_ask_above(Price from_price) const {
    for (uint32_t price_u32 = static_cast<uint32_t>(from_price) + 1; price_u32 < PRICE_RANGE_SIZE; ++price_u32) {
      Price price = static_cast<Price>(price_u32);
      if (visible_price_bitmap_[price]) {
        return price;
      }
    }
    return 0;
  }

  // Find next visible bid price below given price (descending scan)
  HOT_INLINE Price next_bid_below(Price from_price) const {
    if (from_price == 0)
      return 0;
    for (Price price = from_price - 1; price != UINT16_MAX; --price) {
      if (visible_price_bitmap_[price]) {
        return price;
      }
    }
    return 0;
  }

  // Get minimum visible price (from sorted cache)
  HOT_INLINE Price min_visible_price() const {
    visibility_refresh_cache();
    return cached_visible_prices_.empty() ? 0 : cached_visible_prices_.front();
  }

  // Get maximum visible price (from sorted cache)
  HOT_INLINE Price max_visible_price() const {
    visibility_refresh_cache();
    return cached_visible_prices_.empty() ? 0 : cached_visible_prices_.back();
  }

  //======================================================================================
  // ORDER OPERATIONS (Order Lifecycle Management)
  //======================================================================================
  // Symmetric API for creating, modifying, and moving orders in the book
  
  // Helper: Calculate signed quantity delta for order type
  // Returns: +qty (add to level), -qty (deduct from level)
  HOT_INLINE Quantity order_get_delta_qty(const L2::Order &order) const {
    const bool is_bid = (order.order_dir == L2::OrderDirection::BID);

    switch (order.order_type) {
    case L2::OrderType::MAKER:
      return is_bid ? +order.volume : -order.volume;
    case L2::OrderType::CANCEL:
      return is_bid ? -order.volume : +order.volume;
    case L2::OrderType::TAKER: {
      // For TAKER, determine sign based on which ID is smaller (the maker side)
      bool target_is_bid;
      if (order.bid_order_id != 0 && order.ask_order_id != 0) {
        target_is_bid = (order.bid_order_id < order.ask_order_id);
      } else {
        target_is_bid = (order.bid_order_id != 0);
      }
      // Deduct from maker: BID maker needs -volume, ASK maker needs +volume
      return target_is_bid ? -order.volume : +order.volume;
    }
    default:
      return 0;
    }
  }

  // Helper: Extract target order ID (the order to be modified)
  HOT_INLINE OrderId order_get_target_id(const L2::Order &order) const {
    const bool is_bid = (order.order_dir == L2::OrderDirection::BID);

    switch (order.order_type) {
    case L2::OrderType::MAKER:
      return is_bid ? order.bid_order_id : order.ask_order_id;
    case L2::OrderType::CANCEL:
      return is_bid ? order.bid_order_id : order.ask_order_id;
    case L2::OrderType::TAKER:
      // Use smaller (earlier) ID as it must be the passive maker order
      if (order.bid_order_id != 0 && order.ask_order_id != 0) {
        return (order.bid_order_id < order.ask_order_id) ? order.bid_order_id : order.ask_order_id;
      }
      return (order.bid_order_id != 0) ? order.bid_order_id : order.ask_order_id;
    default:
      return 0;
    }
  }

  // Core: Upsert order (update existing or insert new)
  // This is the heart of the LOB reconstruction engine
  //
  // Parameters:
  //   order_id        - Unique order identifier
  //   price           - Price level for this order
  //   quantity_delta  - Signed quantity change (+add, -deduct)
  //   order_iter      - Iterator from order_lookup (from prior find operation)
  //   flags           - Order state flags (NORMAL, OUT_OF_ORDER, etc.)
  //   level_hint      - Optional pre-fetched level pointer (optimization)
  //
  // Returns: true if order was fully consumed (removed), false otherwise
  //
  // Optimization: Pass level_hint to avoid redundant hash lookups when caller
  //               already knows the target level
  HOT_NOINLINE bool order_upsert( // update and insert
      OrderId order_id,
      Price price,
      Quantity quantity_delta,
      decltype(order_lookup_.find(order_id)) order_iter,
      OrderFlags flags = OrderFlags::NORMAL,
      Level *level_hint = nullptr) {

    if (order_iter != order_lookup_.end()) {
      // ORDER EXISTS - Update existing order
      Level *level = order_iter->second.level;
      size_t order_index = order_iter->second.index;
      Order *order = level->orders[order_index];

      // Apply quantity delta
      const Quantity old_qty = order->qty;
      const Quantity new_qty = old_qty + quantity_delta;

      if (new_qty == 0) {
        // FULLY CONSUMED - Remove order completely
#if DEBUG_ORDER_FLAGS_RESOLVE
        if (order->flags != OrderFlags::NORMAL) {
          print_order_flags_resolve(order_id, price, price, old_qty, 0, order->flags, OrderFlags::NORMAL, "CONSUME   ");
        }
#endif

        // Remove order from level
        level->remove(order_index);
        order_lookup_.erase(order_iter);

        // Fix up order_lookup_ index for moved order (swap-and-pop side effect)
        if (order_index < level->orders.size()) {
          auto moved_order_iter = order_lookup_.find(level->orders[order_index]->id);
          if (moved_order_iter != order_lookup_.end()) {
            moved_order_iter->second.index = order_index;
          }
        }

        // Cleanup: Remove empty level or update visibility
        const bool was_visible = level->has_visible_quantity();
        if (level->empty()) {
          level_remove(level, was_visible);
        } else if (was_visible != level->has_visible_quantity()) {
          visibility_update_from_level(level);
        }

        return true; // Fully consumed
      } else {
        // PARTIALLY CONSUMED - Update order quantity
        const bool was_visible = level->has_visible_quantity();
        level->net_quantity += quantity_delta;
        order->qty = new_qty;

        // Update flags if needed
        [[maybe_unused]] OrderFlags old_flags = order->flags;
        if (flags != OrderFlags::NORMAL || order->flags != OrderFlags::NORMAL) {
#if DEBUG_ORDER_FLAGS_RESOLVE
          if (old_flags != flags) {
            print_order_flags_resolve(order_id, price, price, old_qty, new_qty, old_flags, flags, "UPDATE_FLG");
          }
#endif
          order->flags = flags;
        }
        
        // Update visibility only if it changed
        if (was_visible != level->has_visible_quantity()) {
          visibility_update_from_level(level);
        }
        return false; // Partially consumed
      }

    } else {
      // ORDER DOESN'T EXIST - Create new order
#if DEBUG_ANOMALY_PRINT
      Order *new_order = order_memory_pool_.construct(quantity_delta, order_id, curr_tick_, flags);
#else
      Order *new_order = order_memory_pool_.construct(quantity_delta, order_id, flags);
#endif
      if (!new_order)
        return false;

      // Get level: use hint if provided (optimization), otherwise get or create atomically
      Level *level = level_hint ? level_hint : level_get_or_create(price);

      // Add order to level and register in lookup table
      size_t order_index = level->orders.size();
      level->add(new_order);
      order_lookup_.try_emplace(order_id, level, order_index);
      
      // Update visibility if level just became visible
      if (quantity_delta != 0 && !visible_price_bitmap_[level->price]) {
        visibility_mark_visible(level->price);
      }

#if DEBUG_ORDER_FLAGS_CREATE
      print_order_flags_create(order_id, price, quantity_delta, flags);
#endif

      return false; // New order created
    }
  }

  //======================================================================================
  // ORDER MIGRATION (Move orders between price levels)
  //======================================================================================
  // Used for corner cases: price mismatch, special orders, etc.
  
  // Move: Relocate order from one price level to another (using iterator)
  // Optimization: Accepts iterator to avoid redundant hash lookup
  HOT_NOINLINE void order_move_to_price(
      std::pmr::unordered_map<OrderId, Location, OrderIdHash>::iterator order_iter, 
      Price new_price) {
    Level *old_level = order_iter->second.level;
    size_t old_index = order_iter->second.index;
    Price old_price = old_level->price;

    if (old_price == new_price)
      return; // Already at correct level

    Order *order = old_level->orders[old_index];

    // Step 1: Remove from old level (swap-and-pop)
    old_level->remove(old_index);

    // Step 2: Fix up order_lookup_ for swapped order (swap-and-pop side effect)
    if (old_index < old_level->orders.size()) {
      auto swapped_order_iter = order_lookup_.find(old_level->orders[old_index]->id);
      if (swapped_order_iter != order_lookup_.end()) {
        swapped_order_iter->second.index = old_index;
      }
    }

    // Step 3: Add to new level (get or create atomically)
    Level *new_level = level_get_or_create(new_price);

    size_t new_index = new_level->orders.size();
    new_level->add(order);

    // Step 4: Update order_lookup_ to point to new location
    order_iter->second.level = new_level;
    order_iter->second.index = new_index;

    // Step 5: Update visibility for both levels
    visibility_update_from_level(old_level);
    visibility_update_from_level(new_level);

    // Step 6: Cleanup empty old level
    if (old_level->empty()) {
      level_remove(old_level);
    }

#if DEBUG_ORDER_FLAGS_RESOLVE
    print_order_flags_resolve(order->id, old_price, new_price, order->qty, order->qty, order->flags, order->flags, "MIGRATE   ");
#endif
  }

  // Move: Relocate order by ID (requires hash lookup, less efficient)
  HOT_NOINLINE void order_move_by_id(OrderId order_id, Price new_price) {
    auto order_iter = order_lookup_.find(order_id);
    if (order_iter == order_lookup_.end())
      return; // Order not found

    order_move_to_price(order_iter, new_price);
  }

  //======================================================================================
  // TOP OF BOOK (TOB)
  //======================================================================================

  // Bootstrap TOB from visible prices (lazy initialization)
  HOT_INLINE void update_tob() const {
    if (!tob_dirty_)
      return;

    if (best_bid_ == 0 && best_ask_ == 0) {
      best_bid_ = max_visible_price();
      best_ask_ = min_visible_price();
    }

    tob_dirty_ = false;
  }

  // Update one side of TOB after trade
  HOT_INLINE void update_tob_one_side(bool is_active_bid, bool was_fully_consumed, Price trade_price) {
    if (was_fully_consumed) {
      // Level emptied - advance to next level
      if (is_active_bid) {
        best_ask_ = next_ask_above(trade_price);
      } else {
        best_bid_ = next_bid_below(trade_price);
      }
    } else {
      // Level partially filled - TOB stays at trade price
      if (is_active_bid) {
        best_ask_ = trade_price;
      } else {
        best_bid_ = trade_price;
      }
    }
    tob_dirty_ = false;
  }

  // Update TOB after single-side trade (SSE continuous trading)
  HOT_INLINE void update_tob_after_trade(const L2::Order &order, bool was_fully_consumed, Price trade_price) {
    const bool is_active_bid = (order.order_dir == L2::OrderDirection::BID);
    update_tob_one_side(is_active_bid, was_fully_consumed, trade_price);
  }

  //======================================================================================
  // TRADING SESSION STATE
  //======================================================================================

  // Update session state flags based on current time (called when tick changes)
  HOT_NOINLINE void update_trading_session_state() {
    const uint16_t hhmm = ((curr_tick_ >> 16) & 0xFFFF); // hour * 256 + minute

    // Time boundaries
    constexpr uint16_t T_0915 = (9 << 8) | 15;  // 9:15
    constexpr uint16_t T_0925 = (9 << 8) | 25;  // 9:25
    constexpr uint16_t T_0930 = (9 << 8) | 30;  // 9:30
    constexpr uint16_t T_1457 = (14 << 8) | 57; // 14:57
    constexpr uint16_t T_1500 = (15 << 8) | 0;  // 15:00

    // Matching period: 9:25-9:30, 14:57-15:00
    in_matching_period_ = (hhmm >= T_0925 && hhmm < T_0930) || (hhmm >= T_1457 && hhmm <= T_1500);

    // Call auction: 9:15-9:30, 14:57-15:00 (includes matching period)
    in_call_auction_ = (hhmm >= T_0915 && hhmm < T_0930) || (hhmm >= T_1457 && hhmm <= T_1500);

    // Continuous trading: outside call auction
    in_continuous_trading_ = !in_call_auction_;
  }

  // Clear CALL_AUCTION flags at 9:30:00 (orders stay at current levels)
  HOT_NOINLINE void flush_call_auction_flags() {
    for (auto &[price, level] : price_levels_) {
      for (Order *order : level->orders) {
        if (order->flags == OrderFlags::CALL_AUCTION) {
#if DEBUG_ORDER_FLAGS_RESOLVE
          print_order_flags_resolve(order->id, price, price, order->qty, order->qty, OrderFlags::CALL_AUCTION, OrderFlags::NORMAL, "FLUSH_930 ");
#endif
          order->flags = OrderFlags::NORMAL;
        }
      }
    }
    // TOB will be updated by first continuous trading order
  }

  //======================================================================================
  // TIME UTILITIES
  //======================================================================================

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

  //======================================================================================
  // DEBUG: Order Flags
  //======================================================================================

  // Helper: Get flags string
  static const char *get_order_flags_str(OrderFlags flags) {
    switch (flags) {
    case OrderFlags::NORMAL:
      return "NORMAL         ";
    case OrderFlags::UNKNOWN:
      return "UNKNOWN        ";
    case OrderFlags::OUT_OF_ORDER:
      return "OUT_OF_ORDER   ";
    case OrderFlags::CALL_AUCTION:
      return "CALL_AUCTION   ";
    case OrderFlags::SPECIAL_MAKER:
      return "SPECIAL_MAKER  ";
    case OrderFlags::ZERO_PRICE:
      return "ZERO_PRICE     ";
    case OrderFlags::ANOMALY_MATCH:
      return "ANOMALY_MATCH  ";
    default:
      return "UNKNOWN_FLAG   ";
    }
  }

#if DEBUG_ORDER_FLAGS_CREATE
  // 🟡 CREATE: Print when order with special flags is created (Yellow)
  void print_order_flags_create(OrderId order_id, Price price, Quantity qty, OrderFlags flags) const {
    if (flags == OrderFlags::NORMAL)
      return; // Skip normal orders

    std::cout << "\033[33m[CREATE] " << format_time()
              << " | " << get_order_flags_str(flags)
              << " | ID=" << std::setw(7) << std::right << order_id
              << " Price=" << std::setw(5) << std::right << price
              << " Qty=" << std::setw(6) << std::right << qty
              << " | TotalOrders=" << std::setw(5) << std::right << (total_orders() + 1)
              << "\033[0m\n";
  }
#endif

#if DEBUG_ORDER_FLAGS_RESOLVE
  // 🔵 RESOLVE: Print when order with special flags is resolved (Blue)
  // Types: MIGRATE (price change), CONSUME (fully matched), UPDATE_FLAGS (flags changed)
  void print_order_flags_resolve(OrderId order_id, Price old_price, Price new_price,
                                 Quantity old_qty, Quantity new_qty,
                                 OrderFlags old_flags, OrderFlags new_flags,
                                 const char *action) const {
    if (old_flags == OrderFlags::NORMAL && new_flags == OrderFlags::NORMAL)
      return; // Skip normal orders

    std::cout << "\033[36m[" << action << "] " << format_time()
              << " | " << get_order_flags_str(old_flags)
              << " → " << get_order_flags_str(new_flags)
              << " | ID=" << std::setw(7) << std::right << order_id;

    // Price field: show old→new if changed, otherwise single value (12 chars total)
    if (old_price != new_price) {
      std::cout << " Price=" << std::setw(5) << std::right << old_price
                << "→" << std::setw(5) << std::right << new_price;
    } else {
      std::cout << " Price=" << std::setw(5) << std::right << old_price << "      ";
    }

    // Qty field: show old→new if changed, otherwise single value (12 chars total)
    if (old_qty != new_qty) {
      std::cout << " Qty=" << std::setw(6) << std::right << old_qty
                << "→" << std::setw(5) << std::right << new_qty;
    } else {
      std::cout << " Qty=" << std::setw(6) << std::right << new_qty << "      ";
    }

    std::cout << " | TotalOrders=" << std::setw(5) << std::right << total_orders()
              << "\033[0m\n";
  }
#endif

#if DEBUG_ANOMALY_PRINT
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
#endif

  //======================================================================================
  // DEBUG: Anomaly Detection
  //======================================================================================
#if DEBUG_ANOMALY_PRINT

  // Debug state storage
  struct DebugState {
    L2::Order last_order;
    std::unordered_set<Price> printed_anomalies;
  };
  mutable DebugState debug_;

  // Check for sign anomaly in level (print far anomalies N+ ticks from TOB during continuous trading)
  void check_anomaly(Level *level) const {
    using namespace TradingSession;
    using namespace AnomalyDetection;
    update_tob();

    // Step 1: Distance filter - only check far levels (N+ ticks from TOB)
    const bool is_far_below_bid = (best_bid_ > 0 && level->price < best_bid_ - MIN_DISTANCE_FROM_TOB);
    const bool is_far_above_ask = (best_ask_ > 0 && level->price > best_ask_ + MIN_DISTANCE_FROM_TOB);
    if (!is_far_below_bid && !is_far_above_ask)
      return;

    // Step 2: Classify by price relative to TOB mid price
    const Price tob_mid = (best_bid_ + best_ask_) / 2;
    const bool is_bid_side = (level->price < tob_mid);

    const bool has_anomaly = (is_bid_side && level->net_quantity < 0) || (!is_bid_side && level->net_quantity > 0);

    // Skip if no anomaly or already printed
    if (!has_anomaly)
      return;
    if (debug_.printed_anomalies.count(level->price))
      return;

    // Step 3: Time filter - only print during continuous trading (use cached state)
    if (!in_continuous_trading_) {
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
      if (is_reverse)
        anomaly_orders.push_back(order);
    }
    if (anomaly_orders.empty())
      return;

    // Sort by ID (smallest first, earlier orders)
    std::sort(anomaly_orders.begin(), anomaly_orders.end(),
              [](const Order *a, const Order *b) { return a->id < b->id; });

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

  //======================================================================================
  // DEBUG: Book Display
  //======================================================================================

  // Display current market depth
  void inline print_book([[maybe_unused]] const L2::Order &order) const {
    // Don't print if there are no visible prices (covers pre-9:30 case naturally)
    visibility_refresh_cache();
    if (cached_visible_prices_.empty())
      return;

    // Only print during continuous trading (9:30-15:00) - use cached state
    if (!in_continuous_trading_)
      return;

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
      book_output << "[" << format_time() << "] [" << std::setfill('0') << std::setw(3) << total_level_zero_orders() << std::setfill(' ') << "] ";

      using namespace BookDisplay;

      update_tob();

      // Skip printing if TOB is invalid (e.g. after flush_call_auction_flags)
      if (best_bid_ > best_ask_)
        return;

#if DEBUG_ANOMALY_PRINT
      // At continuous trading start (09:30:00), scan all existing levels
      // This ensures anomalies that existed during call auction are detected
      static uint32_t last_check_second = 0;
      const uint32_t curr_second = (curr_tick_ >> 8); // Remove milliseconds
      if (curr_second != last_check_second) {
        last_check_second = curr_second;
        const uint8_t hour = (curr_tick_ >> 24) & 0xFF;
        const uint8_t minute = (curr_tick_ >> 16) & 0xFF;
        const uint8_t second = (curr_tick_ >> 8) & 0xFF;

        using namespace TradingSession;
        if (hour == CONTINUOUS_TRADING_START_HOUR &&
            minute == CONTINUOUS_TRADING_START_MINUTE &&
            second == 0) {
          debug_.printed_anomalies.clear();
          visibility_refresh_cache();
          for (const Price price : cached_visible_prices_) {
            Level *level = level_find(price);
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

      // Count anomalies: wrong sign on non-TOB levels
      size_t anomaly_count = 0;
      visibility_refresh_cache();
      for (const Price price : cached_visible_prices_) {
        // Skip TOB area
        if (price >= best_bid_ && price <= best_ask_)
          continue;

        const Level *level = level_find(price);
        if (!level || !level->has_visible_quantity())
          continue;

        // BID side (< best_bid) should be positive, ASK side (> best_ask) should be negative
        if ((price < best_bid_ && level->net_quantity < 0) || (price > best_ask_ && level->net_quantity > 0)) {
          ++anomaly_count;
        }
      }

      if (anomaly_count > 0) {
        book_output << " \033[31m[" << anomaly_count << " anomalies]\033[0m";
      }

      std::cout << book_output.str() << "\n";

#if DEBUG_ANOMALY_PRINT
      // Check anomalies for ALL visible levels (proactive detection)
      for (const Price price : cached_visible_prices_) {
        Level *level = level_find(price);
        if (level && level->has_visible_quantity()) {
          check_anomaly(level);
        }
      }
#endif
    }
#if DEBUG_ORDER_PRINT
    char order_type_char = (order.order_type == L2::OrderType::MAKER) ? 'M' : (order.order_type == L2::OrderType::CANCEL) ? 'C'
                                                                                                                          : 'T';
    char order_dir_char = (order.order_dir == L2::OrderDirection::BID) ? 'B' : 'S';
    std::cout << "[" << format_time() << "] " << " ID: " << order_get_target_id(order) << " Type: " << order_type_char << " Direction: " << order_dir_char << " Price: " << order.price << " Volume: " << order.volume << std::endl;
#endif
  }
};
