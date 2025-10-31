#pragma once

#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <vector>

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "define/FastBitmap.hpp"
#include "define/MemPool.hpp"
// #include "features/backend/FeatureStoreConfig.hpp"
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
#define DEBUG_BOOK_PRINT 0          // Print order book snapshot when effective TOB updated
#define DEBUG_BOOK_AS_AMOUNT 1      // 0: 股, 1: 1万元, 2: 2万元, 3: 3万元, ...
#define DEBUG_ANOMALY_PRINT 1       // Print max unmatched order with creation timestamp

// Auto-disable dependent switches based on logical relationships
#if DEBUG_BOOK_PRINT == 0
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

// Effective TOB filter parameters
namespace EffectiveTOBFilter {
constexpr uint32_t MIN_TIME_INTERVAL_MS = 100; // Minimum time interval in milliseconds for effective TOB update
}

// 设计理念: 抵扣模型 + Order迁移机制
// --------------------------------------------------------------------------------
// - TAKER成交价是最终真相, MAKER挂价可能不准确
// - 不要尝试维护完全准确的TOB, TOB用最近的成交价定义, 如果吃空则顺延
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
// 1. OUT_OF_ORDER (乱序到达) - ~2-5% - TAKER/CANCEL先于MAKER到达, 预创建Order后抵扣
// 2. CALL_AUCTION (集合竞价) - 9:15-9:30, 14:57-15:00 - MAKER挂价≠撮合价, TAKER到达时迁移
// 3. SPECIAL_MAKER (特殊挂单) - ~1-2% - 市价单('1')/本方最优('U')等price=0订单, 存Level[0]后迁移
// 4. ZERO_PRICE_CANCEL (深交所零价格撤单) - ~5-10% of cancels - 深交所撤单price=0, 存Level[0]待MAKER到达迁移
// 5. ANOMALY_MATCH (异常撮合) - rare - 连续竞价时MAKER挂价≠TAKER成交价, 按成交价迁移并标记
// 6. UNKNOWN (信息不足) - temporary - 无法确定订单类型的暂时状态, 等待后续数据澄清
//
//========================================================================================
// ORDER PROCESSING FLOW (3 ORDER TYPES)
//========================================================================================
//
// ┌─────────────────────────────────────────────────────────────────────────────────┐
// │ 1. MAKER FLOW (挂单)                                                            │
// └─────────────────────────────────────────────────────────────────────────────────┘
//
// 触发: order_type == MAKER
// 特点: 永不进入循环，单次lookup → order_upsert → 返回
//
// 流程 (99%+):
//   1. lookup(order_id) → not found
//   2. order_upsert(创建Order, flags=NORMAL, 放置在Level[price])
//      └─ level->add(order) → level.net_quantity += qty
//      └─ visibility: if (!bitmap.test(price)) → bitmap.set(price)
//   3. return
//
// 特殊情况 (1%-):
//   - OUT_OF_ORDER: order已存在 → 迁移价格 + 补充qty
//      └─ order_move_to_price() → 更新 old_level + new_level visibility
//   - CALL_AUCTION: in_call_auction=true → flags=CALL_AUCTION
//   - SPECIAL_MAKER: price=0 → 放置在Level[0]
//
// ┌─────────────────────────────────────────────────────────────────────────────────┐
// │ 2. TAKER FLOW (成交)                                                            │
// └─────────────────────────────────────────────────────────────────────────────────┘
//
// 触发: order_type == TAKER
// 交易所差异:
//   - SSE: 集合竞价撮合期(9:25-9:30, 14:57-15:00)双边，连续竞价(9:30-14:57)单边
//   - SZSE: 全天双边
//
// 核心函数: process_taker_side(order, order_id, delta)
//   - order_id: 对手方订单ID
//   - delta: bid侧负值, ask侧正值
//   - 返回: true=完全消耗, false=部分消耗或新创建
//
// ─────────────────────────────────────────────────────────────────────────────────
// 2.1 双边撮合
// ─────────────────────────────────────────────────────────────────────────────────
// 触发: need_bilateral && bid_id != 0 && ask_id != 0
//
// 流程 (99%+):
//   1. consumed_bid = process_taker_side(order, bid_id, -volume)
//      → lookup(bid_id) → order_upsert(抵扣qty) → 返回是否完全消耗
//      └─ level.net_quantity -= volume
//      └─ visibility: 检查 level.has_visible_quantity()
//         - 仍有挂单(≠0): 无操作
//         - 变空(=0): bitmap.clear(price)
//   2. consumed_ask = process_taker_side(order, ask_id, +volume)
//      → lookup(ask_id) → order_upsert(抵扣qty) → 返回是否完全消耗
//      └─ visibility: 同上
//   3. update_tob_one_side(is_active_bid, consumed_bid, price)
//
// 特殊情况 (1%-):
//   - 价格不匹配 → order_move_to_price() + flags=ANOMALY_MATCH
//      └─ 更新 old_level + new_level visibility
//   - OUT_OF_ORDER → 预创建Order于Level[0]
//
// ─────────────────────────────────────────────────────────────────────────────────
// 2.2 单边撮合
// ─────────────────────────────────────────────────────────────────────────────────
// 触发: !need_bilateral || 只有一个ID != 0
//
// 流程 (99%+):
//   1. order_extract_params(order) → (target_id, delta_qty, is_bid)
//   2. consumed = process_taker_side(order, target_id, delta_qty)
//      → lookup(target_id) → order_upsert(抵扣qty) → 返回是否完全消耗
//      └─ visibility: 检查 level.has_visible_quantity()
//         - 仍有挂单(≠0): 无操作
//         - 变空(=0): bitmap.clear(price)
//   3. update_tob_one_side(is_bid, consumed, price)
//
// 特殊情况 (1%-): 与双边相同
//
// ┌─────────────────────────────────────────────────────────────────────────────────┐
// │ 3. CANCEL FLOW (撤单)                                                           │
// └─────────────────────────────────────────────────────────────────────────────────┘
//
// 触发: order_type == CANCEL
// 特点: 复用process_taker_side()，但不更新TOB
//
// 流程 (99%+):
//   1. order_extract_params(order) → (target_id, delta_qty, is_bid)
//   2. consumed = process_taker_side(order, target_id, delta_qty)
//      → lookup(target_id) → order_upsert(减少qty) → 返回是否完全消耗
//      └─ level.net_quantity -= qty
//      └─ visibility: 检查 level.has_visible_quantity()
//         - 仍有挂单(≠0): 无操作
//         - 变空(=0): bitmap.clear(price)
//   3. return (不更新TOB)
//
// 特殊情况 (1%-):
//   - ZERO_PRICE_CANCEL: price=0撤单 → 预创建于Level[0]
//   - SPECIAL_MAKER撤单: 撤销Level[0]的订单
//   - OUT_OF_ORDER → 预创建Order于Level[0]
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
// TAKER:  1/2x hash lookup (order_lookup_) + deduct Order   → O(1/2)
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
//========================================================================================
// N-档市场深度数据维护
//========================================================================================
//
// 订单(逐笔, 高频)+tob更新(时间过滤, 低频)驱动
// visible_price_bitmap_: 逐笔全局位图(逐笔): price → next/prev_visible(net_quantity!=0)_price index 映射
// LOB_feature_.depth_buffer_[2N]: (订单+时间):
//    CBuffer: [0]:卖N, [N-1]:卖1, [N]:买1, ..., [2N-1]:买N, 价格单调下降
//    数据单元: {Level* level_ptr}, 支持ring_buffer/dequeue通用API: pop, push, insert, erase, etc.
//    index -> price/volume(Level) 映射
//
// 更新流程:
//   1. 订单based(逐笔, 高频):
//          每个订单(哪怕是双边撮合的taker)只会最多trigger一个add/remove level操作
//          通过判断 depth_buffer_[0/-1].price 来判断价格是否在N-档范围内, 如果在, 用binary search做price -> index, 更新depth_buffer_
//   2. tob更新based(时间过滤, 低频):
//          tob 更新触发条件:
//          - best_bid_ < best_ask_
//          - best_bid_volume > 0 && best_ask_volume < 0
//          - curr_tick_ > next_depth_update_tick_
//          TOB更新晚于逐笔更新, 所以触发后, 只需要判断向左/右push几次就可以, 先通过bitmap_找到对应的price, Level, 再push_front/back

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
struct alignas(16) Order {
  Quantity qty;
  OrderId id;
  uint32_t timestamp; // Creation timestamp (updated in DEBUG_ANOMALY_PRINT mode only)
  OrderFlags flags;   // Order state/type flags
  uint8_t padding[3]; // Explicit padding for alignment

  Order(Quantity q, OrderId i, uint32_t ts = 0, OrderFlags f = OrderFlags::NORMAL)
      : qty(q), id(i), timestamp(ts), flags(f) {}

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

  // Move constructor (for deque reallocation optimization)
  Level(Level &&other) noexcept
      : price(other.price),
        net_quantity(other.net_quantity),
        order_count(other.order_count),
        orders(std::move(other.orders)) {}

  // Disable copy to prevent accidental expensive copies
  Level(const Level &) = delete;
  Level &operator=(const Level &) = delete;
  Level &operator=(Level &&) = delete;

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

// Order location tracking (POD for fast construction)
struct Location {
  Level *level;
  size_t index;

  Location() = default;
  constexpr Location(Level *l, size_t i) noexcept : level(l), index(i) {}
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

class LimitOrderBook {

public:
  //======================================================================================
  // CONSTRUCTOR & CONFIGURATION
  //======================================================================================

  explicit LimitOrderBook(size_t ORDER_SIZE = L2::DEFAULT_ENCODER_ORDER_SIZE, ExchangeType exchange_type = ExchangeType::SSE)
      : price_levels_(10),
        order_lookup_(ORDER_SIZE),      // BumpDict with pre-allocated capacity
        order_memory_pool_(ORDER_SIZE), // BumpPool for Order objects
        exchange_type_(exchange_type) {
    // Depth is updated in time-driven manner
  }

  //======================================================================================
  // PUBLIC API: Order Processing
  //======================================================================================

  // Process single order and update LOB state
  HOT_NOINLINE bool process(const L2::Order &order) {
    // Parse timestamp
    curr_tick_ = (order.hour << 24) | (order.minute << 16) | (order.second << 8) | order.millisecond;
    new_tick_ = curr_tick_ != prev_tick_;
    curr_sec_ = (curr_tick_ >> 8);
    new_sec_ = curr_sec_ != prev_sec_;

    // Update feature timestamp (only on new tick)
    if (new_tick_) {
      LOB_feature_.hour = order.hour;
      LOB_feature_.minute = order.minute;
      LOB_feature_.second = order.second;
      LOB_feature_.millisecond = order.millisecond;
      update_trading_session_state();
    }

    // Parse order metadata (cache for reuse)
    is_maker_ = (order.order_type == L2::OrderType::MAKER);
    is_taker_ = (order.order_type == L2::OrderType::TAKER);
    is_cancel_ = (order.order_type == L2::OrderType::CANCEL);
    is_bid_ = (order.order_dir == L2::OrderDirection::BID);

    // Update feature order metadata (every order)
    LOB_feature_.is_maker = is_maker_;
    LOB_feature_.is_taker = is_taker_;
    LOB_feature_.is_cancel = is_cancel_;
    LOB_feature_.is_bid = is_bid_;
    LOB_feature_.price = order.price;
    LOB_feature_.volume = order.volume;

    // Detect transition from matching period to continuous trading
    static bool was_in_matching_period = false; // only inited at 1st call of process(), persist across calls
    if (was_in_matching_period && !in_matching_period_ && !in_call_auction_) [[unlikely]] {
      flush_call_auction_flags();
    }
    was_in_matching_period = in_matching_period_;

    bool result = update_lob(order);

    // Process resampling (only for TAKER orders)
    if (is_taker_) {
      if (resampler_.resample(curr_tick_, is_bid_, order.volume)) {
        // std::cout << "[RESAMPLE] Bar formed at " << format_time() << std::endl;
      }
    }

    // Time-driven depth update (check if it's time to update)
    if (curr_tick_ >= next_depth_update_tick_) {
      sync_tob_to_depth_center();
    }

    prev_tick_ = curr_tick_;
    prev_sec_ = curr_tick_ >> 8;

    return result;
  };

  //======================================================================================
  // PUBLIC API: Utilities
  //======================================================================================

  // Complete reset
  HOT_NOINLINE void clear() {
    price_levels_.clear();
    level_storage_.clear();
    order_lookup_.clear();
    order_memory_pool_.reset();
    visible_price_bitmap_.reset();
    best_bid_ = 0;
    best_ask_ = 0;
    tob_dirty_ = true;
    LOB_feature_ = {};
    depth_buffer_.clear();
    last_depth_update_tick_ = 0;
    next_depth_update_tick_ = 0;
    prev_tick_ = 0;
    curr_tick_ = 0;
    new_tick_ = false;
    new_sec_ = false;
    in_call_auction_ = false;
    in_matching_period_ = false;
    in_continuous_trading_ = false;
    delta_qty_ = 0;
    target_id_ = 0;
    actual_price_ = 0;
#if DEBUG_ANOMALY_PRINT
    debug_.printed_anomalies.clear();
#endif

    if (DEBUG_BOOK_PRINT) { // exit to only print 1 day
      exit(0);
    }
  }

private:
  //======================================================================================
  // DATA STRUCTURES (按层次组织)
  //======================================================================================

  //------------------------------------------------------------------------------------
  // Layer 1: Price Level Storage (价格档位基础层)
  //------------------------------------------------------------------------------------
  std::deque<Level> level_storage_;                // All price levels (deque guarantees stable pointers)
  MemPool::BumpDict<Price, Level *> price_levels_; // Price -> Level* mapping for O(1) lookup (few erases, BumpDict is fine)

  //------------------------------------------------------------------------------------
  // Layer 2: Order Tracking Infrastructure (订单追踪层)
  //------------------------------------------------------------------------------------
  MemPool::BumpDict<OrderId, Location, OrderIdHash> order_lookup_; // OrderId -> Location(Level*, index) for O(1) order lookup
  MemPool::BumpPool<Order> order_memory_pool_;                     // Memory pool for Order object allocation

  //------------------------------------------------------------------------------------
  // Layer 3: Global Visibility Tracking (全局可见性层 - 逐笔更新)
  //------------------------------------------------------------------------------------
  FastBitmap<PRICE_RANGE_SIZE> visible_price_bitmap_; // Bitmap: mark all prices with net_quantity ≠ 0
                                                      // Usage: find_next/prev for adjacent price lookup
                                                      // Update: immediate O(1) on any level change

  //------------------------------------------------------------------------------------
  // Layer 4: Tick-by-Tick TOB (逐笔盘口层)
  //------------------------------------------------------------------------------------
  mutable Price best_bid_ = 0;    // Tick-by-tick best bid (highest buy price with visible quantity)
  mutable Price best_ask_ = 0;    // Tick-by-tick best ask (lowest sell price with visible quantity)
  mutable bool tob_dirty_ = true; // TOB needs recalculation flag

  //------------------------------------------------------------------------------------
  // Layer 5: Feature Depth (特征深度层 - 时间驱动低频更新)
  //------------------------------------------------------------------------------------
  mutable L2::LOB_Feature LOB_feature_; // Feature depth data (N≤20 levels): bid/ask price_ticks[], volumes[]

  // N-level depth buffer (订单+时间双驱动):
  // CBuffer: [0]:卖N, [N-1]:卖1, [N]:买1, ..., [2N-1]:买N, 价格单调下降
  static constexpr size_t DEPTH_N = L2::LOB_FEATURE_DEPTH_LEVELS;
  CBuffer<Level *, 2 * DEPTH_N> depth_buffer_; // Level* pointers for efficient depth tracking

  // Time-driven depth update control
  mutable uint32_t last_depth_update_tick_ = 0; // Last tick when depth was updated
  mutable uint32_t next_depth_update_tick_ = 0; // Next allowed tick for depth update

  //------------------------------------------------------------------------------------
  // Auxiliary State (辅助状态)
  //------------------------------------------------------------------------------------

  // Market timestamp tracking (hour|minute|second|millisecond)
  uint32_t prev_tick_ = 0; // Previous tick timestamp
  uint32_t curr_tick_ = 0; // Current tick timestamp
  uint32_t prev_sec_ = 0;  // Previous second timestamp
  uint32_t curr_sec_ = 0;  // Current second timestamp
  bool new_tick_ = false;  // Flag: entered new tick
  bool new_sec_ = false;   // Flag: entered new second (for feature snapshot update)

  // Trading session state cache (computed once per order)
  bool in_call_auction_ = false;
  bool in_matching_period_ = false;
  bool in_continuous_trading_ = false;

  // Exchange type - determines matching mechanism (SSE vs SZSE)
  ExchangeType exchange_type_ = ExchangeType::SSE;

  // Hot path temporary variable cache (reduce allocation overhead)
  mutable Quantity delta_qty_; // Signed quantity change (+add/-deduct)
  mutable OrderId target_id_;  // Target order ID for current operation
  mutable Price actual_price_; // Actual effective price for current operation

  // Order parsing cache (parsed once per order in process())
  mutable bool is_maker_;
  mutable bool is_taker_;
  mutable bool is_cancel_;
  mutable bool is_bid_;

  // Resampling components
  ResampleRunBar resampler_;

  //======================================================================================
  // LEVEL MANAGEMENT (价格档位基础操作)
  //======================================================================================

  // Query: Find existing level by price (returns nullptr if not found)
  HOT_INLINE Level *level_find(Price price) const {
    Level *const *level_ptr = price_levels_.find(price);
    return level_ptr ? *level_ptr : nullptr;
  }

  // Get or create: Atomically get existing level or create new one (single hash lookup)
  [[gnu::hot, gnu::always_inline]]
  inline Level *level_get_or_create(Price price) {
    auto [level_ptr, inserted] = price_levels_.try_emplace(price, nullptr);
    if (inserted) [[unlikely]] {
      level_storage_.emplace_back(price);
      *level_ptr = &level_storage_.back();
    }
    return *level_ptr;
  }

  // Create: Create new level (assumes level doesn't exist)
  HOT_INLINE Level *level_create(Price price) {
    level_storage_.emplace_back(price);
    Level *level = &level_storage_.back();
    price_levels_.insert(price, level);
    return level;
  }

  // Remove: Delete empty level from book
  HOT_INLINE void level_remove(Level *level, bool update_visibility = true) {
    Price price = level->price;
    price_levels_.erase(price);
    if (update_visibility) {
      visibility_mark_invisible_safe(price);
    }
  }

  //======================================================================================
  // VISIBILITY TRACKING (可见性位图维护)
  //======================================================================================

  // Check if price has visible quantity
  HOT_INLINE bool visibility_is_visible(Price price) const {
    return visible_price_bitmap_.test(price);
  }

  // Mark price as visible (O(1))
  HOT_INLINE void visibility_mark_visible(Price price) {
    visible_price_bitmap_.set(price);
    // Order-driven depth update: level became visible
    Level *level = level_find(price);
    if (level) {
      depth_on_level_add(level);
    }
  }

  // Mark price as invisible (O(1))
  HOT_INLINE void visibility_mark_invisible(Price price) {
    visible_price_bitmap_.clear(price);
    // Order-driven depth update: level became invisible
    Level *level = level_find(price);
    if (level) {
      depth_on_level_remove(level);
    }
  }

  // Mark with duplicate check
  HOT_INLINE void visibility_mark_visible_safe(Price price) {
    if (!visibility_is_visible(price)) {
      visibility_mark_visible(price);
    }
  }

  HOT_INLINE void visibility_mark_invisible_safe(Price price) {
    if (visibility_is_visible(price)) {
      visibility_mark_invisible(price);
    }
  }

  // Update visibility based on level state
  HOT_INLINE void visibility_update_from_level(Level *level) {
    if (level->has_visible_quantity()) {
      visibility_mark_visible_safe(level->price);
    } else {
      visibility_mark_invisible_safe(level->price);
    }
  }

  // Find next visible price (bitmap scan)
  HOT_INLINE Price next_ask_above(Price from_price) const {
    size_t next = visible_price_bitmap_.find_next(from_price);
    return (next < PRICE_RANGE_SIZE) ? static_cast<Price>(next) : 0;
  }

  HOT_INLINE Price next_bid_below(Price from_price) const {
    size_t prev = visible_price_bitmap_.find_prev(from_price);
    return (prev < PRICE_RANGE_SIZE) ? static_cast<Price>(prev) : 0;
  }

  //======================================================================================
  // TIME UTILITIES (时间工具函数)
  //======================================================================================

  // Convert packed timestamp to human-readable format (HH:MM:SS.mmm)
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

  // Convert packed tick to milliseconds
  HOT_INLINE uint32_t tick_to_ms(uint32_t tick) const {
    return ((tick >> 24) & 0xFF) * 3600000 + ((tick >> 16) & 0xFF) * 60000 +
           ((tick >> 8) & 0xFF) * 1000 + (tick & 0xFF) * 10;
  }
#endif

  //======================================================================================
  // TRADING SESSION STATE (交易时段状态管理)
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
    price_levels_.for_each([](const Price &price, Level *const &level) {
      for (Order *order : level->orders) {
        if (order->flags == OrderFlags::CALL_AUCTION) {
#if DEBUG_ORDER_FLAGS_RESOLVE
          print_order_flags_resolve(order->id, price, price, order->qty, order->qty, OrderFlags::CALL_AUCTION, OrderFlags::NORMAL, "FLUSH_930 ");
#else
          (void)price;
#endif
          order->flags = OrderFlags::NORMAL;
        }
      }
    });
    // TOB will be updated by first continuous trading order
  }

  //======================================================================================
  // ORDER OPERATIONS (订单生命周期管理)
  //======================================================================================

  // Helper: Extract operation parameters (delta_qty and target_id) atomically
  // Sets: delta_qty_ (signed quantity change) and target_id_ (target order ID)
  // NOTE: Uses cached is_maker_/is_taker_/is_cancel_ and is_bid_ from process()
  HOT_INLINE void order_extract_params(const L2::Order &order) {
    if (is_maker_) {
      delta_qty_ = is_bid_ ? +order.volume : -order.volume;
      target_id_ = is_bid_ ? order.bid_order_id : order.ask_order_id;
    } else if (is_cancel_) {
      delta_qty_ = is_bid_ ? -order.volume : +order.volume;
      target_id_ = is_bid_ ? order.bid_order_id : order.ask_order_id;
    } else if (is_taker_) {
      // For TAKER, determine maker side based on which ID is smaller (earlier order)
      if (order.bid_order_id != 0 && order.ask_order_id != 0) {
        const bool target_is_bid = (order.bid_order_id < order.ask_order_id);
        delta_qty_ = target_is_bid ? -order.volume : +order.volume;
        target_id_ = target_is_bid ? order.bid_order_id : order.ask_order_id;
      } else {
        const bool target_is_bid = (order.bid_order_id != 0);
        delta_qty_ = target_is_bid ? -order.volume : +order.volume;
        target_id_ = target_is_bid ? order.bid_order_id : order.ask_order_id;
      }
    } else {
      delta_qty_ = 0;
      target_id_ = 0;
    }
  }

  // Core: Upsert order (update existing or insert new)
  // This is the heart of the LOB reconstruction engine
  //
  // Parameters:
  //   order_id        - Unique order identifier
  //   price           - Price level for this order
  //   quantity_delta  - Signed quantity change (+add, -deduct)
  //   loc             - Location pointer from order_lookup (from prior find operation)
  //   flags           - Order state flags (NORMAL, OUT_OF_ORDER, etc.)
  //   level_hint      - Optional pre-fetched level pointer (optimization)
  //
  // Returns: true if order was fully consumed (removed), false otherwise
  //
  // Optimization: Pass level_hint to avoid redundant hash lookups when caller
  //               already knows the target level
  HOT_NOINLINE bool order_upsert(
      OrderId order_id,
      Price price,
      Quantity quantity_delta,
      Location *loc,
      OrderFlags flags = OrderFlags::NORMAL,
      Level *level_hint = nullptr) {

    if (loc != nullptr) [[likely]] {
      // ORDER EXISTS - Update existing order (HOT PATH)
      Level *level = loc->level;
      size_t order_index = loc->index;
      Order *order = level->orders[order_index];

      // Apply quantity delta
      const Quantity old_qty = order->qty;
      const Quantity new_qty = old_qty + quantity_delta;

      if (new_qty == 0) [[unlikely]] {
        // FULLY CONSUMED - Remove order completely (COLD PATH)
#if DEBUG_ORDER_FLAGS_RESOLVE
        if (order->flags != OrderFlags::NORMAL) {
          print_order_flags_resolve(order_id, price, price, old_qty, 0, order->flags, OrderFlags::NORMAL, "CONSUME   ");
        }
#endif

        // Update feature all_volume (incremental) before removal
        const uint32_t abs_qty = std::abs(old_qty);
        LOB_feature_.all_bid_volume -= (old_qty > 0) ? abs_qty : 0;
        LOB_feature_.all_ask_volume -= (old_qty < 0) ? abs_qty : 0;

        // Record visibility state BEFORE removal
        const bool was_visible = level->has_visible_quantity();

        // Remove order from level (BumpPool doesn't need individual deallocation)
        level->remove(order_index);
        order_lookup_.erase(order_id);
        // order_memory_pool_.deallocate(order); // No-op for BumpPool, memory reclaimed at EOD clear()

        // Fix up order_lookup_ index for moved order (swap-and-pop side effect)
        if (order_index < level->orders.size()) {
          Location *moved_loc = order_lookup_.find(level->orders[order_index]->id);
          if (moved_loc != nullptr) {
            moved_loc->index = order_index;
          }
        }

        // Cleanup: Remove empty level or update visibility
        if (level->empty()) [[unlikely]] {
          level_remove(level, was_visible);
        } else {
          // Visibility changed: was_visible (before) → has_visible_quantity() (after)
          if (was_visible != level->has_visible_quantity()) [[unlikely]] {
            visibility_update_from_level(level);
          }
        }

        return true; // Fully consumed
      } else {
        // PARTIALLY CONSUMED - Update order quantity (HOT PATH)
        const bool was_visible = level->has_visible_quantity();
        level->net_quantity += quantity_delta;
        order->qty = new_qty;

        // Update feature all_volume (incremental) - apply delta
        const uint32_t abs_delta = std::abs(quantity_delta);
        LOB_feature_.all_bid_volume += (quantity_delta > 0) ? abs_delta : 0;
        LOB_feature_.all_ask_volume += (quantity_delta < 0) ? abs_delta : 0;

        // Update flags if needed (rare)
        if ((flags != OrderFlags::NORMAL || order->flags != OrderFlags::NORMAL)) [[unlikely]] {
#if DEBUG_ORDER_FLAGS_RESOLVE
          [[maybe_unused]] OrderFlags old_flags = order->flags;
          if (old_flags != flags) {
            print_order_flags_resolve(order_id, price, price, old_qty, new_qty, old_flags, flags, "UPDATE_FLG");
          }
#endif
          order->flags = flags;
        }

        // Update visibility only if it changed (rare)
        if (was_visible != level->has_visible_quantity()) [[unlikely]] {
          visibility_update_from_level(level);
        }
        return false; // Partially consumed
      }

    } else {
      // ORDER DOESN'T EXIST - Create new order (LESS FREQUENT)
      const uint32_t ts = DEBUG_ANOMALY_PRINT ? curr_tick_ : 0;
      Order *new_order = order_memory_pool_.construct(quantity_delta, order_id, ts, flags);
      if (!new_order) [[unlikely]]
        return false;

      // Get level: use hint if provided (optimization), otherwise get or create atomically
      Level *level = level_hint ? level_hint : level_get_or_create(price);

      // Add order to level and register in lookup table
      size_t order_index = level->orders.size();
      level->add(new_order);
      order_lookup_.try_emplace(order_id, Location{level, order_index});

      // Update feature all_volume (incremental) for new order
      const uint32_t abs_delta = std::abs(quantity_delta);
      LOB_feature_.all_bid_volume += (quantity_delta > 0) ? abs_delta : 0;
      LOB_feature_.all_ask_volume += (quantity_delta < 0) ? abs_delta : 0;

      // Update visibility if level just became visible
      if (quantity_delta != 0 && !visibility_is_visible(level->price)) [[likely]] {
        visibility_mark_visible(level->price);
      }

#if DEBUG_ORDER_FLAGS_CREATE
      print_order_flags_create(order_id, price, quantity_delta, flags);
#endif

      return false; // New order created
    }
  }

  // Move: Relocate order from one price level to another (using location pointer)
  // Optimization: Accepts location pointer to avoid redundant hash lookup
  HOT_NOINLINE void order_move_to_price(
      Location *loc,
      Price new_price) {
    Level *old_level = loc->level;
    size_t old_index = loc->index;
    Price old_price = old_level->price;

    if (old_price == new_price)
      return; // Already at correct level

    Order *order = old_level->orders[old_index];

    // Step 1: Remove from old level (swap-and-pop)
    old_level->remove(old_index);

    // Step 2: Fix up order_lookup_ for swapped order (swap-and-pop side effect)
    if (old_index < old_level->orders.size()) {
      Location *swapped_loc = order_lookup_.find(old_level->orders[old_index]->id);
      if (swapped_loc != nullptr) {
        swapped_loc->index = old_index;
      }
    }

    // Step 3: Add to new level (get or create atomically)
    Level *new_level = level_get_or_create(new_price);

    size_t new_index = new_level->orders.size();
    new_level->add(order);

    // Step 4: Update order_lookup_ to point to new location
    loc->level = new_level;
    loc->index = new_index;

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
    Location *loc = order_lookup_.find(order_id);
    if (loc == nullptr)
      return; // Order not found

    order_move_to_price(loc, new_price);
  }

  //======================================================================================
  // TOB MANAGEMENT (盘口管理)
  //======================================================================================

  // Bootstrap TOB from visible prices (lazy initialization)
  HOT_INLINE void init_tob() const {
    if (!tob_dirty_)
      return;

    if (best_bid_ == 0 && best_ask_ == 0) {
      // Find max visible price (best bid candidate)
      best_bid_ = next_bid_below(PRICE_RANGE_SIZE - 1);
      // Find min visible price (best ask candidate)
      best_ask_ = next_ask_above(0);
    }

    tob_dirty_ = false;
  }

  // Update one side of TOB after trade
  HOT_INLINE void update_tob_one_side(bool is_active_bid, bool was_fully_consumed, Price trade_price) {
    // Update tick-by-tick TOB
    if (was_fully_consumed) {
      // Level emptied - advance to next level
      if (is_active_bid) {
        best_ask_ = next_ask_above(trade_price);
      } else {
        best_bid_ = next_bid_below(trade_price);
      }
    } else {
      // Level partially filled - TOB stays at trade_price
      if (is_active_bid) {
        best_ask_ = trade_price;
      } else {
        best_bid_ = trade_price;
      }
    }
    tob_dirty_ = false;
  }

  //======================================================================================
  // HIGH-LEVEL PROCESSING (高层处理逻辑)
  //======================================================================================

  // Helper: Process one taker side (for bilateral or unilateral)
  // Returns: true if order was fully consumed
  HOT_INLINE bool process_taker_side(
      const L2::Order &order,
      OrderId order_id,
      Quantity delta) {

    Location *loc = order_lookup_.find(order_id);
    const bool found = (loc != nullptr);

    // FAST PATH: found && correct price && not in call auction
    if (found && !in_call_auction_) [[likely]] {
      Level *level = loc->level;
      if (level->price == order.price) [[likely]] {
        actual_price_ = order.price;
        return order_upsert(order_id, order.price, delta, loc, OrderFlags::NORMAL, level);
      }
    }

    // DEFERRED PATH: Handle corner cases
    target_id_ = order_id;
    delta_qty_ = delta;
    return update_lob_deferred(order, loc, found, in_call_auction_, in_matching_period_);
  }

  // Main LOB update logic (dispatches to MAKER/TAKER/CANCEL)
  HOT_NOINLINE bool update_lob(const L2::Order &order) {
#if DEBUG_ANOMALY_PRINT
    debug_.last_order = &order;
#endif

    //====================================================================================
    // MAKER: Always unilateral, never enters loop
    //====================================================================================
    if (is_maker_) {
      order_extract_params(order);
      if (delta_qty_ == 0 || target_id_ == 0) [[unlikely]]
        return false;

      // FAST PATH: Normal maker with price
      if (!in_call_auction_ && order.price != 0) [[likely]] {
        Location *loc = order_lookup_.find(target_id_);
        if (loc == nullptr) [[likely]] {
          actual_price_ = order.price;
          order_upsert(target_id_, actual_price_, delta_qty_, loc, OrderFlags::NORMAL);
          return true;
        }
      }

      // DEFERRED PATH: Special cases (out-of-order, call auction, price=0)
      Location *loc = order_lookup_.find(target_id_);
      return update_lob_deferred(order, loc, loc != nullptr, in_call_auction_, in_matching_period_);
    }

    //====================================================================================
    // TAKER/CANCEL: May be bilateral or unilateral
    //====================================================================================
    const bool need_bilateral = (exchange_type_ == ExchangeType::SZSE) || (exchange_type_ == ExchangeType::SSE && in_matching_period_);
    const bool is_bilateral = is_taker_ && need_bilateral && order.bid_order_id != 0 && order.ask_order_id != 0;

    if (is_bilateral) {
      //==================================================================================
      // BILATERAL TAKER: Process both bid and ask sides (symmetrically)
      //==================================================================================
      const bool is_active_bid = (order.bid_order_id > order.ask_order_id);

      bool consumed_bid = process_taker_side(order, order.bid_order_id, -static_cast<Quantity>(order.volume));
      bool consumed_ask = process_taker_side(order, order.ask_order_id, +static_cast<Quantity>(order.volume));

      actual_price_ = order.price;
      update_tob_one_side(is_active_bid, is_active_bid ? consumed_ask : consumed_bid, actual_price_);
      return true;

    } else {
      //==================================================================================
      // UNILATERAL TAKER/CANCEL: Single target
      //==================================================================================
      order_extract_params(order);
      if (delta_qty_ == 0 || target_id_ == 0) [[unlikely]]
        return false;

      bool consumed = process_taker_side(order, target_id_, delta_qty_);

      if (is_taker_) {
        update_tob_one_side(is_bid_, consumed, actual_price_);
      }
      return true;
    }
  };

  // Slow path: handle corner cases (out-of-order, call auction, special prices)
  // NOTE: Uses cached is_maker_/is_taker_/is_cancel_ and is_bid_ from process()
  [[gnu::cold]] [[gnu::noinline]] bool update_lob_deferred(
      const L2::Order &order,
      Location *loc,
      bool found,
      bool in_call_auction,
      bool in_matching_period) {

    //====================================================================================
    // MAKER ORDER
    //====================================================================================
    if (is_maker_) {
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
        Price existing_price = loc->level->price;
        if (existing_price != placement_price) {
          order_move_to_price(loc, placement_price);
        }
        order_upsert(target_id_, placement_price, delta_qty_, loc, flags);
      } else {
        // Create new order
        order_upsert(target_id_, placement_price, delta_qty_, loc, flags);
      }

      return true;
    }

    //====================================================================================
    // TAKER ORDER
    //====================================================================================
    if (is_taker_) {
      const OrderId self_id = is_bid_ ? order.bid_order_id : order.ask_order_id;
      const bool both_ids_present = (order.bid_order_id != 0 && order.ask_order_id != 0);

      // Handle target order (counterparty)
      bool was_fully_consumed = false;
      if (found) {
        Price target_price = loc->level->price;

        if (target_price != order.price) {
          OrderFlags anomaly_flag = OrderFlags::NORMAL;
          if (!in_call_auction && !in_matching_period) {
            anomaly_flag = OrderFlags::ANOMALY_MATCH;
          }

          order_move_to_price(loc, order.price);

          if (anomaly_flag == OrderFlags::ANOMALY_MATCH) {
            Order *target_order = loc->level->orders[loc->index];
            target_order->flags = anomaly_flag;
          }
        }

        actual_price_ = order.price;
        was_fully_consumed = order_upsert(target_id_, actual_price_, delta_qty_, loc);
      } else {
        // OUT_OF_ORDER: create placeholder
        actual_price_ = order.price;
        order_upsert(target_id_, actual_price_, delta_qty_, loc, OrderFlags::OUT_OF_ORDER);
        was_fully_consumed = false;
      }

      // Handle self order (market orders from Level[0])
      const bool need_self_order = (self_id != 0 && self_id != target_id_) && !both_ids_present;

      if (need_self_order) {
        Location *self_loc = order_lookup_.find(self_id);
        if (self_loc != nullptr) {
          Order *self_order = self_loc->level->orders[self_loc->index];

          if (self_order->flags == OrderFlags::SPECIAL_MAKER) {
            Price self_price = self_loc->level->price;
            if (self_price != order.price) {
              order_move_to_price(self_loc, order.price);
            }
          }

          order_upsert(self_id, order.price, -delta_qty_, self_loc);
        }
      }

      return was_fully_consumed;
    }

    //====================================================================================
    // CANCEL ORDER
    //====================================================================================
    if (is_cancel_) {
      if (found) {
        Price self_price = loc->level->price;

        // Migrate from Level[0] if CANCEL has price
        if (self_price == 0 && order.price != 0) {
          order_move_to_price(loc, order.price);
          self_price = order.price;
        }

        order_upsert(target_id_, self_price, delta_qty_, loc);
      } else {
        // OUT_OF_ORDER or ZERO_PRICE: create placeholder
        Price placement_price = (order.price == 0) ? 0 : order.price;
        OrderFlags flags = (order.price == 0) ? OrderFlags::ZERO_PRICE : OrderFlags::OUT_OF_ORDER;
        order_upsert(target_id_, placement_price, delta_qty_, loc, flags);
      }

      return true;
    }

    return false;
  };

  //======================================================================================
  // DEPTH BUFFER MANAGEMENT (N档深度缓冲区管理 - 订单+时间双驱动)
  //======================================================================================

  // Binary search price in entire depth_buffer (descending order)
  HOT_INLINE size_t depth_binary_search(Price price) const {
    size_t left = 0, right = depth_buffer_.size();
    while (left < right) {
      size_t mid = left + (right - left) / 2;
      if (depth_buffer_[mid]->price > price) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }
    return left;
  }

  // Find N visible levels beyond boundary price using bitmap
  // is_ask_side: true = ask side (higher prices), false = bid side (lower prices)
  // boundary_price: starting price (exclusive)
  // count: number of levels to find
  // Returns: vector of level pointers (may be < count if not enough levels available)
  HOT_INLINE std::vector<Level *> depth_find_levels_beyond(bool is_ask_side, Price boundary_price, size_t count) const {
    std::vector<Level *> levels;
    levels.reserve(count);

    Price current_price = boundary_price;
    for (size_t i = 0; i < count; ++i) {
      Price next_price = is_ask_side ? next_ask_above(current_price) : next_bid_below(current_price);
      if (next_price == 0)
        break;

      Level *next_level = level_find(next_price);
      if (next_level && next_level->has_visible_quantity()) {
        levels.push_back(next_level);
        current_price = next_price;
      } else {
        break;
      }
    }

    return levels;
  }

  // Order-driven: Add level to depth buffer
  HOT_INLINE void depth_on_level_add(Level *level) {
    if (level->price == 0) [[unlikely]]
      return;

    if (depth_buffer_.empty()) [[unlikely]] {
      depth_buffer_.push_back(level);
      return;
    }

    // Check if price is within current range
    Price high_price = depth_buffer_.front()->price;
    Price low_price = depth_buffer_.back()->price;
    if (level->price >= high_price || low_price <= level->price)
      return;

    // Binary search to find insert position
    size_t idx = depth_binary_search(level->price);

    // Just insert, CBuffer will auto-pop front when full
    depth_buffer_.insert(idx, level);
  }

  // Order-driven: Remove level from depth buffer
  HOT_INLINE void depth_on_level_remove(Level *level) {
    if (level->price == 0 || depth_buffer_.empty()) [[unlikely]]
      return;

    // Find level in buffer
    size_t idx = depth_binary_search(level->price);
    if (idx >= depth_buffer_.size() || depth_buffer_[idx] != level)
      return;

    // Determine which side
    bool is_ask_side = (idx < depth_buffer_.size() / 2);

    // Remove level
    depth_buffer_.erase(idx);

    // Refill from bitmap
    if (is_ask_side && !depth_buffer_.empty()) {
      Price boundary = depth_buffer_.front()->price;
      auto new_levels = depth_find_levels_beyond(true, boundary, 1);
      if (!new_levels.empty()) {
        depth_buffer_.push_front(new_levels[0]);
      }
    } else if (!is_ask_side && !depth_buffer_.empty()) {
      Price boundary = depth_buffer_.back()->price;
      auto new_levels = depth_find_levels_beyond(false, boundary, 1);
      if (!new_levels.empty()) {
        depth_buffer_.push_back(new_levels[0]);
      }
    }
  }

  //======================================================================================
  // FEATURE UPDATES (特征更新 - 时间驱动)
  //======================================================================================

  // Update depth if TOB is valid (called from process() when time interval reached)
  HOT_NOINLINE void sync_tob_to_depth_center() {
    init_tob();

    Level *bid_level = level_find(best_bid_);
    Level *ask_level = level_find(best_ask_);

    // Check conditions: TOB valid AND buffer ready
    if (!(best_bid_ < best_ask_ && bid_level && bid_level->net_quantity > 0 &&
          ask_level && ask_level->net_quantity < 0 && depth_buffer_.size() == 2 * DEPTH_N)) {
      return; // Wait for next update
    }

    // Use best_bid as anchor: should be at index N
    size_t bid_idx = depth_binary_search(best_bid_);
    int shift = static_cast<int>(bid_idx) - static_cast<int>(DEPTH_N);

    for (int i = 0; i < abs(shift); ++i) {
      (shift > 0) ? depth_buffer_.pop_front() : depth_buffer_.pop_back();
      Price boundary = (shift > 0) ? (depth_buffer_.back() ? depth_buffer_.back()->price : 0)
                                   : (depth_buffer_.front() ? depth_buffer_.front()->price : 0);
      Price next = (shift > 0) ? next_bid_below(boundary - 1) : next_ask_above(boundary + 1);
      Level *level = (next > 0) ? level_find(next) : nullptr;
      (shift > 0) ? depth_buffer_.push_back(level && level->has_visible_quantity() ? level : nullptr)
                  : depth_buffer_.push_front(level && level->has_visible_quantity() ? level : nullptr);
    }

    // Sync depth_buffer_ to LOB_feature_ arrays for serialization
    constexpr size_t N = L2::LOB_FEATURE_DEPTH_LEVELS;

    // Ask side: depth_buffer_[N-1...0] → LOB_feature_.ask_*[0...N-1]
    for (size_t i = 0; i < N; ++i) {
      if (DEPTH_N > i) {
        const size_t buf_idx = DEPTH_N - 1 - i;
        if (buf_idx < depth_buffer_.size() && depth_buffer_[buf_idx]) {
          LOB_feature_.ask_price_ticks[i] = depth_buffer_[buf_idx]->price;
          LOB_feature_.ask_volumes[i] = depth_buffer_[buf_idx]->net_quantity;
        } else {
          LOB_feature_.ask_price_ticks[i] = 0;
          LOB_feature_.ask_volumes[i] = 0;
        }
      } else {
        LOB_feature_.ask_price_ticks[i] = 0;
        LOB_feature_.ask_volumes[i] = 0;
      }
    }

    // Bid side: depth_buffer_[N...2N-1] → LOB_feature_.bid_*[0...N-1]
    for (size_t i = 0; i < N; ++i) {
      const size_t buf_idx = DEPTH_N + i;
      if (buf_idx < depth_buffer_.size() && depth_buffer_[buf_idx]) {
        LOB_feature_.bid_price_ticks[i] = depth_buffer_[buf_idx]->price;
        LOB_feature_.bid_volumes[i] = depth_buffer_[buf_idx]->net_quantity;
      } else {
        LOB_feature_.bid_price_ticks[i] = 0;
        LOB_feature_.bid_volumes[i] = 0;
      }
    }

    // Mark update time
    last_depth_update_tick_ = curr_tick_;
    next_depth_update_tick_ = curr_tick_ + (EffectiveTOBFilter::MIN_TIME_INTERVAL_MS / 10);

#if DEBUG_BOOK_PRINT
    print_book();
#endif
  }

  //======================================================================================
  // DEBUG UTILITIES (调试工具)
  //======================================================================================

  // Helper: Get flags string for debug output
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
  // Debug state storage
  struct DebugState {
    const L2::Order *last_order = nullptr;
    std::unordered_set<Price> printed_anomalies;
  };
  mutable DebugState debug_;

  // Check for sign anomaly in level (print far anomalies N+ ticks from TOB during continuous trading)
  void check_anomaly(Level *level) const {
    using namespace TradingSession;
    using namespace AnomalyDetection;

    // Skip level 0 (special level)
    if (level->price == 0)
      return;

    init_tob();

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
                << " Age=" << (tick_to_ms(curr_tick_) - tick_to_ms(order->timestamp)) << "ms\033[0m\n";
    }
  }

#endif // DEBUG_ANOMALY_PRINT

  //======================================================================================
  // DEBUG: Book Display
  //======================================================================================

#if DEBUG_BOOK_PRINT

  // Display current market depth
  void inline print_book() const {
    // Only print during continuous trading
    if (!in_continuous_trading_)
      return;

    // Highlight time if > 10 seconds since last depth update
    const bool highlight_time = (last_depth_update_tick_ > 0) && (tick_to_ms(curr_tick_) - tick_to_ms(last_depth_update_tick_) > 10000);

    std::ostringstream book_output;
    if (highlight_time) {
      book_output << "\033[33m[" << format_time() << "]\033[0m"; // Yellow
    } else {
      book_output << "[" << format_time() << "]";
    }
    Level *level0 = level_find(0);
    book_output << " [" << std::setfill('0') << std::setw(3)
                << (level0 ? level0->order_count : 0) << std::setfill(' ') << "] ";

    using namespace BookDisplay;
    constexpr size_t display_levels = std::min(MAX_DISPLAY_LEVELS, L2::LOB_FEATURE_DEPTH_LEVELS);

    // Fill empty space on the left if display_levels < MAX_DISPLAY_LEVELS
    for (size_t i = 0; i < MAX_DISPLAY_LEVELS - display_levels; ++i) {
      book_output << std::setw(LEVEL_WIDTH) << " ";
    }

    // Display ask levels (left side, from LOB_feature_)
    // Reverse display: show ask[display_levels-1] ... ask[1] ask[0]
    for (int i = display_levels - 1; i >= 0; --i) {
      const Price price = LOB_feature_.ask_price_ticks[i];
      const int32_t volume = LOB_feature_.ask_volumes[i];

      if (price == 0) {
        book_output << std::setw(LEVEL_WIDTH) << " ";
        continue;
      }

      const int32_t display_volume = -volume;       // Negate: normal negative -> positive for display
      const bool is_anomaly = (display_volume < 0); // Any negative display value is anomaly

#if DEBUG_BOOK_AS_AMOUNT == 0
      const std::string qty_str = std::to_string(display_volume);
#else
      const double amount = display_volume * price / (DEBUG_BOOK_AS_AMOUNT * 10000.0);
      const std::string qty_str = (display_volume < 0 ? "-" : "") + std::to_string(static_cast<int>(std::abs(amount) + 0.5));
#endif
      const std::string level_str = std::to_string(price) + "x" + qty_str;

      if (is_anomaly) {
        book_output << "\033[31m" << std::setw(LEVEL_WIDTH) << std::left << level_str << "\033[0m";
      } else {
        book_output << std::setw(LEVEL_WIDTH) << std::left << level_str;
      }
    }

    // Display header with ASK and BID labels
    book_output << " (" << std::setw(4) << best_ask_ << ")ASK | BID("
                << std::setw(4) << best_bid_ << ") ";

    // Display bid levels (right side, from LOB_feature_)
    for (size_t i = 0; i < display_levels; ++i) {
      const Price price = LOB_feature_.bid_price_ticks[i];
      const int32_t volume = LOB_feature_.bid_volumes[i];

      if (price == 0) {
        book_output << std::setw(LEVEL_WIDTH) << " ";
        continue;
      }

      const int32_t display_volume = volume;        // Bid displays as-is (should be positive)
      const bool is_anomaly = (display_volume < 0); // Any negative display value is anomaly

#if DEBUG_BOOK_AS_AMOUNT == 0
      const std::string qty_str = std::to_string(display_volume);
#else
      const double amount = display_volume * price / (DEBUG_BOOK_AS_AMOUNT * 10000.0);
      const std::string qty_str = (display_volume < 0 ? "-" : "") + std::to_string(static_cast<int>(std::abs(amount) + 0.5));
#endif
      const std::string level_str = std::to_string(price) + "x" + qty_str;

      if (is_anomaly) {
        book_output << "\033[31m" << std::setw(LEVEL_WIDTH) << std::left << level_str << "\033[0m";
      } else {
        book_output << std::setw(LEVEL_WIDTH) << std::left << level_str;
      }
    }

    // Fill empty space on the right if display_levels < MAX_DISPLAY_LEVELS
    for (size_t i = 0; i < MAX_DISPLAY_LEVELS - display_levels; ++i) {
      book_output << std::setw(LEVEL_WIDTH) << " ";
    }

    // Count anomalies: wrong sign on non-TOB levels (excluding level 0)
    size_t anomaly_count = 0;
    visible_price_bitmap_.for_each_set([&](size_t price_idx) {
      Price price = static_cast<Price>(price_idx);

      // Skip level 0 (special level)
      if (price == 0)
        return;

      // Skip TOB area
      if (price >= best_bid_ && price <= best_ask_)
        return;

      const Level *level = level_find(price);
      if (!level || !level->has_visible_quantity())
        return;

      // BID side (< best_bid) should be positive, ASK side (> best_ask) should be negative
      if ((price < best_bid_ && level->net_quantity < 0) || (price > best_ask_ && level->net_quantity > 0)) {
        ++anomaly_count;
      }
    });

    if (anomaly_count > 0) {
      book_output << " \033[31m[" << anomaly_count << " anomalies]\033[0m";
    }

    std::cout << book_output.str() << "\n";

#if DEBUG_ANOMALY_PRINT
    // Check anomalies for ALL visible levels (proactive detection, excluding level 0)
    visible_price_bitmap_.for_each_set([&](size_t price_idx) {
      Price price = static_cast<Price>(price_idx);
      if (price == 0)
        return; // Skip level 0 (special level)
      Level *level = level_find(price);
      if (level && level->has_visible_quantity()) {
        check_anomaly(level);
      }
    });
#endif
  }
#endif // DEBUG_BOOK_PRINT
};
