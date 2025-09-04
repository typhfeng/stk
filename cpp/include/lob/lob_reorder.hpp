#pragma once

#include "../codec/L2_DataType.hpp"
#include "mem_pool.hpp"
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <memory_resource>
#include <numeric>
#include <sstream>
#include <unordered_map>
#include <vector>

namespace lob {

// Performance configuration parameters
namespace Config {
// Hash table optimization for millions of orders
static constexpr size_t EXPECTED_TOTAL_ORDERS = 100000; // Peak active orders (1M)
static constexpr size_t EXPECTED_PRICE_LEVELS = 100;    // Expected bid/ask levels per side
static constexpr size_t EXPECTED_QUEUE_ORDERS = 64;     // Initial reservation per level

static constexpr size_t HASH_TABLE_BUCKETS = 33554432; // 32M buckets for ultra-low collision rate
static constexpr float HASH_LOAD_FACTOR = 0.3f;        // Keep load factor low for speed
} // namespace Config

// Core types and constants

using Price = uint16_t;
using Quantity = uint16_t;
using OrderId = uint32_t;

namespace OrderType {
constexpr uint8_t MAKER = 0;
constexpr uint8_t CANCEL = 1;
constexpr uint8_t TAKER = 3;
} // namespace OrderType

namespace OrderDirection {
constexpr uint8_t BID = 0;
constexpr uint8_t ASK = 1;
} // namespace OrderDirection

// Data structures

// Core order book entry - optimized for cache efficiency (price stored in PriceLevel)
struct alignas(8) OrderEntry {
  Quantity quantity;
  OrderId maker_id;

  OrderEntry() = default;
  OrderEntry(Quantity q, OrderId id) : quantity(q), maker_id(id) {}
};

// Price level with efficient vector storage
struct PriceLevel {
  Price price;
  std::vector<OrderEntry *> orders; // Use pointers to pool-allocated entries

  PriceLevel() = default;
  explicit PriceLevel(Price p) : price(p) {
    orders.reserve(Config::EXPECTED_QUEUE_ORDERS);
  }
};

// Fast order location tracking for O(1) operations
struct OrderLocation {
  Price price_level;
  OrderEntry *order_ptr; // Direct pointer to pool-allocated entry
  size_t level_index;    // Index in bid/ask levels vector
  size_t order_index;    // Index within price level orders vector
  bool is_bid;

  OrderLocation(Price price, OrderEntry *ptr, size_t lvl_idx, size_t ord_idx, bool bid)
      : price_level(price), order_ptr(ptr), level_index(lvl_idx), order_index(ord_idx), is_bid(bid) {}
};

class LimitOrderBook {
private:
  // Core data members

  // Vector-based storage for better cache efficiency than std::map
  std::vector<PriceLevel> bid_levels; // Sorted highest to lowest (bid priority)
  std::vector<PriceLevel> ask_levels; // Sorted lowest to highest (ask priority)

  /*
  order pool和lookup pool都需要高频访问
  功能层面:
    1. order不需要erase(只用swap+pop), lookup必须支持erase
    2. order会被反复读写剩余量, 需要cache perf, 指针需要稳定(refered by PriceLevel)
    3. lookup主要是hash探测, 内部值不会被反复访问, 而且存的类型是值, 不需要指针稳定, 也不需要自定义内部存储方式
  因此:
    1. 对于order, 我们建立自定义高性能mem pool
    2. 对于lookup, 我们采用带池化的普通mem pool
  */

  // High-performance hash table with optimal configuration for millions of orders
  std::pmr::unsynchronized_pool_resource lookup_pool_;
  std::pmr::unordered_map<OrderId, OrderLocation> order_lookup;

  // Memory pool for zero-allocation order processing
  mem_pool::MemoryPool<OrderEntry> entry_pool_;

  // Multi-level caching for ultra-fast market data access
  mutable Price cached_best_bid = 0;
  mutable Price cached_best_ask = 0;
  mutable bool bid_cache_valid = false;
  mutable bool ask_cache_valid = false;

  // Cache total quantities at best levels for spread calculations
  mutable Quantity cached_best_bid_qty = 0;
  mutable Quantity cached_best_ask_qty = 0;
  mutable bool bid_qty_cache_valid = false;
  mutable bool ask_qty_cache_valid = false;

  // Track previous best prices for change detection
  mutable Price prev_best_bid = 0;
  mutable Price prev_best_ask = 0;

  // Current order time tracking for market depth display
  uint8_t current_hour = 0;
  uint8_t current_minute = 0;
  uint8_t current_second = 0;
  uint8_t current_millisecond = 0;

public:
  // Constructor

  LimitOrderBook() : order_lookup(&lookup_pool_), entry_pool_(Config::EXPECTED_TOTAL_ORDERS) {
    // Pre-allocate containers
    bid_levels.reserve(Config::EXPECTED_PRICE_LEVELS);
    ask_levels.reserve(Config::EXPECTED_PRICE_LEVELS);

    // Configure hash table
    order_lookup.reserve(Config::EXPECTED_TOTAL_ORDERS);
    order_lookup.rehash(Config::HASH_TABLE_BUCKETS);
    order_lookup.max_load_factor(Config::HASH_LOAD_FACTOR);
  }

private:
  // Helper methods

  // Binary search for price level
  size_t find_bid_level_index(Price price) const {
    auto it = std::lower_bound(bid_levels.begin(), bid_levels.end(), price,
                               [](const PriceLevel &level, Price p) { return level.price > p; }); // Bid: higher prices first
    return std::distance(bid_levels.begin(), it);
  }

  size_t find_ask_level_index(Price price) const {
    auto it = std::lower_bound(ask_levels.begin(), ask_levels.end(), price,
                               [](const PriceLevel &level, Price p) { return level.price < p; }); // Ask: lower prices first
    return std::distance(ask_levels.begin(), it);
  }

  // Get or create price level
  std::pair<size_t, bool> get_or_create_bid_level(Price price) {
    size_t index = find_bid_level_index(price);

    if (index < bid_levels.size() && bid_levels[index].price == price) {
      return {index, false}; // Existing level
    }

    // Create new level
    bid_levels.emplace(bid_levels.begin() + index, price);
    bid_cache_valid = false; // Invalidate cache when structure changes
    return {index, true};
  }

  std::pair<size_t, bool> get_or_create_ask_level(Price price) {
    size_t index = find_ask_level_index(price);

    if (index < ask_levels.size() && ask_levels[index].price == price) {
      return {index, false}; // Existing level
    }

    // Create new level
    ask_levels.emplace(ask_levels.begin() + index, price);
    ask_cache_valid = false; // Invalidate cache when structure changes
    return {index, true};
  }

  // Remove empty price level and maintain sorted order
  void remove_empty_bid_level(size_t index) {
    assert(index < bid_levels.size());
    assert(bid_levels[index].orders.empty());

    const bool was_best = (bid_cache_valid && index == 0);
    bid_levels.erase(bid_levels.begin() + index);

    if (was_best) [[unlikely]] {
      bid_cache_valid = false;
      bid_qty_cache_valid = false;
    }
  }

  void remove_empty_ask_level(size_t index) {
    assert(index < ask_levels.size());
    assert(ask_levels[index].orders.empty());

    const bool was_best = (ask_cache_valid && index == 0);
    ask_levels.erase(ask_levels.begin() + index);

    if (was_best) [[unlikely]] {
      ask_cache_valid = false;
      ask_qty_cache_valid = false;
    }
  }

public:
  // Main processing interface
  [[gnu::hot]] bool process_order(const L2::Order &order) {
    // Update current time tracking
    current_hour = order.hour;
    current_minute = order.minute;
    current_second = order.second;
    current_millisecond = order.millisecond;

    std::cout << "Type:" << static_cast<int>(order.order_type) << " Dir:" << static_cast<int>(order.order_dir) << " Price:" << order.price << " Vol:" << order.volume << " Bid:" << order.bid_order_id << " Ask:" << order.ask_order_id << "\n";
    check_and_print_best_level_changes();

    bool result = false;
    switch (order.order_type) {
    case OrderType::MAKER:
      result = add_maker(order);
      break;
    case OrderType::CANCEL:
      result = add_cancel(order);
      break;
    case OrderType::TAKER:
      result = add_taker(order);
      break;
    default:
      [[unlikely]] return false;
    }

    if (result) {
    }

    return result;
  }

  // Market data access with caching

  [[gnu::hot]] Price get_best_bid_price() const {
    if (!bid_cache_valid) [[unlikely]] {
      cached_best_bid = bid_levels.empty() ? 0 : bid_levels[0].price;
      bid_cache_valid = true;
    }
    return cached_best_bid;
  }

  [[gnu::hot]] Price get_best_ask_price() const {
    if (!ask_cache_valid) [[unlikely]] {
      cached_best_ask = ask_levels.empty() ? 0 : ask_levels[0].price;
      ask_cache_valid = true;
    }
    return cached_best_ask;
  }

  // Quantity caching
  Quantity get_best_bid_quantity() const {
    if (!bid_qty_cache_valid) [[unlikely]] {
      cached_best_bid_qty = bid_levels.empty() ? 0 : std::accumulate(bid_levels[0].orders.begin(), bid_levels[0].orders.end(), Quantity{0}, [](Quantity sum, const OrderEntry *order) { return sum + order->quantity; });
      bid_qty_cache_valid = true;
    }
    return cached_best_bid_qty;
  }

  Quantity get_best_ask_quantity() const {
    if (!ask_qty_cache_valid) [[unlikely]] {
      cached_best_ask_qty = ask_levels.empty() ? 0 : std::accumulate(ask_levels[0].orders.begin(), ask_levels[0].orders.end(), Quantity{0}, [](Quantity sum, const OrderEntry *order) { return sum + order->quantity; });
      ask_qty_cache_valid = true;
    }
    return cached_best_ask_qty;
  }

  // Check for best level changes and print market depth if changed
  void check_and_print_best_level_changes() const {
    const Price current_best_bid = get_best_bid_price();
    const Price current_best_ask = get_best_ask_price();

    bool changed = false;
    if (current_best_bid != prev_best_bid) {
      prev_best_bid = current_best_bid;
      changed = true;
    }
    if (current_best_ask != prev_best_ask) {
      prev_best_ask = current_best_ask;
      changed = true;
    }

    if (changed) {
      print_market_depth();
      // print_pool_debug_info();
    }
  }

  // Fast accessors
  size_t get_bid_level_count() const { return bid_levels.size(); }
  size_t get_ask_level_count() const { return ask_levels.size(); }
  size_t get_total_order_count() const { return order_lookup.size(); }

  // Time formatting helper
  std::string format_current_time() const {
    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(2) << static_cast<int>(current_hour) << ":"
        << std::setfill('0') << std::setw(2) << static_cast<int>(current_minute) << ":"
        << std::setfill('0') << std::setw(2) << static_cast<int>(current_second) << "."
        << std::setfill('0') << std::setw(3) << static_cast<int>(current_millisecond * 10);
    return oss.str();
  }

  // Print ±3 levels around best bid/ask in aligned single line
  void print_market_depth() const {
    std::ostringstream ask_stream, bid_stream;

    // Build ASK string with fixed width formatting
    ask_stream << "ASK: ";
    for (int i = std::min(2, static_cast<int>(ask_levels.size()) - 1); i >= 0; --i) {
      const auto &level = ask_levels[i];
      Quantity total_qty = std::accumulate(level.orders.begin(), level.orders.end(), Quantity{0},
                                           [](Quantity sum, const OrderEntry *order) { return sum + order->quantity; });
      ask_stream << std::setw(3) << std::right << level.price << "x" << std::setw(3) << std::left << total_qty;
      if (i > 0)
        ask_stream << " ";
    }

    // Build BID string with fixed width formatting
    bid_stream << "BID: ";
    for (size_t i = 0; i < std::min(3UL, bid_levels.size()); ++i) {
      const auto &level = bid_levels[i];
      Quantity total_qty = std::accumulate(level.orders.begin(), level.orders.end(), Quantity{0},
                                           [](Quantity sum, const OrderEntry *order) { return sum + order->quantity; });
      bid_stream << std::setw(3) << std::right << level.price << "x" << std::setw(3) << std::left << total_qty;
      if (i < std::min(3UL, bid_levels.size()) - 1)
        bid_stream << " ";
    }

    // Print with fixed total width alignment and time
    std::string ask_str = ask_stream.str();
    std::string bid_str = bid_stream.str();
    std::string time_str = format_current_time();

    std::cout << "[" << time_str << "] " << std::setw(35) << std::left << ask_str << " | " << bid_str << "\n";
  }

  // Debug function to print memory pool statistics
  void print_pool_debug_info() const {
    auto pool_stats = entry_pool_.get_memory_stats();
    std::cout << "  [POOL DEBUG] Entry pool: " << pool_stats.total_used << "/" << pool_stats.total_allocated
              << " (" << std::fixed << std::setprecision(1) << pool_stats.utilization_rate * 100 << "%) | "
              << "lookup_ size: " << order_lookup.size() << " | "
              << "lookup_ buckets: " << order_lookup.bucket_count() << " | "
              << "Total levels: " << (bid_levels.size() + ask_levels.size()) << "\n";
  }

  // Performance monitoring
  struct MemoryStats {
    size_t total_orders;
    size_t total_levels;
    size_t hash_table_buckets;
    size_t memory_pool_used;
    size_t estimated_memory_bytes;
  };

  MemoryStats get_memory_stats() const {
    const size_t total_orders = order_lookup.size();
    const size_t total_levels = bid_levels.size() + ask_levels.size();
    const size_t hash_buckets = order_lookup.bucket_count();

    auto pool_stats = entry_pool_.get_memory_stats();
    const size_t pool_used = pool_stats.total_used;

    // Estimate total memory usage
    const size_t order_entry_memory = pool_used * sizeof(OrderEntry);
    const size_t lookup_memory = total_orders * (sizeof(OrderId) + sizeof(OrderLocation)) + hash_buckets * sizeof(void *);
    const size_t level_memory = total_levels * sizeof(PriceLevel);

    return {
        total_orders,
        total_levels,
        hash_buckets,
        pool_used,
        order_entry_memory + lookup_memory + level_memory + pool_stats.memory_overhead_bytes};
  }

  // Management interface

  void clear_all() {
    // Clear order lookup_ first to ensure no dangling references
    order_lookup.clear();

    // Clear price level containers (which contain pointers to pool objects)
    bid_levels.clear();
    ask_levels.clear();

    // Reset memory pool only after all references are cleared
    entry_pool_.reset();

    // Invalidate all caches
    bid_cache_valid = false;
    ask_cache_valid = false;
    bid_qty_cache_valid = false;
    ask_qty_cache_valid = false;

    // Reset previous best prices for change detection
    prev_best_bid = 0;
    prev_best_ask = 0;
  }

private:
  // Order processing implementation

  using LookupResult = std::pair<bool, std::pmr::unordered_map<OrderId, OrderLocation>::const_iterator>;

  // Order validation with hash lookup_
  [[gnu::hot]] LookupResult validate_and_lookup(const L2::Order &order) const {
    const bool is_bid = (order.order_dir == OrderDirection::BID);
    const bool opposing_side = (order.order_type == OrderType::TAKER);
    const OrderId maker_id = extract_maker_id(order, opposing_side);

    // Single hash lookup_ - most critical performance bottleneck
    auto it = order_lookup.find(maker_id);

    // Validate existence rules with branch prediction hints
    const bool should_exist = (order.order_type != OrderType::MAKER);
    const bool does_exist = (it != order_lookup.end());
    if (should_exist != does_exist) [[unlikely]]
      return {false, it};

    // For taker/cancel orders, perform additional validations
    if (should_exist) [[likely]] {
      const OrderLocation &location = it->second;

      // Side consistency check
      if (order.order_type == OrderType::CANCEL) {
        if (location.is_bid != is_bid) [[unlikely]]
          return {false, it};
      } else { // TAKER
        if (location.is_bid == is_bid) [[unlikely]]
          return {false, it};
      }

      // Exact price match validation
      if (order.price != location.price_level) [[unlikely]]
        return {false, it};

      // CANCEL-specific quantity validation using direct pointer access
      if (order.order_type == OrderType::CANCEL) {
        const Quantity remaining_qty = location.order_ptr->quantity;
        if (order.volume < remaining_qty) [[unlikely]]
          return {false, it};
      }
    }

    return {true, it};
  }

  // Core helper functions

  // Order removal with vector-based storage
  [[gnu::hot]] void remove_order_from_level(size_t level_index, size_t order_index, bool is_bid) {
    if (is_bid) {
      assert(level_index < bid_levels.size());
      auto &level = bid_levels[level_index];
      assert(order_index < level.orders.size());

      // Swap with last and pop
      if (order_index != level.orders.size() - 1) {
        std::swap(level.orders[order_index], level.orders.back());

        // Update moved order's location
        OrderEntry *moved_order = level.orders[order_index];
        auto moved_it = order_lookup.find(moved_order->maker_id);
        if (moved_it != order_lookup.end()) [[likely]] {
          moved_it->second.order_index = order_index;
        }
      }
      level.orders.pop_back();

      // Remove empty price level
      if (level.orders.empty()) {
        remove_empty_bid_level(level_index);

        // Update subsequent level indices
        for (auto &pair : order_lookup) {
          auto &loc = pair.second;
          if (loc.is_bid && loc.level_index > level_index) {
            loc.level_index--;
          }
        }
      } else if (level_index == 0) {
        // Best level quantity changed
        bid_qty_cache_valid = false;
      }
    } else {
      assert(level_index < ask_levels.size());
      auto &level = ask_levels[level_index];
      assert(order_index < level.orders.size());

      // Swap with last and pop
      if (order_index != level.orders.size() - 1) {
        std::swap(level.orders[order_index], level.orders.back());

        // Update moved order's location
        OrderEntry *moved_order = level.orders[order_index];
        auto moved_it = order_lookup.find(moved_order->maker_id);
        if (moved_it != order_lookup.end()) [[likely]] {
          moved_it->second.order_index = order_index;
        }
      }
      level.orders.pop_back();

      // Remove empty price level
      if (level.orders.empty()) {
        remove_empty_ask_level(level_index);

        // Update subsequent level indices
        for (auto &pair : order_lookup) {
          auto &loc = pair.second;
          if (!loc.is_bid && loc.level_index > level_index) {
            loc.level_index--;
          }
        }
      } else if (level_index == 0) {
        // Best level quantity changed
        ask_qty_cache_valid = false;
      }
    }
  }

  // Extract maker ID based on order direction and type
  [[gnu::hot]] OrderId extract_maker_id(const L2::Order &order, bool opposing_side = false) const {
    const bool is_bid = (order.order_dir == OrderDirection::BID);

    if (opposing_side) {
      // Taker orders target opposite side makers
      return is_bid ? order.ask_order_id : order.bid_order_id;
    }
    // Maker/Cancel orders use same-side ID
    return is_bid ? order.bid_order_id : order.ask_order_id;
  }

  // Order processing methods

  // Maker order processing
  [[gnu::hot]] bool add_maker(const L2::Order &order) {
    if (order.volume == 0) [[unlikely]]
      return false;

    auto [is_valid, lookup_iter] = validate_and_lookup(order);
    if (!is_valid) [[unlikely]]
      std::cout << "  [ERROR] Invalid order: " << order.price << "\n";
    exit(1);
    return false;

    const bool is_bid = (order.order_dir == OrderDirection::BID);
    const OrderId maker_id = extract_maker_id(order);

    // Allocate order from memory pool
    OrderEntry *new_order = entry_pool_.construct(order.volume, maker_id);
    if (!new_order) [[unlikely]] {
      auto pool_stats = entry_pool_.get_memory_stats();
      std::cout << "  [ERROR] Pool exhausted! Used: " << pool_stats.total_used << "/" << pool_stats.total_allocated << "\n";
      exit(1);
      return false;
    }

    // Get or create price level
    auto [level_index, level_created] = is_bid ? get_or_create_bid_level(order.price) : get_or_create_ask_level(order.price);

    // Add order to price level
    auto &level = is_bid ? bid_levels[level_index] : ask_levels[level_index];
    const size_t order_index = level.orders.size();
    level.orders.push_back(new_order);

    // Store location for lookup_
    order_lookup.emplace(maker_id, OrderLocation(order.price, new_order, level_index, order_index, is_bid));

    // Invalidate relevant caches
    if (is_bid) {
      if (level_index == 0) [[likely]] {
        bid_cache_valid = false;
        bid_qty_cache_valid = false;
      }
    } else {
      if (level_index == 0) [[likely]] {
        ask_cache_valid = false;
        ask_qty_cache_valid = false;
      }
    }

    return true;
  }

  // Order cancellation
  [[gnu::hot]] bool add_cancel(const L2::Order &order) {
    auto [is_valid, lookup_iter] = validate_and_lookup(order);
    if (!is_valid) [[unlikely]]
      return false;

    // Extract location data before erasing to avoid reference invalidation
    const size_t level_index = lookup_iter->second.level_index;
    const size_t order_index = lookup_iter->second.order_index;
    const bool is_bid = lookup_iter->second.is_bid;

    order_lookup.erase(lookup_iter);
    remove_order_from_level(level_index, order_index, is_bid);
    return true;
  }

  // Order matching
  [[gnu::hot]] bool add_taker(const L2::Order &order) {
    auto [is_valid, lookup_iter] = validate_and_lookup(order);
    if (!is_valid) [[unlikely]]
      return false;

    // Access target order
    OrderEntry *target_order = lookup_iter->second.order_ptr;
    const OrderLocation &location = lookup_iter->second;

    if (target_order->quantity <= order.volume) [[likely]] {
      // Complete fill - remove order entirely
      // Extract location data before erasing to avoid reference invalidation
      const size_t level_index = location.level_index;
      const size_t order_index = location.order_index;
      const bool is_bid = location.is_bid;

      order_lookup.erase(lookup_iter);
      remove_order_from_level(level_index, order_index, is_bid);
    } else {
      // Partial fill - update quantity in-place
      target_order->quantity -= order.volume;

      // Invalidate quantity cache if this affects best level
      if (location.level_index == 0) {
        if (location.is_bid) {
          bid_qty_cache_valid = false;
        } else {
          ask_qty_cache_valid = false;
        }
      }
    }

    return true;
  }
};

// Convenience aliases

using LOB = LimitOrderBook;

} // namespace lob
