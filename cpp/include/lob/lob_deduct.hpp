#pragma once

#include "../codec/L2_DataType.hpp"
#include "mem_pool.hpp"
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <deque>
#include <iomanip>
#include <iostream>
#include <memory_resource>
#include <set>
#include <sstream>
#include <unordered_map>
#include <vector>

namespace lob {

// Optimized configuration for ultra-low latency
namespace Config {
static constexpr size_t EXPECTED_LEVEL_NUM = 200;
static constexpr size_t EXPECTED_QUEUE_SIZE = 128;
static constexpr size_t EXPECTED_ORDER_NUM = 100000;

static constexpr size_t CACHE_LINE_SIZE = 64;

static constexpr size_t HASH_BUCKETS = 16777216; // 16M buckets for minimal collisions
static constexpr float HASH_LOAD_FACTOR = 0.2f;  // Ultra-low load factor
} // namespace Config

// Core types
using Price = uint16_t;
using Quantity = int32_t; // Supports negative quantities for deduction model
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

// Ultra-compact order entry - cache-optimized
struct alignas(8) Order {
  Quantity qty;
  OrderId id;

  Order(Quantity q, OrderId i) : qty(q), id(i) {}

  // Fast operations
  bool is_positive() const { return qty > 0; }
  bool is_depleted() const { return qty <= 0; }
  void subtract(Quantity amount) { qty -= amount; }
  void add(Quantity amount) { qty += amount; }
};

// Simple unified price level - no side field needed
struct alignas(Config::CACHE_LINE_SIZE) Level {
  Price price;                         // Price level identifier
  Quantity total_visible_quantity = 0; // Cached sum of positive quantities only
  uint16_t order_count = 0;            // Fast size tracking
  uint16_t alignment_padding = 0;      // Explicit padding for cache line alignment
  std::vector<Order *> orders;         // Pointers to orders at this price level

  explicit Level(Price p) : price(p) {
    orders.reserve(Config::EXPECTED_QUEUE_SIZE);
  }

  // High-performance order management
  [[gnu::hot]] void add(Order *order) {
    orders.push_back(order);
    ++order_count;
    if (order->is_positive()) {
      total_visible_quantity += order->qty;
    }
  }

  [[gnu::hot]] void remove(size_t order_index) {
    assert(order_index < orders.size());
    Order *removed_order = orders[order_index];

    // Update cached total before removal
    if (removed_order->is_positive()) {
      total_visible_quantity -= removed_order->qty;
    }

    // Swap-and-pop for O(1) removal
    if (order_index != orders.size() - 1) {
      orders[order_index] = orders.back();
    }
    orders.pop_back();
    --order_count;
  }

  // Fast level state queries
  bool empty() const { return order_count == 0; }
  bool has_visible_quantity() const { return total_visible_quantity > 0; }

  // Recalculate cached total from scratch
  void refresh_total() {
    total_visible_quantity = 0;
    for (const Order *current_order : orders) {
      if (current_order->is_positive()) {
        total_visible_quantity += current_order->qty;
      }
    }
  }
};

// Order location tracking
struct Location {
  Level *level;
  size_t index;

  Location(Level *l, size_t i) : level(l), index(i) {}
};

/**
 * Simple and Robust LOB with dynamic TOB tracking
 *
 * Key design principles:
 * - Unified price levels without side distinction
 * - Dynamic best_ask_price_/best_bid_price_ tracking
 * - Side judgment by price comparison with TOB
 * - Robust handling of partial out-of-order input
 */
class LOB {
private:
  // ========================================================================================
  // SIMPLE DATA STRUCTURES
  // ========================================================================================

  // Simple unified price level storage
  std::deque<Level> level_storage_;                 // Container for all price levels
  std::unordered_map<Price, Level *> price_levels_; // Simple price -> level mapping
  std::set<Price> visible_prices_;                  // Ordered prices with positive visible quantity

  // Simple TOB tracking - just maintain the key prices
  mutable Price best_bid_price_ = 0, best_ask_price_ = 0;
  mutable bool tob_invalid_ = true;

  // High-performance order lookup infrastructure
  std::pmr::unsynchronized_pool_resource order_lookup_memory_pool_;
  std::pmr::unordered_map<OrderId, Location> order_lookup_;
  mem_pool::MemoryPool<Order> order_memory_pool_;

  // Market timestamp tracking
  uint32_t current_timestamp_ = 0;

public:
  // ========================================================================================
  // CONSTRUCTOR
  // ========================================================================================

  LOB() : order_lookup_(&order_lookup_memory_pool_), order_memory_pool_(Config::EXPECTED_ORDER_NUM) {
    // Configure hash table for minimal collisions
    order_lookup_.reserve(Config::EXPECTED_ORDER_NUM);
    order_lookup_.rehash(Config::HASH_BUCKETS);
    order_lookup_.max_load_factor(Config::HASH_LOAD_FACTOR);
  }

private:
  // ========================================================================================
  // SIMPLE LEVEL MANAGEMENT
  // ========================================================================================

  // Simple price level lookup
  [[gnu::hot]] Level *find_level(Price price) const {
    auto level_iterator = price_levels_.find(price);
    return (level_iterator != price_levels_.end()) ? level_iterator->second : nullptr;
  }

  // Create new price level
  Level *create_level(Price price) {
    level_storage_.emplace_back(price);
    Level *new_level = &level_storage_.back();
    price_levels_[price] = new_level;
    return new_level;
  }

  // Remove empty level
  void remove_level(Level *level_to_remove, bool erase_visible = true) {
    price_levels_.erase(level_to_remove->price);
    if (erase_visible) {
      visible_prices_.erase(level_to_remove->price);
    }
  }

  // Simple TOB update when needed (bootstrap only)
  void update_tob() const {
    if (!tob_invalid_)
      return;

    if (best_bid_price_ == 0 && best_ask_price_ == 0) {
      best_bid_price_ = max_visible_price();
      best_ask_price_ = min_visible_price();
    }

    tob_invalid_ = false;
  }

  // Maintain visible price ordering after any level total change
  void update_visible_price(Level *level) {
    if (level->has_visible_quantity()) {
      visible_prices_.insert(level->price);
    } else {
      visible_prices_.erase(level->price);
    }
  }

  // Find next ask level strictly above a given price with visible quantity
  Price next_ask_above(Price from_price) const {
    auto it = visible_prices_.upper_bound(from_price);
    return it == visible_prices_.end() ? 0 : *it;
  }

  // Find next bid level strictly below a given price with visible quantity
  Price next_bid_below(Price from_price) const {
    auto it = visible_prices_.lower_bound(from_price);
    if (it == visible_prices_.begin())
      return 0;
    if (it == visible_prices_.end() || *it >= from_price) {
      if (it == visible_prices_.begin())
        return 0;
      --it;
      return *it;
    }
    --it;
    return *it;
  }

  // Find minimum price with visible quantity
  Price min_visible_price() const {
    return visible_prices_.empty() ? 0 : *visible_prices_.begin();
  }

  // Find maximum price with visible quantity
  Price max_visible_price() const {
    return visible_prices_.empty() ? 0 : *visible_prices_.rbegin();
  }

  // Dynamic side judgment based on current TOB
  [[gnu::hot]] uint8_t judge_side(Price price) const {
    update_tob(); // Ensure TOB is current

    if (best_bid_price_ == 0 && best_ask_price_ == 0) {
      // No market yet - can't judge, will be determined by order direction
      return OrderDirection::BID; // Default fallback
    }

    if (best_bid_price_ > 0 && price >= best_bid_price_) {
      return OrderDirection::BID;
    }
    if (best_ask_price_ > 0 && price <= best_ask_price_) {
      return OrderDirection::ASK;
    }

    // In between or unknown - closer to which side?
    if (best_bid_price_ > 0 && best_ask_price_ > 0) {
      Price mid = (best_bid_price_ + best_ask_price_) / 2;
      return price >= mid ? OrderDirection::BID : OrderDirection::ASK;
    }

    // Only one side exists
    return best_bid_price_ > 0 ? OrderDirection::BID : OrderDirection::ASK;
  }

  // Deduction type enumeration
  enum class DeductionType : int {
    CANCEL = 0,
    TAKER = 1
  };

  // Simple deduction logic
  template <DeductionType Type>
  [[gnu::hot]] bool deduct(const L2::Order &order) {
    const bool is_bid_order = (order.order_dir == OrderDirection::BID);

    // Determine target order ID
    const OrderId target_order_id = (Type == DeductionType::CANCEL)
                                        ? (is_bid_order ? order.bid_order_id : order.ask_order_id)
                                        : (is_bid_order ? order.ask_order_id : order.bid_order_id);

    auto order_lookup_iterator = order_lookup_.find(target_order_id);

    if (order_lookup_iterator != order_lookup_.end()) {
      // Order exists - perform deduction
      Level *target_level = order_lookup_iterator->second.level;
      size_t order_index = order_lookup_iterator->second.index;
      Order *target_order = target_level->orders[order_index];
      const Price counterparty_price = target_level->price;

      // Apply deduction with O(1) delta update
      const Quantity old_qty = target_order->qty;
      const Quantity dec = static_cast<Quantity>(order.volume);
      const Quantity new_qty = old_qty - dec;
      target_order->qty = new_qty;

      if (new_qty <= 0) {
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

        // On taker events, advance TOB using visible_prices_ iterators
        if constexpr (Type == DeductionType::TAKER) {
          auto it = visible_prices_.find(counterparty_price);
          // Remove the just-emptied price from visible set
          if (it != visible_prices_.end()) {
            auto next_it = it;
            auto prev_it = it;
            bool has_next = (++next_it != visible_prices_.end());
            bool has_prev = (it != visible_prices_.begin() && (--prev_it, true));
            visible_prices_.erase(it);

            if (is_bid_order) {
              // 买方taker：推进 ask 到更高价
              best_ask_price_ = has_next ? *next_it : 0;
            } else {
              // 卖方taker：推进 bid 到更低价
              best_bid_price_ = has_prev ? *prev_it : 0;
            }
          } else {
            // Fallback if the price was not in visible set
            if (is_bid_order) {
              best_ask_price_ = next_ask_above(counterparty_price);
            } else {
              best_bid_price_ = next_bid_below(counterparty_price);
            }
          }

          // Now remove empty level (visible already updated)
          remove_level(target_level, /*erase_visible=*/false);
          tob_invalid_ = false;
        } else {
          // Non-taker path: just cleanup level and maintain visibility
          if (target_level->empty()) {
            remove_level(target_level);
          } else {
            update_visible_price(target_level);
          }
        }
      } else {
        // Partial deduction - delta update totals
        const Quantity visible_before = old_qty > 0 ? old_qty : 0;
        const Quantity visible_after = new_qty > 0 ? new_qty : 0;
        target_level->total_visible_quantity += (visible_after - visible_before);
        update_visible_price(target_level);
        if constexpr (Type == DeductionType::TAKER) {
          if (is_bid_order) {
            best_ask_price_ = counterparty_price;
          } else {
            best_bid_price_ = counterparty_price;
          }
          tob_invalid_ = false;
        }
      }

    } else {
      // Out-of-order processing: create negative quantity placeholder
      Order *negative_order = order_memory_pool_.construct(-static_cast<Quantity>(order.volume), target_order_id);
      if (!negative_order)
        return false;

      // Simply place at the order's price - don't need to guess side
      Level *target_level = find_level(order.price);
      if (!target_level) {
        target_level = create_level(order.price);
      }

      size_t new_order_index = target_level->orders.size();
      target_level->add(negative_order);
      order_lookup_.emplace(target_order_id, Location(target_level, new_order_index));
      update_visible_price(target_level);

      // On taker events with missing maker, use provided price as TOB estimate
      if constexpr (Type == DeductionType::TAKER) {
        if (is_bid_order) {
          best_ask_price_ = order.price;
        } else {
          best_bid_price_ = order.price;
        }
        tob_invalid_ = false;
      }
    }

    return true;
  }

  // Simple maker order processing
  [[gnu::hot]] bool add_maker_fast(const L2::Order &order) {
    if (order.volume == 0) [[unlikely]]
      return false;

    const bool is_bid_order = (order.order_dir == OrderDirection::BID);
    const OrderId order_id = is_bid_order ? order.bid_order_id : order.ask_order_id;

    auto existing_order_lookup = order_lookup_.find(order_id);

    if (existing_order_lookup == order_lookup_.end()) [[likely]] {
      // Fast path: completely new order
      Order *new_order = order_memory_pool_.construct(order.volume, order_id);
      if (!new_order) [[unlikely]]
        return false;

      Level *price_level = find_level(order.price);
      if (!price_level) {
        price_level = create_level(order.price);
      }

      size_t order_index = price_level->orders.size();
      price_level->add(new_order);
      order_lookup_.emplace(order_id, Location(price_level, order_index));
      update_visible_price(price_level);

      // print_book();

      return true;

    } else {
      // Merge with existing negative order (out-of-order scenario)
      Level *existing_level = existing_order_lookup->second.level;
      Order *existing_order = existing_level->orders[existing_order_lookup->second.index];

      // Merge with O(1) delta update
      const Quantity old_qty_merge = existing_order->qty;
      const Quantity inc = static_cast<Quantity>(order.volume);
      existing_order->qty = old_qty_merge + inc;

      if (existing_order->qty == 0) {
        // Order completely cancelled out - remove from book
        existing_level->remove(existing_order_lookup->second.index);
        order_lookup_.erase(existing_order_lookup);

        // Update lookup index for swapped order
        if (existing_order_lookup->second.index < existing_level->orders.size()) {
          auto swapped_order_lookup = order_lookup_.find(existing_level->orders[existing_order_lookup->second.index]->id);
          if (swapped_order_lookup != order_lookup_.end()) {
            swapped_order_lookup->second.index = existing_order_lookup->second.index;
          }
        }

        // Remove empty levels
        if (existing_level->empty()) {
          remove_level(existing_level);
        } else {
          update_visible_price(existing_level);
        }
      } else {
        // Delta update totals
        const Quantity visible_before = old_qty_merge > 0 ? old_qty_merge : 0;
        const Quantity visible_after = existing_order->qty > 0 ? existing_order->qty : 0;
        existing_level->total_visible_quantity += (visible_after - visible_before);
        update_visible_price(existing_level);
      }

      return true;
    }
  }

public:
  // ========================================================================================
  // PUBLIC INTERFACE
  // ========================================================================================

  // Main order processing entry point
  [[gnu::hot]] bool process(const L2::Order &order) {
    // Pack timestamp
    current_timestamp_ = (order.hour << 24) | (order.minute << 16) | (order.second << 8) | order.millisecond;

    switch (order.order_type) {
    case OrderType::MAKER:
      return add_maker_fast(order);
    case OrderType::CANCEL:
      return deduct<DeductionType::CANCEL>(order);
    case OrderType::TAKER:
      return deduct<DeductionType::TAKER>(order);
    default:
      return false;
    }
  }

  // ========================================================================================
  // SIMPLE MARKET DATA ACCESS
  // ========================================================================================

  // Get best bid price
  [[gnu::hot]] Price best_bid() const {
    update_tob();
    return best_bid_price_;
  }

  // Get best ask price
  [[gnu::hot]] Price best_ask() const {
    update_tob();
    return best_ask_price_;
  }

  // Get best bid quantity
  [[gnu::hot]] Quantity best_bid_qty() const {
    update_tob();
    if (best_bid_price_ == 0)
      return 0;

    Level *level = find_level(best_bid_price_);
    return level ? level->total_visible_quantity : 0;
  }

  // Get best ask quantity
  [[gnu::hot]] Quantity best_ask_qty() const {
    update_tob();
    if (best_ask_price_ == 0)
      return 0;

    Level *level = find_level(best_ask_price_);
    return level ? level->total_visible_quantity : 0;
  }

  // Calculate bid-ask spread
  [[gnu::hot]] Price spread() const {
    update_tob();
    return (best_bid_price_ && best_ask_price_) ? best_ask_price_ - best_bid_price_ : 0;
  }

  // ========================================================================================
  // UTILITIES
  // ========================================================================================

  // Display current market depth
  void print_book() const {
    std::ostringstream book_output;
    book_output << "[" << format_time() << "] ";

    constexpr size_t MAX_DISPLAY_LEVELS = 10;
    constexpr size_t LEVEL_WIDTH = 12;

    update_tob();

    // Collect ask levels (price <= best_ask or no clear TOB)
    std::vector<std::pair<Price, Quantity>> ask_data;
    for_each_visible_ask([&](Price price, Quantity quantity) {
      ask_data.emplace_back(price, quantity);
    },
                         MAX_DISPLAY_LEVELS);

    // Reverse ask data for display
    std::reverse(ask_data.begin(), ask_data.end());

    // Display ask levels
    book_output << "ASK: ";
    size_t ask_empty_spaces = MAX_DISPLAY_LEVELS - ask_data.size();
    for (size_t i = 0; i < ask_empty_spaces; ++i) {
      book_output << std::setw(LEVEL_WIDTH) << " ";
    }
    for (size_t i = 0; i < ask_data.size(); ++i) {
      book_output << std::setw(LEVEL_WIDTH) << std::left
                  << (std::to_string(ask_data[i].first) + "x" + std::to_string(ask_data[i].second));
    }

    book_output << "| BID: ";

    // Collect bid levels
    std::vector<std::pair<Price, Quantity>> bid_data;
    for_each_visible_bid([&](Price price, Quantity quantity) {
      bid_data.emplace_back(price, quantity);
    },
                         MAX_DISPLAY_LEVELS);

    // Display bid levels
    for (size_t i = 0; i < MAX_DISPLAY_LEVELS; ++i) {
      if (i < bid_data.size()) {
        book_output << std::setw(LEVEL_WIDTH) << std::left
                    << (std::to_string(bid_data[i].first) + "x" + std::to_string(bid_data[i].second));
      } else {
        book_output << std::setw(LEVEL_WIDTH) << " ";
      }
    }

    std::cout << book_output.str() << "\n";
  }

  // Book statistics
  size_t total_orders() const { return order_lookup_.size(); }
  size_t total_levels() const { return price_levels_.size(); }

  // Complete reset
  void clear() {
    price_levels_.clear();
    level_storage_.clear();
    order_lookup_.clear();
    order_memory_pool_.reset();
    visible_prices_.clear();
    best_bid_price_ = best_ask_price_ = 0;
    tob_invalid_ = true;
    current_timestamp_ = 0;
  }

  // ========================================================================================
  // MARKET DEPTH ITERATION - SIMPLE VERSION
  // ========================================================================================

  // Iterate through bid levels (price >= best_bid_price_)
  template <typename Func>
  void for_each_visible_bid(Func &&callback_function, size_t max_levels = 5) const {
    update_tob();

    // Collect bid levels
    std::vector<std::pair<Price, Level *>> bid_levels;
    if (best_bid_price_ == 0)
      return;
    auto it = visible_prices_.upper_bound(best_bid_price_);
    while (it != visible_prices_.begin() && bid_levels.size() < max_levels) {
      --it;
      Price price = *it;
      Level *level = find_level(price);
      if (level && level->has_visible_quantity()) {
        bid_levels.emplace_back(price, level);
      }
    }

    // Sort by price descending (highest bid first)
    std::sort(bid_levels.begin(), bid_levels.end(),
              [](const auto &a, const auto &b) { return a.first > b.first; });

    size_t levels_processed = 0;
    for (const auto &[price, level] : bid_levels) {
      if (levels_processed >= max_levels)
        break;
      callback_function(price, level->total_visible_quantity);
      ++levels_processed;
    }
  }

  // Iterate through ask levels (price <= best_ask_price_)
  template <typename Func>
  void for_each_visible_ask(Func &&callback_function, size_t max_levels = 5) const {
    update_tob();

    // Collect ask levels
    std::vector<std::pair<Price, Level *>> ask_levels;
    if (best_ask_price_ == 0)
      return;
    auto it = visible_prices_.lower_bound(best_ask_price_);
    for (; it != visible_prices_.end() && ask_levels.size() < max_levels; ++it) {
      Price price = *it;
      Level *level = find_level(price);
      if (level && level->has_visible_quantity()) {
        ask_levels.emplace_back(price, level);
      }
    }

    // Sort by price ascending (lowest ask first)
    std::sort(ask_levels.begin(), ask_levels.end(),
              [](const auto &a, const auto &b) { return a.first < b.first; });

    size_t levels_processed = 0;
    for (const auto &[price, level] : ask_levels) {
      if (levels_processed >= max_levels)
        break;
      callback_function(price, level->total_visible_quantity);
      ++levels_processed;
    }
  }

  // ========================================================================================
  // BATCH PROCESSING
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

private:
  // ========================================================================================
  // TIME FORMATTING
  // ========================================================================================

  // Convert packed timestamp to human-readable format
  std::string format_time() const {
    uint8_t hours = (current_timestamp_ >> 24) & 0xFF;
    uint8_t minutes = (current_timestamp_ >> 16) & 0xFF;
    uint8_t seconds = (current_timestamp_ >> 8) & 0xFF;
    uint8_t milliseconds = current_timestamp_ & 0xFF;

    std::ostringstream time_formatter;
    time_formatter << std::setfill('0')
                   << std::setw(2) << int(hours) << ":"
                   << std::setw(2) << int(minutes) << ":"
                   << std::setw(2) << int(seconds) << "."
                   << std::setw(3) << int(milliseconds * 10);
    return time_formatter.str();
  }
};

} // namespace lob