#pragma once

#include "../codec/L2_DataType.hpp"
#include <algorithm>
#include <cstdint>
#include <map>
#include <vector>

// this is a taker driven model that does not try to determine matching sequence by itself, but use maker id info in taker order directly
// TODO: 最大威胁:
//  1. 同一ms内(甚至不同时刻间), order之间可能为乱序
//  2. order信息可能丢失
//  3. snapshot数据为异步 (对于沪深两市, 快照的时间点不确定, 并不是0ms对齐的, 这意味着快照只能作为大规模偏移后的模糊矫正)

// implmentation:

namespace lob {

// ============================================================================
// Type Aliases
// ============================================================================

using L2Order = L2::Order;
using Price = uint16_t;
using Quantity = uint16_t;
using OrderId = uint32_t;

// ============================================================================
// Constants
// ============================================================================

namespace OrderType {
constexpr uint8_t MAKER = 0;
constexpr uint8_t CANCEL = 1;
constexpr uint8_t TAKER = 3;
} // namespace OrderType

namespace OrderDirection {
constexpr uint8_t BID = 0;
constexpr uint8_t ASK = 1;
} // namespace OrderDirection

// ============================================================================
// Data Structures
// ============================================================================

// Order entry in the limit order book
struct OrderEntry {
  Price price;
  Quantity quantity;
  OrderId bid_id;
  OrderId ask_id;

  OrderEntry(Price price_val, Quantity qty_val, OrderId bid_id_val, OrderId ask_id_val)
      : price(price_val), quantity(qty_val), bid_id(bid_id_val), ask_id(ask_id_val) {}
};

// Price comparators for proper bid/ask ordering
struct BidPriceComparator {
  bool operator()(Price lhs, Price rhs) const {
    return lhs > rhs; // Higher prices have priority for bids
  }
};

struct AskPriceComparator {
  bool operator()(Price lhs, Price rhs) const {
    return lhs < rhs; // Lower prices have priority for asks
  }
};

// ============================================================================
// Limit Order Book Implementation
// ============================================================================

class LimitOrderBook {
public:
  // Type aliases for better readability
  using OrderQueue = std::vector<OrderEntry>;
  using BidMap = std::map<Price, OrderQueue, BidPriceComparator>;
  using AskMap = std::map<Price, OrderQueue, AskPriceComparator>;

  // Order queues organized by price level
  BidMap bid_orders;
  AskMap ask_orders;

  // ========================================================================
  // Public Interface
  // ========================================================================

  // Process incoming L2 order
  bool process_order(const L2Order &order) {
    switch (order.order_type) {
    case OrderType::MAKER:
      return add_maker(order);
    case OrderType::CANCEL:
      return add_cancel(order);
    case OrderType::TAKER:
      return add_taker(order);
    default:
      return false;
    }
  }

  // Market data accessors
  Price get_best_bid_price() const {
    return bid_orders.empty() ? 0 : bid_orders.begin()->first;
  }

  Price get_best_ask_price() const {
    return ask_orders.empty() ? 0 : ask_orders.begin()->first;
  }

  size_t get_bid_level_count() const { return bid_orders.size(); }
  size_t get_ask_level_count() const { return ask_orders.size(); }

private:
  // ========================================================================
  // Template Helpers
  // ========================================================================

  // Generic order cancellation for any order map type
  template <typename OrderMap>
  bool cancel_order_from_map(OrderMap &order_map, OrderId target_bid_id, OrderId target_ask_id) {
    for (auto &[price_level, order_queue] : order_map) {
      auto order_iter = std::find_if(order_queue.begin(), order_queue.end(),
                                     [target_bid_id, target_ask_id](const OrderEntry &entry) {
                                       return entry.bid_id == target_bid_id && entry.ask_id == target_ask_id;
                                     });

      if (order_iter != order_queue.end()) {
        order_queue.erase(order_iter);
        if (order_queue.empty()) {
          order_map.erase(price_level);
        }
        return true;
      }
    }
    return false;
  }

  // Clear specific order based on taker information (maker_id specifies which order to clear)
  template <typename OrderMap>
  bool clear_order_by_maker_id(OrderMap &order_map, OrderId maker_bid_id, OrderId maker_ask_id, Quantity clear_quantity) {
    for (auto &[price_level, order_queue] : order_map) {
      auto order_iter = std::find_if(order_queue.begin(), order_queue.end(),
                                     [maker_bid_id, maker_ask_id](const OrderEntry &entry) {
                                       return entry.bid_id == maker_bid_id && entry.ask_id == maker_ask_id;
                                     });

      if (order_iter != order_queue.end()) {
        // Reduce quantity or remove order
        if (order_iter->quantity <= clear_quantity) {
          order_queue.erase(order_iter);
        } else {
          order_iter->quantity -= clear_quantity;
        }
        
        // Remove empty price levels
        if (order_queue.empty()) {
          order_map.erase(price_level);
        }
        return true;
      }
    }
    return false;
  }

  // ========================================================================
  // Order Processing Implementation
  // ========================================================================

  bool add_maker(const L2Order &order) {
    bool is_buy_order = (order.order_dir == OrderDirection::BID);
    Price order_price = order.price;
    Quantity order_quantity = order.volume;
    OrderId bid_id = order.bid_order_id;
    OrderId ask_id = order.ask_order_id;

    if (order_quantity == 0)
      return false;

    // Add order to the book (no self-derived matching)
    OrderEntry new_entry(order_price, order_quantity, bid_id, ask_id);
    if (is_buy_order) {
      bid_orders[order_price].push_back(new_entry);
    } else {
      ask_orders[order_price].push_back(new_entry);
    }

    return true;
  }

  bool add_cancel(const L2Order &order) {
    bool is_buy_order = (order.order_dir == OrderDirection::BID);
    OrderId target_bid_id = order.bid_order_id;
    OrderId target_ask_id = order.ask_order_id;

    return is_buy_order ? cancel_order_from_map(bid_orders, target_bid_id, target_ask_id)
                        : cancel_order_from_map(ask_orders, target_bid_id, target_ask_id);
  }

  bool add_taker(const L2Order &order) {
    // Taker order contains both taker_id and maker_id to specify exact match
    bool is_buy_order = (order.order_dir == OrderDirection::BID);
    Quantity order_quantity = order.volume;
    OrderId taker_bid_id = order.bid_order_id;
    OrderId taker_ask_id = order.ask_order_id;

    // For taker orders, we interpret the IDs as:
    // - One ID is the taker_id (the new order)
    // - The other ID is the maker_id (existing order to match against)
    // Clear the specified maker order from the opposing side
    if (is_buy_order) {
      // Buy taker matches against ask orders
      return clear_order_by_maker_id(ask_orders, taker_bid_id, taker_ask_id, order_quantity);
    } else {
      // Sell taker matches against bid orders  
      return clear_order_by_maker_id(bid_orders, taker_bid_id, taker_ask_id, order_quantity);
    }
  }
};

// Convenience alias
using LOB = LimitOrderBook;

} // namespace lob
