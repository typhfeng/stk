#pragma once

#include "L2_DataType.hpp"
#include <cmath>
#include <string>
#include <vector>

// Zstandard compression library
#include "../../package/zstd-1.5.7/zstd.h"

namespace L2 {

// Inline constexpr functions to calculate column widths from bit widths
// These will be optimized away at compile time
namespace BitWidthFormat {
// Calculate number of decimal digits needed for a given bit width
inline constexpr int calc_width(uint8_t bit_width) {
  return bit_width <= 3 ? 1 : // 1-3 bits: 1 digit
             bit_width <= 6 ? 2
                            : // 4-6 bits: 2 digits
             bit_width <= 10 ? 3
                             : // 7-10 bits: 3 digits
             bit_width <= 13 ? 4
                             : // 11-13 bits: 4 digits
             bit_width <= 16 ? 5
                             : // 14-16 bits: 5 digits
             bit_width <= 32 ? 10
                             : 15; // 17-32 bits: 10 digits, >32: 15 digits
}

// Helper function to get column bit width
inline constexpr int get_column_width(std::string_view column_name) {
  constexpr size_t schema_size = sizeof(Snapshot_Schema) / sizeof(Snapshot_Schema[0]);
  return calc_width(SchemaUtils::get_column_bitwidth(Snapshot_Schema, schema_size, column_name));
}

// Snapshot field widths - using schema names
inline constexpr int hour_width() { return get_column_width("hour"); }
inline constexpr int minute_width() { return get_column_width("minute"); }
inline constexpr int second_width() { return get_column_width("second"); }
inline constexpr int trade_count_width() { return get_column_width("trade_count"); }
inline constexpr int volume_width() { return get_column_width("volume"); }
inline constexpr int turnover_width() { return get_column_width("turnover"); }
inline constexpr int price_width() { return get_column_width("high"); } // Using high as reference for price width
inline constexpr int direction_width() { return get_column_width("direction"); }
inline constexpr int vwap_width() { return get_column_width("all_bid_vwap"); }
inline constexpr int total_volume_width() { return get_column_width("all_bid_volume"); }

// Order field widths - using schema names
inline constexpr int order_type_width() { return get_column_width("order_type"); }
inline constexpr int order_dir_width() { return get_column_width("order_dir"); }
inline constexpr int order_price_width() { return get_column_width("price"); }
inline constexpr int order_volume_width() { return get_column_width("volume"); }
inline constexpr int order_id_width() { return get_column_width("bid_order_id"); }
inline constexpr int millisecond_width() { return get_column_width("millisecond"); } // Using schema-defined bit width
} // namespace BitWidthFormat

class BinaryDecoder_L2 {
public:
  // Constructor with optional capacity hints for better memory allocation
  BinaryDecoder_L2(size_t estimated_snapshots = 100000, size_t estimated_orders = 500000);

  // decoder functions
  bool decode_snapshots(const std::string &filepath, std::vector<Snapshot> &snapshots, bool use_delta = ENABLE_DELTA_ENCODING);
  bool decode_orders(const std::string &filepath, std::vector<Order> &orders, bool use_delta = ENABLE_DELTA_ENCODING);

  // Zstandard decompression helper functions (pure standard decompression)
  static bool read_and_decompress_data(const std::string &filepath, void *data, size_t expected_size, size_t &actual_size);

  // Print snapshot in human-readable format
  static void print_snapshot(const Snapshot &snapshot, size_t index = 0);

  // Print order in human-readable format
  static void print_order(const Order &order, size_t index = 0);

  // Print all snapshots with array details
  static void print_all_snapshots(const std::vector<Snapshot> &snapshots);

  // Print all orders with array details
  static void print_all_orders(const std::vector<Order> &orders);

  // Convert time components back to readable format
  static std::string time_to_string(uint8_t hour, uint8_t minute, uint8_t second, uint8_t millisecond_10ms = 0);

  // Convert price from internal format to readable format (hot path - inlined)
  static inline double price_to_rmb(uint16_t price_ticks);

  // Convert VWAP price from internal format to readable format (0.001 RMB units)
  static inline double vwap_to_rmb(uint16_t vwap_ticks);

  // Convert volume from internal format to readable format (hot path - inlined)
  static inline uint32_t volume_to_shares(uint16_t volume_100shares);

  // Helper function to extract count from filename (used by dictionary compression)
  static size_t extract_count_from_filename(const std::string &filepath);



private:
  // Reusable vector tables for delta decoding (snapshots)
  mutable std::vector<uint8_t> temp_hours, temp_minutes, temp_seconds;
  mutable std::vector<uint16_t> temp_highs, temp_lows, temp_closes;
  mutable std::vector<uint16_t> temp_bid_prices[10], temp_ask_prices[10];
  mutable std::vector<uint16_t> temp_all_bid_vwaps, temp_all_ask_vwaps;
  mutable std::vector<uint32_t> temp_all_bid_volumes, temp_all_ask_volumes;

  // Reusable vector tables for delta decoding (orders)
  mutable std::vector<uint8_t> temp_order_hours, temp_order_minutes, temp_order_seconds, temp_order_milliseconds;
  mutable std::vector<uint16_t> temp_order_prices;
  mutable std::vector<uint32_t> temp_bid_order_ids, temp_ask_order_ids;

  static const char *order_type_to_string(uint8_t order_type);
  static const char *order_dir_to_string(uint8_t order_dir);
  static const char *order_type_to_char(uint8_t order_type);
  static const char *order_dir_to_char(uint8_t order_dir);
};

} // namespace L2
