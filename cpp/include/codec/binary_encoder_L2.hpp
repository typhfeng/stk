#pragma once

#include "L2_DataType.hpp"
#include <cassert>
#include <cstdint>
#include <string>
#include <vector>

// Zstandard compression library
#include "../../package/zstd-1.5.7/zstd.h"

namespace L2 {

// Zstandard compression configuration - optimized for offline encoding + online decoding
// Scenario: Millions of small files (1KB-100KB), prioritize compression ratio for storage efficiency
// Online decoding needs extremely fast decompression (zstd excels at fast decompression regardless of compression level)
//
// Compression level trade-offs for small files:
// Level 1:  Fast encoding (510 MB/s), ratio 2.896x, decompression 1550 MB/s
// Level 3:  Good encoding/ratio balance, better for small files, decompression still ~1400+ MB/s
// Level 6:  Better ratio for storage, acceptable encoding time offline, decompression ~1300+ MB/s
// Level 9:  High compression ratio, slower encoding (offline OK), decompression ~1200+ MB/s
//
// For millions of small files, higher compression level saves significant storage
inline constexpr int ZSTD_COMPRESSION_LEVEL = 6;           // Balanced for offline encoding + excellent decompression
inline constexpr size_t ZSTD_COMPRESSION_BOUND_FACTOR = 2; // Conservative bound factor for worst-case compressed size

// 1. create normal compressed library
// 2. use dictionary training of Zstd to create smaller compressed library

// Intermediate CSV data structures for parsing
struct CSVSnapshot {
  std::string stock_code;
  std::string exchange_code;
  uint32_t date;
  uint32_t time;
  uint32_t price;    // in 0.01 RMB units
  uint32_t volume;   // in 100-share units
  uint64_t turnover; // in fen
  uint32_t trade_count;

  // uint32_t high;
  // uint32_t low;
  // uint32_t open;
  // uint32_t prev_close;

  // bid/ask prices and volumes (10 levels each)
  uint32_t bid_prices[10];  // in 0.01 RMB units
  uint32_t bid_volumes[10]; // in 100-share units
  uint32_t ask_prices[10];  // in 0.01 RMB units
  uint32_t ask_volumes[10]; // in 100-share units

  uint32_t weighted_avg_ask_price; // in 0.001 RMB units (VWAP)
  uint32_t weighted_avg_bid_price; // in 0.001 RMB units (VWAP)
  uint32_t total_ask_volume;       // in 100-share units
  uint32_t total_bid_volume;       // in 100-share units
};

struct CSVOrder {
  std::string stock_code;
  std::string exchange_code;
  uint32_t date;
  uint32_t time;
  uint64_t order_id;
  uint64_t exchange_order_id;
  char order_type; // A:add, D:delete for SSE; 0 for SZSE
  char order_side; // B:bid, S:ask
  uint32_t price;  // in 0.01 RMB units
  uint32_t volume; // in 100-share units
};

struct CSVTrade {
  std::string stock_code;
  std::string exchange_code;
  uint32_t date;
  uint32_t time;
  uint64_t trade_id;
  char trade_code; // 0:trade, C:cancel for SZSE; empty for SSE
  char dummy_code; // not used
  char bs_flag;    // B:buy, S:sell, empty:cancel
  uint32_t price;  // in 0.01 RMB units
  uint32_t volume; // in 100-share units
  uint64_t ask_order_id;
  uint64_t bid_order_id;
};

// Compression statistics
struct CompressionStats {
  size_t original_size = 0;
  size_t compressed_size = 0;
  double ratio = 0.0;
};

class BinaryEncoder_L2 {
public:
  // Constructor with optional capacity hints for better memory allocation
  BinaryEncoder_L2(size_t estimated_snapshots = 5000, size_t estimated_orders = 1000000);

  // CSV parsing functions (hot path functions inlined)
  static std::vector<std::string> split_csv_line(const std::string &line);
  static uint32_t parse_time_to_ms(uint32_t time_int);
  static inline uint32_t parse_price_to_fen(const std::string &price_str);
  static inline uint32_t parse_vwap_price(const std::string &price_str);           // For VWAP prices with 0.001 RMB precision
  static inline uint32_t parse_volume_to_100shares(const std::string &volume_str); // Convert shares to 100-share units
  static inline uint64_t parse_turnover_to_fen(const std::string &turnover_str);

  bool parse_snapshot_csv(const std::string &filepath, std::vector<CSVSnapshot> &snapshots);
  bool parse_order_csv(const std::string &filepath, std::vector<CSVOrder> &orders);
  bool parse_trade_csv(const std::string &filepath, std::vector<CSVTrade> &trades);

  // CSV to L2 conversion functions
  static Snapshot csv_to_snapshot(const CSVSnapshot &csv_snap);
  static Order csv_to_order(const CSVOrder &csv_order);
  static Order csv_to_trade(const CSVTrade &csv_trade);

  // binary encoding functions
  bool encode_snapshots(const std::vector<Snapshot> &snapshots, const std::string &filepath, bool use_delta = ENABLE_DELTA_ENCODING);
  bool encode_orders(const std::vector<Order> &orders, const std::string &filepath, bool use_delta = ENABLE_DELTA_ENCODING);

  // Zstandard compression helper functions (pure standard compression)
  bool compress_and_write_data(const std::string &filepath, const void *data, size_t data_size);
  static size_t calculate_compression_bound(size_t data_size);

  // High-level processing functions
  bool process_stock_data(const std::string &stock_dir,
                          const std::string &output_dir,
                          const std::string &stock_code,
                          std::vector<Snapshot> *out_snapshots = nullptr,
                          std::vector<Order> *out_orders = nullptr);

  // Get compression statistics
  const CompressionStats &get_compression_stats() const { return compression_stats; }

private:
  // Reusable vector tables for delta encoding (snapshots)
  mutable std::vector<uint8_t> temp_hours, temp_minutes, temp_seconds;
  mutable std::vector<uint16_t> temp_highs, temp_lows, temp_closes;
  mutable std::vector<uint16_t> temp_bid_prices[10], temp_ask_prices[10];
  mutable std::vector<uint16_t> temp_all_bid_vwaps, temp_all_ask_vwaps;
  mutable std::vector<uint32_t> temp_all_bid_volumes, temp_all_ask_volumes;

  // Reusable vector tables for delta encoding (orders)
  mutable std::vector<uint8_t> temp_order_hours, temp_order_minutes, temp_order_seconds, temp_order_milliseconds;
  mutable std::vector<uint16_t> temp_order_prices;
  mutable std::vector<uint32_t> temp_bid_order_ids, temp_ask_order_ids;

  // Compression statistics
  mutable CompressionStats compression_stats;

  // Time conversion functions (inlined for performance)
  static inline uint8_t time_to_hour(uint32_t time_ms);
  static inline uint8_t time_to_minute(uint32_t time_ms);
  static inline uint8_t time_to_second(uint32_t time_ms);
  static inline uint8_t time_to_millisecond_10ms(uint32_t time_ms);

  // Market detection (inlined for performance)
  static inline bool is_szse_market(const std::string &stock_code);

  // Order processing (inlined for performance)
  static inline uint8_t determine_order_type(char csv_order_type, char csv_trade_code, bool is_trade, bool is_szse);
  static inline bool determine_order_direction(char side_flag);
};

} // namespace L2
