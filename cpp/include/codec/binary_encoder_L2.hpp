#pragma once

#include "L2_DataType.hpp"
#include <cassert>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "../../package/zstd-1.5.7/zstd.h"

namespace L2 {

// ============================================================================
// Configuration
// ============================================================================

// Zstandard compression level (offline encoding optimized)
// Trade-off: higher level → better compression ratio, slower encoding
// Level 6: balanced for storage efficiency + acceptable encoding speed
inline constexpr int ZSTD_COMPRESSION_LEVEL = 6;

// ============================================================================
// Intermediate CSV Structures
// ============================================================================

// Snapshot data (行情)
struct CSVSnapshot {
  std::string stock_code;
  std::string exchange_code;
  uint32_t date;
  uint32_t time;
  uint32_t price;       // in 0.01 RMB units
  uint32_t volume;      // in shares
  uint64_t turnover;    // in fen
  uint32_t trade_count; // incremental count

  // 10-level orderbook
  uint32_t bid_prices[10];  // in 0.01 RMB units
  uint32_t bid_volumes[10]; // in shares
  uint32_t ask_prices[10];  // in 0.01 RMB units
  uint32_t ask_volumes[10]; // in shares

  // Aggregated orderbook info
  uint32_t weighted_avg_bid_price; // VWAP in 0.001 RMB units
  uint32_t weighted_avg_ask_price; // VWAP in 0.001 RMB units
  uint32_t total_bid_volume;       // in shares
  uint32_t total_ask_volume;       // in shares
};

// Order data (逐笔委托)
struct CSVOrder {
  std::string stock_code;
  std::string exchange_code;
  uint32_t date;
  uint32_t time;
  uint64_t order_id;          // Internal ID (for data validation)
  uint64_t exchange_order_id; // Exchange ID (actual order ID)
  char order_type;            // SSE: A=add, D=delete; SZSE: varies
  char order_side;            // B=bid, S=ask
  uint32_t price;             // in 0.01 RMB units
  uint32_t volume;            // in shares
};

// Trade data (逐笔成交)
struct CSVTrade {
  std::string stock_code;
  std::string exchange_code;
  uint32_t date;
  uint32_t time;
  uint64_t trade_id;
  char trade_code; // SZSE: 0=trade, C=cancel; SSE: unused
  char dummy_code; // unused
  char bs_flag;    // B=buy, S=sell, empty=cancel
  uint32_t price;  // in 0.01 RMB units
  uint32_t volume; // in shares
  uint64_t ask_order_id;
  uint64_t bid_order_id;
};

// ============================================================================
// Compression Statistics
// ============================================================================

struct CompressionStats {
  size_t original_size = 0;
  size_t compressed_size = 0;
  double ratio = 0.0;
};

// ============================================================================
// Binary Encoder Class
// ============================================================================

class BinaryEncoder_L2 {
public:
  // Constructor with optional capacity hints
  BinaryEncoder_L2(size_t estimated_snapshots = 5000, size_t estimated_orders = 1000000);
  
  // Destructor: clean up ZSTD context
  ~BinaryEncoder_L2();

  // ------------------------------------------------------------
  // CSV Parsing API
  // ------------------------------------------------------------

  // Parse CSV file into intermediate structures
  bool parse_snapshot_csv(const std::string &filepath, std::vector<CSVSnapshot> &snapshots);
  bool parse_order_csv(const std::string &filepath, std::vector<CSVOrder> &orders);
  bool parse_trade_csv(const std::string &filepath, std::vector<CSVTrade> &trades);

  // ------------------------------------------------------------
  // Data Conversion API
  // ------------------------------------------------------------

  // Convert CSV structures to binary structures
  static Snapshot csv_to_snapshot(const CSVSnapshot &csv);
  static Order csv_to_order(const CSVOrder &csv);
  static Order csv_to_trade(const CSVTrade &csv);

  // ------------------------------------------------------------
  // Binary Encoding API
  // ------------------------------------------------------------

  // Encode and compress binary structures to file
  bool encode_snapshots(const std::vector<Snapshot> &snapshots, 
                       const std::string &filepath);
  bool encode_orders(const std::vector<Order> &orders, 
                    const std::string &filepath);

  // ------------------------------------------------------------
  // High-Level Interface
  // ------------------------------------------------------------

  // Process entire stock data: parse → convert → encode
  bool process_stock_data(const std::string &stock_dir,
                         const std::string &output_dir,
                         const std::string &stock_code,
                         std::vector<Snapshot> *out_snapshots = nullptr,
                         std::vector<Order> *out_orders = nullptr);

  // Get compression statistics
  const CompressionStats &get_compression_stats() const { return compression_stats; }

  // ------------------------------------------------------------
  // Utility Functions (public for testing)
  // ------------------------------------------------------------

  static std::vector<std::string_view> split_csv_line_view(std::string_view line);
  static uint32_t parse_time_to_ms(uint32_t time_int);
  static inline uint32_t parse_price_to_fen(std::string_view str);
  static inline uint32_t parse_vwap_price(std::string_view str);
  static inline uint32_t parse_volume(std::string_view str);
  static inline uint64_t parse_turnover_to_fen(std::string_view str);

private:
  // ------------------------------------------------------------
  // Compression Helpers
  // ------------------------------------------------------------

  bool compress_and_write_data(const std::string &filepath, const void *data, size_t data_size);
  static size_t calculate_compression_bound(size_t data_size);

  // ------------------------------------------------------------
  // Delta Encoding Buffers (reusable, avoid reallocation)
  // ------------------------------------------------------------

  // Snapshot buffers
  mutable std::vector<uint8_t> temp_snap_hours, temp_snap_minutes, temp_snap_seconds;
  mutable std::vector<uint16_t> temp_snap_closes;
  mutable std::vector<uint16_t> temp_snap_bid_prices[10], temp_snap_ask_prices[10];
  mutable std::vector<uint16_t> temp_snap_bid_vwaps, temp_snap_ask_vwaps;
  mutable std::vector<uint32_t> temp_snap_bid_volumes, temp_snap_ask_volumes;

  // Order buffers
  mutable std::vector<uint8_t> temp_order_hours, temp_order_minutes, temp_order_seconds, temp_order_millis;
  mutable std::vector<uint16_t> temp_order_prices;
  mutable std::vector<uint32_t> temp_order_bid_ids, temp_order_ask_ids;

  // Compression statistics
  mutable CompressionStats compression_stats;
  
  // ZSTD compression context (reused across calls)
  ZSTD_CCtx* zstd_ctx_;
};

} // namespace L2
