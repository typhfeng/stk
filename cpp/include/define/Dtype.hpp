#pragma once

#include <cstdint>

namespace Table {

// because no need to dump whole snapshot table, we dont pack it tight in memory
struct Snapshot_Record { // discrete or 3s fixed interval
  // timestamp ============================================
  uint16_t year;           // 2 bytes
  uint8_t month;           // 1 byte
  uint8_t day;             // 1 byte
  uint8_t hour;            // 1 byte
  uint8_t minute;          // 1 byte
  uint8_t second;          // 1 byte
  uint32_t seconds_in_day; // 4 bytes - no guarantee that every day start exactly at market open
  // LOB ==================================================
  float latest_price_tick;  // 4 bytes - price in RMB
  uint8_t trade_count;      // 1 byte
  uint16_t volume;          // 2 bytes - unit in hands (100 shares)
  uint32_t turnover;        // 4 bytes - RMB
  float bid_price_ticks[5]; // 20 bytes - prices in RMB
  uint16_t bid_volumes[5];  // 10 bytes - units of 100 shares
  float ask_price_ticks[5]; // 20 bytes - prices in RMB
  uint16_t ask_volumes[5];  // 10 bytes - units of 100 shares
  uint8_t direction;        // 1 byte - 0: buy, 1: sell (direction of the latest trade, not necessarily align with direction of vwap in 3s)
  // features =============================================
  float ofi_ask[5]; // 20 bytes
  float ofi_bid[5]; // 20 bytes
  // Total:  bytes
};

// low-freq data should be aligned to 32b boundary for better cache performance
#pragma pack(push, 1)
struct RunBar_Record { // volume sampled run bar (e.g. n seconds)
  // timestamp = bar close time (so that is actionable)
  uint16_t year;  // 2 bytes
  uint8_t month;  // 1 byte
  uint8_t day;    // 1 byte
  uint8_t hour;   // 1 byte
  uint8_t minute; // 1 byte
  uint8_t second; // 1 byte
  float open;     // 4 bytes
  float high;     // 4 bytes
  float low;      // 4 bytes
  float close;    // 4 bytes
  float vwap;     // 4 bytes
                  // Total: 24 bytes
};
#pragma pack(pop)
} // namespace Table

// Data Struct
inline constexpr int BLen = 100;
inline constexpr int snapshot_interval = 3;
inline constexpr int trade_hrs_in_a_day = 4;

// Resample
inline constexpr int MIN_DATA_BASE_PERIOD = snapshot_interval; // min base period of data (in seconds)
inline constexpr int RESAMPLE_BASE_PERIOD = 30; // target sample period (in seconds)
inline constexpr int RESAMPLE_EMA_DAYS = 5;
// days   3   5   10  25 
// stddev 108 110 114 124
