#pragma once

#include <cstdint>

// struct trade {
//   uint8_t direction; // 1bit - 0:bid 1:ask
//   uint16_t volume;   // 16bit - units of 100 shares (hands)
// };
// 
// // Data Struct
// inline constexpr int BLen = 100;
// inline constexpr int snapshot_interval = 3;
// inline constexpr int trade_hrs_in_a_day = 4;
// 
// // Resample
// inline constexpr int MIN_DATA_BASE_PERIOD = snapshot_interval; // min base period of data (in seconds)
// inline constexpr int RESAMPLE_BASE_PERIOD = 30;                // target sample period (in seconds)
// inline constexpr int RESAMPLE_EMA_DAYS = 5;
// // days   3   5   10  25
// // stddev 108 110 114 124
// 
// // Features
// // convexity-weighted multi-level imbalance (by level)
// inline constexpr int CWI_N = 3;
// inline constexpr float CWI_GAMMA[CWI_N] = {0.1f, 0.8f, 1.5f}; // >0: stronger decay for far away bid/ask(by level)
// // distance–discounted multi-level imbalance (by price)
// inline constexpr int DDI_N = 3;
// inline constexpr float DDI_LAMBDAS[DDI_N] = {0.1f, 0.2f, 0.4f}; // >0: stronger decay for far away bid/ask(by price)
// 
// namespace Table {
// 
// // because no need to dump whole snapshot table, we dont pack it tight in memory
// struct Snapshot_Record { // discrete or 3s fixed interval
//   // timestamp ============================================
//   uint16_t year;           // 2 bytes
//   uint8_t month;           // 1 byte
//   uint8_t day;             // 1 byte
//   uint8_t hour;            // 1 byte
//   uint8_t minute;          // 1 byte
//   uint8_t second;          // 1 byte
//   uint32_t seconds_in_day; // 4 bytes - no guarantee that every day start exactly at market open
//   // LOB ==================================================
//   float latest_price_tick;  // 4 bytes - price in RMB
//   uint8_t trade_count;      // 1 byte
//   uint16_t volume;          // 2 bytes - unit in hands (100 shares)
//   uint32_t turnover;        // 4 bytes - RMB
//   float bid_price_ticks[5]; // 20 bytes - prices in RMB
//   uint16_t bid_volumes[5];  // 10 bytes - units of 100 shares
//   float ask_price_ticks[5]; // 20 bytes - prices in RMB
//   uint16_t ask_volumes[5];  // 10 bytes - units of 100 shares
//   uint8_t direction;        // 1 byte - 0: buy, 1: sell (direction of the latest trade, not necessarily align with direction of vwap in 3s)
//   // features(LOB) ========================================
//   // NOTE: -> means predict, it is expected the feature has predictive power on return after xx time period (after synthesizing into alphas)
//   float spread_z;     // spread: z-score(30min) -> 10s
//   float mpg_z;        // micro-price-gap: z-score(30min) -> 10s
//   float tobi_z;       // top-of-book imbalance: z-score(30min) -> 10s
//   float cwi_z[CWI_N]; // convexity-weighted multi-level imbalance: z-score(30min) -> 1m (0: less decay for far away bid/ask)
//   float ddi_z[DDI_N]; // distance–discounted multi-level imbalance: z-score(30min) -> 1m (0: less decay for far away bid/ask)
//   // Total:  bytes
// };
// 
// // low-freq data should be aligned to 32b boundary for better cache performance
// #pragma pack(push, 1)
// struct RunBar_Record { // volume sampled run bar (e.g. n seconds)
//   // timestamp = bar close time (so that is actionable)
//   uint16_t year;  // 2 bytes
//   uint8_t month;  // 1 byte
//   uint8_t day;    // 1 byte
//   uint8_t hour;   // 1 byte
//   uint8_t minute; // 1 byte
//   uint8_t second; // 1 byte
//   float open;     // 4 bytes
//   float high;     // 4 bytes
//   float low;      // 4 bytes
//   float close;    // 4 bytes
//   float vwap;     // 4 bytes
//                   // Total: 24 bytes
// };
// #pragma pack(pop)
// } // namespace Table
