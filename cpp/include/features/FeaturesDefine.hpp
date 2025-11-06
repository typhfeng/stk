#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

// ============================================================================
// LEVEL 0: Tick-level Features
// ============================================================================
#define LEVEL_0_FIELDS(X)                        \
  X(timestamp, "Event timestamp in nanoseconds") \
  X(mid_price, "Mid price (bid+ask)/2")          \
  X(spread, "Bid-ask spread")                    \
  X(spread_z, "Spread z-score")                  \
  X(tobi, "Top-of-book imbalance")               \
  X(tobi_z, "Top-of-book imbalance z-score")     \
  X(micro_price, "Volume-weighted micro price")  \
  X(mpg, "Micro-price gap (micro - mid)")        \
  X(mpg_z, "Micro-price gap z-score")

// ============================================================================
// LEVEL 1: Minute-level Features
// ============================================================================

#define LEVEL_1_FIELDS(X)                  \
  X(timestamp, "Minute start timestamp")   \
  X(open, "Open price (first mid price)")  \
  X(high, "High price (max mid price)")    \
  X(low, "Low price (min mid price)")      \
  X(close, "Close price (last mid price)") \
  X(vwap, "Volume-weighted average price") \
  X(volume, "Total volume (tick count proxy)")

// ============================================================================
// LEVEL 2: Hour-level Features
// ============================================================================

#define LEVEL_2_FIELDS(X)                                        \
  X(timestamp, "Hour start timestamp")                           \
  X(support_level, "Support level (weighted low prices)")        \
  X(resistance_level, "Resistance level (weighted high prices)") \
  X(pivot_point, "Pivot point ((high + low + close) / 3)")       \
  X(price_range, "Price range (high - low)")                     \
  X(dominant_side, "Dominant side (1=buy, -1=sell, 0=neutral)")

// ============================================================================
// ALL LEVELS REGISTRY
// ============================================================================
// Format: X(level_name, level_index, fields_macro)

#define ALL_LEVELS(X)      \
  X(L0, 0, LEVEL_0_FIELDS) \
  X(L1, 1, LEVEL_1_FIELDS) \
  X(L2, 2, LEVEL_2_FIELDS)

// ============================================================================
// TIME GRANULARITY CONFIGURATION
// ============================================================================

constexpr size_t TRADE_HOURS_PER_DAY = 4;
constexpr size_t TRADE_SECONDS_PER_DAY = TRADE_HOURS_PER_DAY * 3600; // 14400 seconds

// Time unit types
enum class TimeUnit : uint8_t {
  MILLISECOND = 0,
  SECOND = 1,
  MINUTE = 2,
  HOUR = 3
};

// Level time configuration
struct LevelTimeConfig {
  TimeUnit unit;
  size_t interval; // Number of units per time index

  constexpr size_t max_capacity() const {
    switch (unit) {
    case TimeUnit::MILLISECOND:
      return (TRADE_SECONDS_PER_DAY * 1000) / interval + 1;
    case TimeUnit::SECOND:
      return TRADE_SECONDS_PER_DAY / interval + 1;
    case TimeUnit::MINUTE:
      return (TRADE_SECONDS_PER_DAY / 60) / interval + 1;
    case TimeUnit::HOUR:
      return (TRADE_SECONDS_PER_DAY / 3600) / interval + 1;
    }
    return TRADE_SECONDS_PER_DAY + 1;
  }
};

// Predefined level configurations
constexpr LevelTimeConfig LEVEL_CONFIGS[3] = {
    {TimeUnit::SECOND, 1}, // L0: 1s
    {TimeUnit::MINUTE, 1}, // L1: 1min
    {TimeUnit::HOUR, 1}    // L2: 1hour
};

// ============================================================================
// TRADING SESSION MAPPING - High Performance Non-linear Time Conversion
// ============================================================================
// Chinese stock market trading sessions:
//   Morning:   09:30 - 11:30 (2 hours)
//   Lunch:     11:30 - 13:00 (non-trading)
//   Afternoon: 13:00 - 15:00 (2 hours)
// Total trading time: 4 hours = 14400 seconds

// Trading session boundaries (in minutes since midnight)
constexpr uint16_t MORNING_START_MIN = 9 * 60 + 30; // 570 (09:30)
constexpr uint16_t MORNING_END_MIN = 11 * 60 + 30;  // 690 (11:30)
constexpr uint16_t AFTERNOON_START_MIN = 13 * 60;   // 780 (13:00)
constexpr uint16_t AFTERNOON_END_MIN = 15 * 60;     // 900 (15:00)

// Helper: Map clock time to trading seconds (comptime)
// Returns: -1 for pre-market, 0-7199 for morning, 7200-14399 for afternoon, 14400 for post-market
constexpr int16_t map_clock_to_trading_seconds(uint8_t hour, uint8_t minute) {
  const uint16_t total_minutes = hour * 60 + minute;

  // Morning session: 09:30-11:30 → 0-7199 seconds
  if (total_minutes >= MORNING_START_MIN && total_minutes < MORNING_END_MIN) {
    return static_cast<int16_t>((total_minutes - MORNING_START_MIN) * 60);
  }

  // Afternoon session: 13:00-15:00 → 7200-14399 seconds
  if (total_minutes >= AFTERNOON_START_MIN && total_minutes < AFTERNOON_END_MIN) {
    return static_cast<int16_t>(7200 + (total_minutes - AFTERNOON_START_MIN) * 60);
  }

  // Lunch break: map to afternoon session start
  if (total_minutes >= MORNING_END_MIN && total_minutes < AFTERNOON_START_MIN) {
    return 7200;
  }

  // Pre-market
  if (total_minutes < MORNING_START_MIN) {
    return -1;
  }

  // Post-market: clamp to end
  return 14400;
}

// Constexpr function to generate lookup table at compile time
constexpr auto generate_trading_offset_table() {
  std::array<int16_t, 24 * 60> table{};
  for (size_t i = 0; i < 24 * 60; ++i) {
    const uint8_t hour = i / 60;
    const uint8_t minute = i % 60;
    table[i] = map_clock_to_trading_seconds(hour, minute);
  }
  return table;
}

// Compile-time generated lookup table (1440 entries × 2 bytes = 2.88 KB)
static constexpr auto TRADING_OFFSET_LUT = generate_trading_offset_table();

// ============================================================================
// TIME CONVERSION - O(1) Branchless Lookup
// ============================================================================

// Convert time to trading seconds (0-14400)
inline constexpr size_t time_to_trading_seconds(uint8_t hour, uint8_t minute, uint8_t second) {
  const size_t hm_idx = hour * 60 + minute;
  const int16_t base = TRADING_OFFSET_LUT[hm_idx];
  const size_t trading_seconds = (base < 0 ? 0 : static_cast<size_t>(base)) + second;
  return trading_seconds;
}

// Convert time to trading milliseconds (0-14400000)
inline constexpr size_t time_to_trading_milliseconds(uint8_t hour, uint8_t minute, uint8_t second, uint8_t millisecond) {
  return time_to_trading_seconds(hour, minute, second) * 1000 + millisecond;
}
