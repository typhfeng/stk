#pragma once

#include <cmath>
#include <cstdint>

namespace L1 {

// ============================================================================
// COMMON DATA STRUCTURES AND CONSTANTS FOR L1 BINARY FORMAT
// ============================================================================

// Binary record structure (54 bytes total)
#pragma pack(push, 1)
struct BinaryRecord {
  bool sync;                  // 1 byte
  uint8_t day;                // 1 byte
  uint16_t time_s;            // 2 bytes - seconds in day
  int16_t latest_price_tick;  // 2 bytes - price * 100
  uint8_t trade_count;        // 1 byte
  uint32_t turnover;          // 4 bytes - RMB
  uint16_t volume;            // 2 bytes - units of 100 shares
  int16_t bid_price_ticks[5]; // 10 bytes - prices * 100
  uint16_t bid_volumes[5];    // 10 bytes - units of 100 shares
  int16_t ask_price_ticks[5]; // 10 bytes - prices * 100
  uint16_t ask_volumes[5];    // 10 bytes - units of 100 shares
  uint8_t direction;          // 1 byte
                              // Total: 54 bytes
};
#pragma pack(pop)

// Differential encoding configuration
constexpr bool DIFF_FIELDS[] = {
    false, // sync
    true,  // day
    true,  // time_s
    true,  // latest_price_tick
    false, // trade_count
    false, // turnover
    false, // volume
    true,  // bid_price_ticks (array)
    false, // bid_volumes
    true,  // ask_price_ticks (array)
    false, // ask_volumes
    false  // direction
};

// Common utility functions
inline float TickToPrice(int16_t tick) {
  return static_cast<float>(tick * 0.01f);
}

inline int16_t PriceToTick(float price) {
  return static_cast<int16_t>(std::round(price * 100.0f));
}

} // namespace L1
