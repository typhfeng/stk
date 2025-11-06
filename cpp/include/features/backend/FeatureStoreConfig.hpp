#pragma once

#include "../FeaturesDefine.hpp"
#include <cstddef>

// ============================================================================
// FEATURE STORE CONFIGURATION - AUTO-GENERATED FROM SCHEMA
// ============================================================================
// Fully macro-driven code generation:
// - Level metadata (count, indices)
// - Field metadata (count, offsets)
// - Data structures (LevelNData structs)
// - Capacity configuration
//
// All features stored as float (normalized distribution assumption)
// ============================================================================

// ============================================================================
// LEVEL METADATA - AUTO-GENERATED
// ============================================================================

// Count total number of levels
#define COUNT_LEVEL(level_name, level_num, fields) +1
constexpr size_t LEVEL_COUNT = 0 ALL_LEVELS(COUNT_LEVEL);

// Generate level index constants: L0_INDEX, L1_INDEX, L2_INDEX, ...
#define GENERATE_LEVEL_INDEX(level_name, level_num, fields) \
  constexpr size_t level_name##_INDEX = level_num;
ALL_LEVELS(GENERATE_LEVEL_INDEX)

// ============================================================================
// FIELD METADATA - AUTO-GENERATED
// ============================================================================

// Count fields in each level
#define COUNT_FIELD(name, comment) +1

#define GENERATE_FIELD_COUNT_FOR_LEVEL(level_name, level_num, fields) \
  constexpr size_t level_name##_FIELD_COUNT = 0 fields(COUNT_FIELD);
ALL_LEVELS(GENERATE_FIELD_COUNT_FOR_LEVEL)

// Generate field offset enums for each level (scoped to avoid name collisions)
#define GENERATE_OFFSET_ENUM_FOR_LEVEL(level_name, level_num, fields) \
  namespace level_name##_FieldOffset {                                \
    enum : size_t {                                                   \
      fields(GENERATE_FIELD_OFFSET_##level_name)                      \
    };                                                                \
  }

#define GENERATE_FIELD_OFFSET_L0(name, comment) name,
#define GENERATE_FIELD_OFFSET_L1(name, comment) name,
#define GENERATE_FIELD_OFFSET_L2(name, comment) name,

ALL_LEVELS(GENERATE_OFFSET_ENUM_FOR_LEVEL)

// ============================================================================
// DATA STRUCTURES - AUTO-GENERATED
// ============================================================================

// Generate LevelNData structs with all fields as float
#define GENERATE_STRUCT_FIELD(name, comment) float name;

#define GENERATE_LEVEL_DATA_STRUCT(level_name, level_num, fields) \
  struct Level##level_num##Data {                                 \
    fields(GENERATE_STRUCT_FIELD)                                 \
  };
ALL_LEVELS(GENERATE_LEVEL_DATA_STRUCT)

// ============================================================================
// CAPACITY AND FIELD CONFIGURATION
// ============================================================================

// Field counts per level
#define GENERATE_FIELD_COUNT_ENTRY(level_name, level_num, fields) \
  level_name##_FIELD_COUNT,
constexpr size_t FIELDS_PER_LEVEL[LEVEL_COUNT] = {
    ALL_LEVELS(GENERATE_FIELD_COUNT_ENTRY)};

// Max capacities per level (from LEVEL_CONFIGS)
constexpr size_t MAX_ROWS_PER_LEVEL[LEVEL_COUNT] = {
    LEVEL_CONFIGS[0].max_capacity(),
    LEVEL_CONFIGS[1].max_capacity(),
    LEVEL_CONFIGS[2].max_capacity(),
};

// ============================================================================
// TIME INDEX CONVERSION
// ============================================================================

// Convert time to index for specific level
inline size_t time_to_index(size_t level_idx, uint8_t hour, uint8_t minute, uint8_t second, uint8_t millisecond) {
  const LevelTimeConfig &cfg = LEVEL_CONFIGS[level_idx];

  switch (cfg.unit) {
  case TimeUnit::MILLISECOND: {
    const size_t ms = time_to_trading_milliseconds(hour, minute, second, millisecond);
    return ms / cfg.interval;
  }
  case TimeUnit::SECOND: {
    const size_t sec = time_to_trading_seconds(hour, minute, second);
    return sec / cfg.interval;
  }
  case TimeUnit::MINUTE: {
    const size_t sec = time_to_trading_seconds(hour, minute, second);
    return (sec / 60) / cfg.interval;
  }
  case TimeUnit::HOUR: {
    const size_t sec = time_to_trading_seconds(hour, minute, second);
    return (sec / 3600) / cfg.interval;
  }
  }
  return 0;
}
