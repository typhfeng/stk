#pragma once

#include "backend/FeatureStore.hpp"
#include "backend/FeatureStoreConfig.hpp"
#include "lob/LimitOrderBookDefine.hpp"

// ============================================================================
// LEVEL 1 FEATURES - Minute-level OHLC (1min intervals)
// ============================================================================

class FeaturesMinute {
private:
  const LOB_Feature *lob_feature_;
  GlobalFeatureStore *global_store_;
  size_t asset_id_;

  // Time index tracking
  size_t last_time_idx_ = 0;

  // Accumulation state for OHLC
  float open_ = 0.0f;
  float high_ = -1e9f;
  float low_ = 1e9f;
  float close_ = 0.0f;
  double sum_pv_ = 0.0; // price * volume
  double sum_v_ = 0.0;  // volume
  uint32_t tick_count_ = 0;

public:
  FeaturesMinute(const LOB_Feature *lob_feature,
                 GlobalFeatureStore *store,
                 size_t asset_id)
      : lob_feature_(lob_feature),
        global_store_(store),
        asset_id_(asset_id) {}

  void reset() {
    open_ = 0.0f;
    high_ = -1e9f;
    low_ = 1e9f;
    close_ = 0.0f;
    sum_pv_ = 0.0;
    sum_v_ = 0.0;
    tick_count_ = 0;
  }

  // Compute and store - called every tick, extracts data from LOB_Feature
  void compute_and_store() {
    if (!lob_feature_->depth_updated || !global_store_)
      return;

    const auto &depth_buffer = lob_feature_->depth_buffer;
    if (depth_buffer.size() < 2 * LOB_FEATURE_DEPTH_LEVELS)
      return;

    // Extract mid price from LOB_Feature
    const Level *best_ask_level = depth_buffer[LOB_FEATURE_DEPTH_LEVELS - 1];
    const Level *best_bid_level = depth_buffer[LOB_FEATURE_DEPTH_LEVELS];
    const float best_bid_price = static_cast<float>(best_bid_level->price) * 0.01f;
    const float best_ask_price = static_cast<float>(best_ask_level->price) * 0.01f;
    const float mid_price = (best_bid_price + best_ask_price) * 0.5f;
    const float volume = 1.0f;

    const size_t curr_time_idx = time_to_index(L1_INDEX, lob_feature_->hour, lob_feature_->minute,
                                               lob_feature_->second, lob_feature_->millisecond);

    // Time index changed - push accumulated data and reset
    if (curr_time_idx != last_time_idx_) {
      if (tick_count_ > 0 && last_time_idx_ > 0) {
        Level1Data data = {};
        data.timestamp = 0; // TODO
        data.open = open_;
        data.high = high_;
        data.low = low_;
        data.close = close_;
        data.vwap = (sum_v_ > 0.0) ? static_cast<float>(sum_pv_ / sum_v_) : close_;
        data.volume = static_cast<float>(tick_count_);
        global_store_->push(L1_INDEX, asset_id_, last_time_idx_, &data);
      }
      reset();
      last_time_idx_ = curr_time_idx;
    }

    // Accumulate current tick
    if (tick_count_ == 0) {
      open_ = mid_price;
    }
    close_ = mid_price;
    if (mid_price > high_)
      high_ = mid_price;
    if (mid_price < low_)
      low_ = mid_price;
    sum_pv_ += mid_price * volume;
    sum_v_ += volume;
    tick_count_++;
  }
};
