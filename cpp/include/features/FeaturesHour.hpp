#pragma once

#include "backend/FeatureStore.hpp"
#include "backend/FeatureStoreConfig.hpp"
#include "lob/LimitOrderBookDefine.hpp"

// ============================================================================
// LEVEL 2 FEATURES - Hour-level Support/Resistance (1hour intervals)
// ============================================================================

class FeaturesHour {
private:
  const LOB_Feature *lob_feature_;
  GlobalFeatureStore *global_store_;
  size_t asset_id_;

  // Time index tracking
  size_t last_time_idx_ = 0;

  // Accumulation state for support/resistance
  float high_ = -1e9f;
  float low_ = 1e9f;
  float close_ = 0.0f;
  double sum_high_weighted_ = 0.0; // weighted high prices
  double sum_low_weighted_ = 0.0;  // weighted low prices
  double sum_weights_ = 0.0;
  double sum_buy_volume_ = 0.0;
  double sum_sell_volume_ = 0.0;
  uint32_t tick_count_ = 0;

public:
  FeaturesHour(const LOB_Feature *lob_feature,
               GlobalFeatureStore *store,
               size_t asset_id)
      : lob_feature_(lob_feature),
        global_store_(store),
        asset_id_(asset_id) {}

  void reset() {
    high_ = -1e9f;
    low_ = 1e9f;
    close_ = 0.0f;
    sum_high_weighted_ = 0.0;
    sum_low_weighted_ = 0.0;
    sum_weights_ = 0.0;
    sum_buy_volume_ = 0.0;
    sum_sell_volume_ = 0.0;
    tick_count_ = 0;
  }

  // Compute and store - called every tick, extracts data from LOB_Feature
  void compute_and_store() {
    if (!lob_feature_->depth_updated || !global_store_)
      return;

    const auto &depth_buffer = lob_feature_->depth_buffer;
    if (depth_buffer.size() < 2 * LOB_FEATURE_DEPTH_LEVELS)
      return;

    // Extract data from LOB_Feature
    const Level *best_ask_level = depth_buffer[LOB_FEATURE_DEPTH_LEVELS - 1];
    const Level *best_bid_level = depth_buffer[LOB_FEATURE_DEPTH_LEVELS];
    const float best_bid_price = static_cast<float>(best_bid_level->price) * 0.01f;
    const float best_ask_price = static_cast<float>(best_ask_level->price) * 0.01f;
    const float mid_price = (best_bid_price + best_ask_price) * 0.5f;
    const float bid_volume = static_cast<float>(std::abs(best_bid_level->net_quantity));
    const float ask_volume = static_cast<float>(std::abs(best_ask_level->net_quantity));

    const size_t curr_time_idx = time_to_index(L2_INDEX, lob_feature_->hour, lob_feature_->minute,
                                               lob_feature_->second, lob_feature_->millisecond);

    // Time index changed - push accumulated data and reset
    if (curr_time_idx != last_time_idx_) {
      if (tick_count_ > 0 && last_time_idx_ > 0) {
        Level2Data data = {};
        data.timestamp = 0; // TODO

        // Support level: weighted average of low prices
        data.support_level = (sum_weights_ > 0.0) ? static_cast<float>(sum_low_weighted_ / sum_weights_) : low_;

        // Resistance level: weighted average of high prices
        data.resistance_level = (sum_weights_ > 0.0) ? static_cast<float>(sum_high_weighted_ / sum_weights_) : high_;

        // Pivot point: traditional calculation
        data.pivot_point = (high_ + low_ + close_) / 3.0f;

        // Price range
        data.price_range = high_ - low_;

        // Dominant side: -1 (sell), 0 (neutral), +1 (buy)
        const double total_volume = sum_buy_volume_ + sum_sell_volume_;
        if (total_volume > 0.0) {
          const double imbalance = (sum_buy_volume_ - sum_sell_volume_) / total_volume;
          if (imbalance > 0.1) {
            data.dominant_side = 1.0f; // buy side dominant
          } else if (imbalance < -0.1) {
            data.dominant_side = -1.0f; // sell side dominant
          } else {
            data.dominant_side = 0.0f; // neutral
          }
        } else {
          data.dominant_side = 0.0f;
        }

        global_store_->push(L2_INDEX, asset_id_, last_time_idx_, &data);
      }
      reset();
      last_time_idx_ = curr_time_idx;
    }

    // Accumulate current tick
    close_ = mid_price;
    if (mid_price > high_)
      high_ = mid_price;
    if (mid_price < low_)
      low_ = mid_price;

    // Weight by volume for support/resistance calculation
    const float weight = bid_volume + ask_volume;
    sum_high_weighted_ += high_ * weight;
    sum_low_weighted_ += low_ * weight;
    sum_weights_ += weight;

    sum_buy_volume_ += bid_volume;
    sum_sell_volume_ += ask_volume;
    tick_count_++;
  }
};
