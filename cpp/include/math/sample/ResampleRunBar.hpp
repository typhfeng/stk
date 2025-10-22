#pragma once

// System headers
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <numeric>
#include <vector>

// Project headers
#include "codec/L2_DataType.hpp"

class ResampleRunBar {
public:
  explicit ResampleRunBar() {
    daily_labels.reserve(expected_num_daily_samples);
    daily_volumes.reserve(expected_num_daily_samples);
  }

  [[gnu::hot, gnu::always_inline]] inline bool resample(const L2::Order &order) {
    // Fast early exit for non-taker orders
    if (order.order_type != L2::OrderType::TAKER)
      return false;

    // ---- Optimized Label and Volume Calculation ----
    const bool is_bid = order.order_dir == L2::OrderDirection::BID;
    const uint32_t volume = order.volume;

    // Direct accumulation without intermediate variables
    if (is_bid) {
      cumm_buy += volume;
      label_long = true;
    } else {
      cumm_sell += volume;
      label_long = false;
    }

    // ---- Hot Path: Check Bar Formation First ----
    const uint32_t theta = std::max(cumm_buy, cumm_sell);
    const float threshold = std::max(ema_thresh, 0.0f);

    if (theta < threshold)
      return false; // Most common case - no bar formation

    // ---- Time Guard: Prevent Too Frequent Sampling ----
    const uint32_t current_timestamp = (static_cast<uint32_t>(order.hour) << 24) |
                                       (static_cast<uint32_t>(order.minute) << 16) |
                                       (static_cast<uint32_t>(order.second) << 8) |
                                       static_cast<uint32_t>(order.millisecond);
    const uint32_t time_diff_seconds = (current_timestamp >> 8) - (last_sample_timestamp >> 8);

    if (time_diff_seconds < L2::RESAMPLE_MIN_PERIOD) [[unlikely]]
      return false; // Time guard prevents sampling

    // ---- Bar Formation Confirmed - Reset State ----
    last_sample_timestamp = current_timestamp;
    cumm_buy = 0;
    cumm_sell = 0;
    ++daily_bar_count; // Increment daily bar counter

    // ---- New Day Processing ----
    const uint8_t hour = order.hour;
    if (hour == 9 && prev_hour != 9) [[unlikely]] {
      // std::cout << "[DAILY STATS] Previous day formed " << daily_bar_count << " bars" << std::endl;
      daily_bar_count = 1;

      if (!daily_labels.empty()) [[likely]] {
        daily_thresh = find_run_threshold();
        ema_thresh = (ema_thresh < 0.0f) ? daily_thresh : alpha * daily_thresh + (1.0f - alpha) * ema_thresh;
      }
      daily_labels.clear();
      daily_volumes.clear();
    }
    prev_hour = hour;

    // Add to daily tracking (after bar formation to reduce overhead on hot path)
    daily_labels.push_back(label_long);
    daily_volumes.push_back(volume);

    return true;
  }

private:
  int p_tar = L2::RESAMPLE_TARGET_PERIOD; // target bar length (seconds)
  int expected_num_daily_samples = int(3600 * L2::RESAMPLE_TRADE_HRS_PER_DAY / p_tar);
  int tolerance = static_cast<int>(expected_num_daily_samples * 0.05);

  std::vector<bool> daily_labels;
  std::vector<uint32_t> daily_volumes;

  bool label_long;

  const float ema_days = L2::RESAMPLE_EMA_DAYS_PERIOD;
  const float alpha = 2.0f / (ema_days + 1);
  float ema_thresh = L2::RESAMPLE_INIT_VOLUME_THD;

  float daily_thresh = 0.0f;
  uint8_t prev_hour = 255;

  uint32_t cumm_buy = 0;
  uint32_t cumm_sell = 0;

  // Time tracking for sampling frequency control
  uint32_t last_sample_timestamp = 0;

  // Daily statistics
  uint32_t daily_bar_count = 0;

  inline int compute_sample_count(float x) {
    float acc_pos = 0.0f;
    float acc_neg = 0.0f;
    int num_daily_samples = 0;
    size_t n = daily_volumes.size();

    for (size_t i = 0; i < n; ++i) {
      if (daily_labels[i])
        acc_pos += daily_volumes[i];
      else
        acc_neg += daily_volumes[i];

      if (acc_pos >= x || acc_neg >= x) {
        ++num_daily_samples;
        acc_pos = 0.0f;
        acc_neg = 0.0f;
      }
    }
    return num_daily_samples;
  }

  inline float find_run_threshold() {
    if (daily_labels.empty()) [[unlikely]]
      return 0.0f;

    // use accum for safety
    float x_max = std::accumulate(daily_volumes.begin(), daily_volumes.end(), 0.0f);
    float x_min = *std::min_element(daily_volumes.begin(), daily_volumes.end());

    int max_iter = 20;
    float x_mid = 0.0f;
    for (int i = 0; i < max_iter; ++i) {
      x_mid = 0.5f * (x_min + x_max);
      int num_daily_samples = compute_sample_count(x_mid);

      if (std::abs(num_daily_samples - expected_num_daily_samples) <= tolerance || (x_max - x_min) < 100.0f) {
        return x_mid;
      }

      if (num_daily_samples > expected_num_daily_samples)
        x_min = x_mid;
      else
        x_max = x_mid;
    }

    return 0.5f * (x_min + x_max);
  }
};
