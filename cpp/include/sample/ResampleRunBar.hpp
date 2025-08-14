#pragma once

// System headers
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <numeric>
#include <vector>

// Project headers
#include "define/CBuffer.hpp"
#include "define/Dtype.hpp"

template <size_t N> // compiler auto derive
class ResampleRunBar {
public:
  explicit ResampleRunBar(
      CBuffer<uint8_t, 1> *snapshot_day,
      CBuffer<uint16_t, N> *snapshot_delta_t,
      CBuffer<float, N> *snapshot_prices,
      CBuffer<float, N> *snapshot_volumes,
      CBuffer<float, N> *snapshot_turnovers,
      CBuffer<uint8_t, N> *snapshot_directions,
      CBuffer<uint16_t, N> *bar_timedelta,
      CBuffer<float, N> *bar_open,
      CBuffer<float, N> *bar_high,
      CBuffer<float, N> *bar_low,
      CBuffer<float, N> *bar_close,
      CBuffer<float, N> *bar_vwap)
      : snapshot_day_(snapshot_day),
        snapshot_delta_t_(snapshot_delta_t),
        snapshot_prices_(snapshot_prices),
        snapshot_volumes_(snapshot_volumes),
        snapshot_turnovers_(snapshot_turnovers),
        snapshot_directions_(snapshot_directions),
        bar_timedelta_(bar_timedelta),
        bar_open_(bar_open),
        bar_high_(bar_high),
        bar_low_(bar_low),
        bar_close_(bar_close),
        bar_vwap_(bar_vwap) {
    daily_snapshot_label.reserve(int(3600 / MIN_DATA_BASE_PERIOD * trade_hrs_in_a_day));
    daily_snapshot_volume.reserve(int(3600 / MIN_DATA_BASE_PERIOD * trade_hrs_in_a_day));
  }

  inline bool process() {
    // ---- Label Calculation ----
    label_long = snapshot_directions_->back() == 0;
    buy_vol = label_long ? static_cast<float>(snapshot_volumes_->back()) : 0.0f;
    sell_vol = label_long ? 0.0f : static_cast<float>(snapshot_volumes_->back());

    // ---- New Day Handling ----
    auto date = snapshot_day_->back();
    if (date != prev_date) [[unlikely]] {
      if (!daily_snapshot_label.empty()) [[likely]] {
        daily_thresh = find_run_threshold();
        ema_thresh = (ema_thresh < 0.0f) ? daily_thresh : alpha * daily_thresh + (1 - alpha) * ema_thresh;
      }
      daily_snapshot_label.clear();
      daily_snapshot_volume.clear();
      daily_bar_count = 0;
      prev_date = date;
    }

    // ---- Update OHLC ----
    ohlc_open = (ohlc_open == 0.0f) ? snapshot_prices_->back() : ohlc_open;
    ohlc_high = std::max(ohlc_high, snapshot_prices_->back());
    ohlc_low = std::min(ohlc_low, snapshot_prices_->back());
    ohlc_close = snapshot_prices_->back();
    daily_snapshot_label.push_back(label_long);
    daily_snapshot_volume.push_back(snapshot_volumes_->back());

    // ---- Accumulate Volumes ----
    cumm_tdelta += snapshot_delta_t_->back();
    cumm_buy += buy_vol;
    cumm_sell += sell_vol;
    cumm_vol += snapshot_volumes_->back();
    cumm_turnover += snapshot_turnovers_->back();

    // ---- Check Bar Formation ----
    float theta = std::max(cumm_buy, cumm_sell);
    float threshold = (ema_thresh > 0) ? ema_thresh : 0.0f;

    if (theta >= threshold) [[unlikely]] {
      bar_timedelta_->push_back(cumm_tdelta);
      bar_open_->push_back(ohlc_open);
      bar_high_->push_back(ohlc_high);
      bar_low_->push_back(ohlc_low);
      bar_close_->push_back(ohlc_close);
      if (cumm_vol > 0) [[likely]] {
        bar_vwap_->push_back(cumm_turnover / cumm_vol);
      } else {
        bar_vwap_->push_back(ohlc_close);
      }

      // Reset state
      ohlc_open = snapshot_prices_->back();
      ohlc_high = snapshot_prices_->back();
      ohlc_low = snapshot_prices_->back();
      cumm_buy = cumm_sell = cumm_vol = cumm_turnover = 0.0f;
      cumm_tdelta = 0;
      daily_bar_count++;
      return true;
    }

    return false;
  }

private:  
  int p_ori = MIN_DATA_BASE_PERIOD; // original sampling period (seconds)
  int p_tar = RESAMPLE_BASE_PERIOD; // target bar length (seconds)
  int expected_num_daily_samples = int(3600 * trade_hrs_in_a_day / p_tar);
  int tolerance = static_cast<int>(expected_num_daily_samples * 0.05);

  CBuffer<uint8_t, 1> *snapshot_day_;
  CBuffer<uint16_t, N> *snapshot_delta_t_;
  CBuffer<float, N> *snapshot_prices_;
  CBuffer<float, N> *snapshot_volumes_;
  CBuffer<float, N> *snapshot_turnovers_;
  CBuffer<uint8_t, N> *snapshot_directions_;

  CBuffer<uint16_t, N> *bar_timedelta_;
  CBuffer<float, N> *bar_open_;
  CBuffer<float, N> *bar_high_;
  CBuffer<float, N> *bar_low_;
  CBuffer<float, N> *bar_close_;
  CBuffer<float, N> *bar_vwap_;

  bool label_long;
  float buy_vol = 0.0f;
  float sell_vol = 0.0f;

  const float ema_days = RESAMPLE_EMA_DAYS;
  const float alpha = 2.0f / (ema_days + 1);
  float ema_thresh = -1.0f;

  float daily_thresh = 0.0f;
  int daily_bar_count = 0;
  uint8_t prev_date = 255;

  uint16_t cumm_tdelta = 0;
  float cumm_buy = 0.0f;
  float cumm_sell = 0.0f;
  float cumm_vol = 0.0f;
  float cumm_turnover = 0.0f;

  float ohlc_open = 0.0f;
  float ohlc_high = -std::numeric_limits<float>::infinity();
  float ohlc_low = std::numeric_limits<float>::infinity();
  float ohlc_close = 0.0f;

  std::vector<bool> daily_snapshot_label;
  std::vector<float> daily_snapshot_volume;

  inline int compute_sample_count(float x) {
    float acc_pos = 0.0f;
    float acc_neg = 0.0f;
    int num_daily_samples = 0;
    size_t n = daily_snapshot_volume.size();

    for (size_t i = 0; i < n; ++i) {
      if (daily_snapshot_label[i])
        acc_pos += daily_snapshot_volume[i];
      else
        acc_neg += daily_snapshot_volume[i];

      if (acc_pos >= x || acc_neg >= x) {
        ++num_daily_samples;
        acc_pos = 0.0f;
        acc_neg = 0.0f;
      }
    }
    return num_daily_samples;
  }

  inline float find_run_threshold() {
    if (daily_snapshot_label.empty()) [[unlikely]]
      return 0.0f;

    // use accum for safety
    float x_max = std::accumulate(daily_snapshot_volume.begin(), daily_snapshot_volume.end(), 0.0f);
    float x_min = *std::min_element(daily_snapshot_volume.begin(), daily_snapshot_volume.end());

    int max_iter = 20;
    float x_mid = 0.0f;
    for (int i = 0; i < max_iter; ++i) {
      x_mid = 0.5f * (x_min + x_max);
      int num_daily_samples = compute_sample_count(x_mid);

      if (std::abs(num_daily_samples - expected_num_daily_samples) <= tolerance || (x_max - x_min) < 100) {
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
