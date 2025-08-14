// 2021-Deep Learning for Market by Order Data
// https://arxiv.org/abs/2102.08811

// 2025-An Efficient deep learning model to Predict Stock Price Movement Based on Limit Order Book
// https://arxiv.org/abs/2505.22678

#pragma once

#include "define/CBuffer.hpp"
#include "define/Dtype.hpp"
#include "misc/print.hpp"

#define TICK_SIZE 0.01f

template <size_t N> // compiler auto derive
class LimitOrderBook {
public:
  // Constructor
  explicit LimitOrderBook(
      CBuffer<uint16_t, 1> *year,
      CBuffer<uint8_t, 1> *month,
      CBuffer<uint8_t, 1> *day,
      CBuffer<uint8_t, 1> *hour,
      CBuffer<uint8_t, 1> *minute,
      CBuffer<uint8_t, 1> *second,
      CBuffer<uint16_t, N> *delta_t,
      CBuffer<float, N> *prices,
      CBuffer<float, N> *volumes,
      CBuffer<float, N> *turnovers,
      CBuffer<float, N> *vwaps,
      CBuffer<uint8_t, N> *directions,
      CBuffer<float, N> *spreads,
      CBuffer<float, N> *mid_prices)
      : year_(year),
        month_(month),
        day_(day),
        hour_(hour),
        minute_(minute),
        second_(second),
        delta_t_(delta_t),
        prices_(prices),
        volumes_(volumes),
        turnovers_(turnovers),
        vwaps_(vwaps),
        directions_(directions),
        spreads_(spreads),
        mid_prices_(mid_prices) {}

  inline void update(const Table::Snapshot_Record *snapshot, bool is_session_start) {
    const uint16_t delta_t = static_cast<uint16_t>(is_session_start ? 0 : (snapshot->seconds_in_day - last_seconds_in_day));
    const float best_bid_price = snapshot->bid_price_ticks[0];
    const float best_ask_price = snapshot->ask_price_ticks[0];

    // Calculate derived metrics with conditional moves instead of branches
    const float mid_price = (best_bid_price + best_ask_price) * 0.5f;
    const float spread = best_ask_price - best_bid_price;

    // Optimize volume calculations
    const float volume = static_cast<float>(snapshot->volume) * 100.0f; // hands -> shares
    const float turnover = static_cast<float>(snapshot->turnover);
    const float vwap = (volume > 0) ? (turnover / volume) : vwaps_->back();
    // first check vwap dir(avg price up/down), if equal, use last trade dir during n seconds
    const uint8_t dir = static_cast<uint8_t>(is_session_start ? 0 : (vwap == vwaps_->back() ? snapshot->direction : (vwap < vwaps_->back())));

    // println(static_cast<int>(snapshot->direction), static_cast<int>(dir), delta_t, snapshot->latest_price_tick, vwap, volume, spread, mid_price);

    // Batch update analysis buffers for better cache locality
    year_->push_back(snapshot->year);
    month_->push_back(snapshot->month);
    day_->push_back(snapshot->day);
    hour_->push_back(snapshot->hour);
    minute_->push_back(snapshot->minute);
    second_->push_back(snapshot->second);
    delta_t_->push_back(delta_t);
    prices_->push_back(snapshot->latest_price_tick);
    volumes_->push_back(volume);
    turnovers_->push_back(turnover);
    vwaps_->push_back(vwap);
    directions_->push_back(dir);
    spreads_->push_back(spread);
    mid_prices_->push_back(mid_price);

    last_seconds_in_day = snapshot->seconds_in_day;
  }

private:
  uint32_t last_seconds_in_day = 0;
  uint8_t last_ = 0;

  CBuffer<uint16_t, 1> *year_;
  CBuffer<uint8_t, 1> *month_;
  CBuffer<uint8_t, 1> *day_;
  CBuffer<uint8_t, 1> *hour_;
  CBuffer<uint8_t, 1> *minute_;
  CBuffer<uint8_t, 1> *second_;
  CBuffer<uint16_t, N> *delta_t_;
  CBuffer<float, N> *prices_;
  CBuffer<float, N> *volumes_;
  CBuffer<float, N> *turnovers_;
  CBuffer<float, N> *vwaps_;
  CBuffer<uint8_t, N> *directions_;
  CBuffer<float, N> *spreads_;
  CBuffer<float, N> *mid_prices_;
};
