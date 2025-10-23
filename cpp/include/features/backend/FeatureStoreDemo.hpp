#pragma once

#include "./FeatureStore.hpp"
#include "./FeatureStoreConfig.hpp"

// Level 0 has no trigger - always processes base events

// Level 1 trigger - user defines business logic
inline bool level1_trigger() {
  // Example: trigger every 100 level 0 events
  static size_t counter = 0;
  return (++counter % 100) == 0;
}

// Level 2 trigger - user defines business logic
inline bool level2_trigger() {
  // Example: trigger every 1000 level 0 events
  static size_t counter = 0;
  return (++counter % 1000) == 0;
}

// Level 0 updater - processes raw market data
inline void level0_updater(EventDrivenFeatureStore &store, size_t parent_idx) {
  (void)parent_idx; // Level 0 has no parent

  // User creates level 0 features from raw market data
  Level0Features data = {};

  // Example: populate with actual market data
  data.last_price = 100.25f;
  data.bid1 = 100.24f;
  data.ask1 = 100.26f;
  data.volume = 1000;
  data.bid1_size = 500;
  data.ask1_size = 600;
  data.spread = data.ask1 - data.bid1;
  data.mid_price = (data.bid1 + data.ask1) / 2.0f;
  data.is_uptick = data.last_price > 100.20f;
  data.micro_price = data.bid1 + (data.ask1 - data.bid1) * 0.5f;
  data.order_imbalance = static_cast<float>(data.bid1_size - data.ask1_size) / (data.bid1_size + data.ask1_size);
  data.trade_sign = data.last_price > data.mid_price ? 1.0f : -1.0f;
  data.aggressive_buy_vol = data.trade_sign > 0 ? data.volume : 0;
  data.aggressive_sell_vol = data.trade_sign < 0 ? data.volume : 0;
  data.effective_spread = std::abs(data.last_price - data.mid_price) * 2.0f;
  data.realized_spread = data.effective_spread * 0.8f;
  data.price_impact = data.effective_spread * 0.3f;
  data.event_id = 12345;

  // MACRO-GENERATED push_row automatically handles field pushing
  store.push_row(L0_INDEX, data, 0);
}

// Level 1 updater - aggregates from level 0
inline void level1_updater(EventDrivenFeatureStore &store, size_t parent_idx) {
  Level1Features data = {};

  // Access level 0 data for aggregation
  const auto &l0_store = store.get_store(L0_INDEX);
  if (l0_store.size() == 0)
    return;

  // Example aggregation: compute VWAP, TWAP, etc. from recent level 0 data
  size_t window_size = std::min<size_t>(100, l0_store.size());
  size_t start_idx = l0_store.size() - window_size;

  double sum_pv = 0.0, sum_v = 0.0, sum_p = 0.0;
  double high = 0.0, low = 1e9;
  uint64_t total_volume = 0, total_trades = 0;
  uint32_t buy_trades = 0, sell_trades = 0;

  // Access level 0 columns for aggregation
  const float *prices = l0_store.column_data<float>(0);        // last_price
  const uint32_t *volumes = l0_store.column_data<uint32_t>(3); // volume
  const float *trade_signs = l0_store.column_data<float>(11);  // trade_sign

  for (size_t i = start_idx; i < l0_store.size(); ++i) {
    float price = prices[i];
    uint32_t volume = volumes[i];
    float sign = trade_signs[i];

    sum_pv += price * volume;
    sum_v += volume;
    sum_p += price;
    high = std::max(high, static_cast<double>(price));
    low = std::min(low, static_cast<double>(price));
    total_volume += volume;
    total_trades++;

    if (sign > 0)
      buy_trades++;
    else if (sign < 0)
      sell_trades++;
  }

  // Compute aggregated features
  data.vwap = sum_v > 0 ? sum_pv / sum_v : 0.0;
  data.twap = window_size > 0 ? sum_p / window_size : 0.0;
  data.high = high;
  data.low = low;
  data.open = window_size > 0 ? static_cast<double>(prices[start_idx]) : 0.0;
  data.close = l0_store.size() > 0 ? static_cast<double>(prices[l0_store.size() - 1]) : 0.0;
  data.total_volume = total_volume;
  data.total_trades = total_trades;
  data.buy_trades = buy_trades;
  data.sell_trades = sell_trades;

  // Compute derived metrics
  data.price_volatility = (high - low) / data.twap;
  data.volume_volatility = static_cast<double>(total_volume) / window_size;
  data.skewness = 0.0;    // User implements proper skewness calculation
  data.kurtosis = 0.0;    // User implements proper kurtosis calculation
  data.avg_spread = 0.02; // User computes from level 0 spread data
  data.avg_depth = 1000.0;
  data.order_flow_imbalance = static_cast<double>(buy_trades - sell_trades) / total_trades;
  data.rsi = 50.0; // User implements RSI calculation
  data.momentum_short = (data.close - data.open) / data.open;
  data.momentum_long = data.momentum_short * 0.5;
  data.event_id = 67890;

  // MACRO-GENERATED push_row automatically handles field pushing
  store.push_row(L1_INDEX, data, parent_idx);
}

// Level 2 updater - aggregates from level 1
inline void level2_updater(EventDrivenFeatureStore &store, size_t parent_idx) {
  Level2Features data = {};

  // Access level 1 data for higher-level aggregation
  const auto &l1_store = store.get_store(L1_INDEX);
  if (l1_store.size() == 0)
    return;

  // Example: compute long-term statistics from level 1 data
  size_t window_size = std::min<size_t>(50, l1_store.size());
  size_t start_idx = l1_store.size() - window_size;

  // Access level 1 columns
  const double *vwaps = l1_store.column_data<double>(0);       // vwap
  const double *highs = l1_store.column_data<double>(2);       // high
  const double *lows = l1_store.column_data<double>(3);        // low
  const uint64_t *volumes = l1_store.column_data<uint64_t>(6); // total_volume

  double sum_vwap = 0.0, max_high = 0.0, min_low = 1e9;
  uint64_t sum_volume = 0;

  for (size_t i = start_idx; i < l1_store.size(); ++i) {
    sum_vwap += vwaps[i];
    max_high = std::max(max_high, highs[i]);
    min_low = std::min(min_low, lows[i]);
    sum_volume += volumes[i];
  }

  // Compute long-term aggregated features
  data.vwap = window_size > 0 ? sum_vwap / window_size : 0.0;
  data.high = max_high;
  data.low = min_low;
  data.open = window_size > 0 ? vwaps[start_idx] : 0.0;
  data.close = l1_store.size() > 0 ? vwaps[l1_store.size() - 1] : 0.0;
  data.total_volume = sum_volume;
  data.total_trades = window_size * 100; // Estimate

  // Long-term derived features
  data.volatility = (max_high - min_low) / data.vwap;
  data.return_rate = (data.close - data.open) / data.open;
  data.volume_profile_shape = 1.0; // User implements volume profile analysis
  data.participation_rate = 0.15;
  data.sector_correlation = 0.75;
  data.market_beta = 1.2;
  data.relative_strength = 0.85;
  data.funding_rate_impact = 0.01;
  data.options_flow_impact = 0.05;
  data.event_id = 11111;

  // MACRO-GENERATED push_row automatically handles field pushing
  store.push_row(L2_INDEX, data, parent_idx);
}

// auto &store = get_feature_store();
// // NOTE: All levels auto-initialized in constructor - no manual init needed!
//
// // 1. Set user-defined trigger and update functions
// store.set_updater(L0_INDEX, level0_updater);
//
// store.set_trigger(L1_INDEX, level1_trigger);
// store.set_updater(L1_INDEX, level1_updater);
//
// store.set_trigger(L2_INDEX, level2_trigger);
// store.set_updater(L2_INDEX, level2_updater);
//
// // 2. Event-driven processing (runs billions of times)
// // MACRO-GENERATED on_event() automatically scales to all levels
// for (int i = 0; i < 1000000; ++i) {
//   store.on_event(); // Fully inline optimized - zero overhead scaling
// }
//
// // 3. Export complete feature matrix for ML training
// // MACRO-GENERATED tensor export automatically includes all levels
// auto tensor = store.to_expanded_tensor();
//
// // 4. Cleanup when done
// EventDrivenFeatureStore::free_expanded_tensor(tensor);
