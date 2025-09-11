#pragma once

#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <cstdint>
#include <string>
#include <vector>

#include "define/CBuffer.hpp"
#include "define/Dtype.hpp"
#include "math/feature/LimitOrderBook.hpp"
#include "math/sample/ResampleRunBar.hpp"

// #define FILL_GAP_SNAPSHOT
// #define PRINT_SNAPSHOT
// #define PRINT_BAR

#if defined(PRINT_BAR) || defined(PRINT_SNAPSHOT)
#include "misc/print.hpp"
#endif

// Configuration constants
inline constexpr float PRICE_EPSILON = 1e-6f;

// Helper to convert price ticks to actual prices
inline double TickToPrice(int16_t tick) {
  return tick * 0.01;
}

namespace {} // anonymous namespace

class AnalysisHighFrequency {
public:
  AnalysisHighFrequency(size_t capacity)
      : lob(
            &snapshot_delta_t_,
            &snapshot_prices_,
            &snapshot_volumes_,
            &snapshot_turnovers_,
            &snapshot_vwaps_,
            &snapshot_directions_,
            &snapshot_spreads_,
            &snapshot_mid_prices_),
        ResampleRunBar_(
            &snapshot_delta_t_,
            &snapshot_prices_,
            &snapshot_volumes_,
            &snapshot_turnovers_,
            &snapshot_directions_,
            &bar_delta_t_,
            &bar_opens_,
            &bar_highs_,
            &bar_lows_,
            &bar_closes_,
            &bar_vwaps_) {
    // Reserve memory for efficient operation - reduce reallocations
    snapshots_.reserve(capacity);
    bars.reserve(15 * 250 * trade_hrs_in_a_day * 3600 / RESAMPLE_BASE_PERIOD); // 15 years of resampled bars
  }

  ~AnalysisHighFrequency() {
    // Cleanup completed
  }

  // Main interface - processes single snapshot with richer info
  [[gnu::hot]] inline void ProcessSingleSnapshot(Table::Snapshot_Record &snapshot) {
    // Fill gaps by creating intermediate snapshots and processing each one
#ifdef FILL_GAP_SNAPSHOT
    if (has_previous_snapshot_) [[likely]] {
      uint32_t gap_time = last_processed_time_ + snapshot_interval;
      // NOTE: this also check the causality of last and current snapshot
      // making sure time between 2 days are not filled (as last(yesterday close) > current(today open))
      while (gap_time < snapshot.seconds_in_day) {
        GetGapSnapshot(gap_time);
        ProcessSnapshotInternal(gap_snapshot_);
        gap_time += snapshot_interval;
      }
    }
#endif

    // Process the actual incoming snapshot
    ProcessSnapshotInternal(snapshot);

#ifdef FILL_GAP_SNAPSHOT
    // Update state
    last_snapshot_ = snapshot;
    has_previous_snapshot_ = true;
    last_processed_time_ = snapshot.seconds_in_day;
#endif
  }

  // Export functionality
  inline void DumpSnapshotCSV(const std::string &asset_code, const std::string &output_dir, size_t last_n = 0) const {
    DumpRecordsToCSV(snapshots_, asset_code, output_dir, "snapshot_3s", last_n);
  }

  inline void DumpBarCSV(const std::string &asset_code, const std::string &output_dir, size_t last_n = 0) const {
    DumpRecordsToCSV(bars, asset_code, output_dir, "bar_resampled", last_n);
  }

  // Access to internal data for size reporting
  size_t GetSnapshotCount() const { return snapshots_.size(); }
  size_t GetBarCount() const { return bars.size(); }

private:
  // Core unified processing function
  [[gnu::hot]] inline void ProcessSnapshotInternal(Table::Snapshot_Record &snapshot) {
    UpdateMarketState(snapshot);

    if (market_state_ == 2) [[likely]] { // in market open
      AnalyzeSnapshot(snapshot);
      snapshots_.emplace_back(snapshot);

      if (ResampleRunBar_.process(snapshot, resampled_bar_)) {
        AnalyzeRunBar(resampled_bar_);
        bars.emplace_back(resampled_bar_);
      }
    }
  }

  // Analysis functions
  [[gnu::hot]] inline void AnalyzeSnapshot(Table::Snapshot_Record &snapshot) {
    lob.update(snapshot, is_session_start_);

    // Debug output
#ifdef PRINT_SNAPSHOT
    println(
        static_cast<int>(snapshot_year_.back()),
        static_cast<int>(snapshot_month_.back()),
        static_cast<int>(snapshot_day_.back()),
        static_cast<int>(snapshot_hour_.back()),
        static_cast<int>(snapshot_minute_.back()),
        static_cast<int>(snapshot_second_.back()),
        static_cast<int>(snapshot_delta_t_.back()),
        static_cast<float>(snapshot_prices_.back()),
        static_cast<float>(snapshot_vwaps_.back()),
        static_cast<float>(snapshot_mid_prices_.back()),
        static_cast<float>(snapshot_volumes_.back()),
        static_cast<int>(snapshot_directions_.back()),
        static_cast<float>(snapshot_spreads_.back()));
#endif
  }

  inline void AnalyzeRunBar(const Table::RunBar_Record &bar) {
    (void)bar; // Suppress unused parameter warning

#ifdef PRINT_BAR
    println(
        static_cast<int>(bar_delta_t_.back()),
        static_cast<int>(bar.year),
        static_cast<int>(bar.month),
        static_cast<int>(bar.day),
        static_cast<int>(bar.hour),
        static_cast<int>(bar.minute),
        static_cast<int>(bar.second),
        static_cast<float>(bar.open),
        static_cast<float>(bar.high),
        static_cast<float>(bar.low),
        static_cast<float>(bar.close),
        static_cast<float>(bar.vwap));
#endif
  }

  // Check new session start
  uint8_t last_market_hour_ = 255;
  uint8_t last_market_minute_ = 255;
  uint8_t market_state_ = 0; // state: 0: market-close, 1: pre-market, 2: market, 3: post-market
  bool is_session_start_ = false;

  [[gnu::hot]] inline void UpdateMarketState(const Table::Snapshot_Record &snapshot) {
    // States (no seconds):
    // 0 close: default
    // 1 pre-market: 09:15:01-09:25:00 (inclusive minutes)
    // 2 market: 09:30:01-11:30:01 and 13:00:01-14:57:00
    // 3 post-market: 14:57:01-15:00:01

    const uint8_t h = snapshot.hour;
    const uint8_t m = snapshot.minute;
    uint8_t new_state = market_state_;

    // Only recompute when minute changes to minimize work
    if ((h != last_market_hour_) | (m != last_market_minute_)) [[unlikely]] {
      last_market_hour_ = h;
      last_market_minute_ = m;

      // Market time (most common) checked first
      if (((h == 9 && m >= 30) || h == 10 || (h == 11 && m <= 30) || h == 13 || (h == 14 && m <= 56))) [[likely]] {
        new_state = 2;
      } else if ((h == 14 && m >= 57) || (h == 15)) {
        // Post-market minute buckets
        new_state = 3;
      } else if (h == 9 && m >= 15 && m <= 25) [[unlikely]] {
        // Pre-market
        new_state = 1;
      } else {
        new_state = 0;
      }
    }

    is_session_start_ = (market_state_ != 2) && (new_state == 2);
    market_state_ = new_state;
  }

#ifdef FILL_GAP_SNAPSHOT
  inline void GetGapSnapshot(uint32_t timestamp) {
    // Preserve date and static price information from the last valid snapshot
    gap_snapshot_ = last_snapshot_;

    // Update intraday time components derived from the gap timestamp
    gap_snapshot_.seconds_in_day = static_cast<uint32_t>(timestamp);
    gap_snapshot_.hour = static_cast<uint8_t>(timestamp / 3600);
    gap_snapshot_.minute = static_cast<uint8_t>((timestamp % 3600) / 60);
    gap_snapshot_.second = static_cast<uint8_t>(timestamp % 60);

    // Reset trading activity for gap periods
    gap_snapshot_.trade_count = 0;
    gap_snapshot_.volume = 0;
    gap_snapshot_.turnover = 0;
  }

  Table::Snapshot_Record last_snapshot_;
  Table::Snapshot_Record gap_snapshot_;
  bool has_previous_snapshot_ = false;
  uint32_t last_processed_time_ = 0;
  static constexpr uint32_t snapshot_interval = 3; // 3 seconds
#endif

  Table::RunBar_Record resampled_bar_;

  // Data storage
  std::vector<Table::Snapshot_Record> snapshots_;
  std::vector<Table::RunBar_Record> bars;

  // Snapshot data ================================================================================
  LimitOrderBook<BLen> lob;
  CBuffer<uint16_t, BLen> snapshot_delta_t_;
  CBuffer<float, BLen> snapshot_prices_;
  CBuffer<float, BLen> snapshot_volumes_;
  CBuffer<float, BLen> snapshot_turnovers_;
  CBuffer<float, BLen> snapshot_vwaps_;
  CBuffer<uint8_t, BLen> snapshot_directions_; // 0: buy, 1: sell (vwap or last trade direction)
  CBuffer<float, BLen> snapshot_spreads_;
  CBuffer<float, BLen> snapshot_mid_prices_;

  // Resample data ================================================================================
  ResampleRunBar<BLen> ResampleRunBar_;
  CBuffer<uint16_t, BLen> bar_delta_t_;
  CBuffer<float, BLen> bar_opens_;
  CBuffer<float, BLen> bar_highs_;
  CBuffer<float, BLen> bar_lows_;
  CBuffer<float, BLen> bar_closes_;
  CBuffer<float, BLen> bar_vwaps_;

  // daily data ===================================================================================

  // features =============================================
  // Analysis buffers for resample bar data

  // Limit Order Book for snapshot analysis
};


// Helper to output a single field to CSV
template <typename T>
inline void OutputField(std::ostringstream &output, const T &field, bool &first) {
  if (!first)
    output << ",";
  first = false;

  if constexpr (std::is_same_v<T, int16_t>) {
    // Convert price ticks to prices for int16_t fields (likely price ticks)
    output << TickToPrice(field);
  } else if constexpr (std::is_integral_v<T>) {
    output << static_cast<int>(field);
  } else {
    output << field;
  }
}

// Helper to output array fields to CSV
template <typename T, size_t N>
inline void OutputArray(std::ostringstream &output, const T (&array)[N], bool &first) {
  for (size_t i = 0; i < N; ++i) {
    OutputField(output, array[i], first);
  }
}

// Generic CSV dump function that works with any record type using structured bindings
template <typename RecordType>
inline void DumpRecordsToCSV(const std::vector<RecordType> &records,
                             const std::string &asset_code,
                             const std::string &output_dir,
                             const std::string &suffix,
                             size_t last_n = 0) {
  if (records.empty())
    return;

  std::filesystem::create_directories(output_dir);
  std::string filename = output_dir + "/" + asset_code + "_" + suffix + ".csv";
  std::ofstream file(filename);

  if (!file.is_open()) {
    std::cerr << "Failed to create file: " << filename << "\n";
    return;
  }

  // Generate header based on record type
  if constexpr (std::is_same_v<RecordType, Table::Snapshot_Record>) {
    file << "index_1m,seconds,latest_price,trade_count,turnover,volume,";
    file << "bid_price_1,bid_price_2,bid_price_3,bid_price_4,bid_price_5,";
    file << "bid_vol_1,bid_vol_2,bid_vol_3,bid_vol_4,bid_vol_5,";
    file << "ask_price_1,ask_price_2,ask_price_3,ask_price_4,ask_price_5,";
    file << "ask_vol_1,ask_vol_2,ask_vol_3,ask_vol_4,ask_vol_5,direction\n";
  } else if constexpr (std::is_same_v<RecordType, Table::RunBar_Record>) {
    file << "year,month,day,hour,minute,second,open,high,low,close,vwap\n";
  }

  std::ostringstream batch_output;
  batch_output << std::fixed << std::setprecision(2);

  size_t start_index = (last_n == 0 || last_n >= records.size()) ? 0 : records.size() - last_n;
  for (size_t i = start_index; i < records.size(); ++i) {
    const auto &record = records[i];
    bool first = true;

    if constexpr (std::is_same_v<RecordType, Table::Snapshot_Record>) {
      // Manual field access instead of structured binding due to field count mismatch
      const auto &r = record;

      OutputField(batch_output, r.seconds_in_day, first);
      OutputField(batch_output, r.second, first);
      OutputField(batch_output, r.latest_price_tick, first);
      OutputField(batch_output, r.trade_count, first);
      OutputField(batch_output, r.turnover, first);
      OutputField(batch_output, r.volume, first);
      OutputArray(batch_output, r.bid_price_ticks, first);
      OutputArray(batch_output, r.bid_volumes, first);
      OutputArray(batch_output, r.ask_price_ticks, first);
      OutputArray(batch_output, r.ask_volumes, first);
      OutputField(batch_output, r.direction, first);

    } else if constexpr (std::is_same_v<RecordType, Table::RunBar_Record>) {
      auto [year, month, day, hour, minute, second, open, high, low, close, vwap] = record;

      OutputField(batch_output, year, first);
      OutputField(batch_output, month, first);
      OutputField(batch_output, day, first);
      OutputField(batch_output, hour, first);
      OutputField(batch_output, minute, first);
      OutputField(batch_output, second, first);
      OutputField(batch_output, open, first);
      OutputField(batch_output, high, first);
      OutputField(batch_output, low, first);
      OutputField(batch_output, close, first);
      OutputField(batch_output, vwap, first);
    }

    batch_output << "\n";
  }

  file << batch_output.str();
  file.close();

  size_t dumped_count = records.size() - start_index;
  std::cout << "Dumped " << dumped_count << " " << suffix << " records to " << filename << "\n";
}
