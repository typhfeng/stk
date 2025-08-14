#pragma once

#include "define/CBuffer.hpp"
#include "define/Dtype.hpp"
#include "math/LimitOrderBook.hpp"
#include "sample/ResampleRunBar.hpp"
#include <cstdint>
#include <string>
#include <vector>

// #define FILL_GAP_SNAPSHOT

class TechnicalAnalysis {
public:
  TechnicalAnalysis(size_t capacity);
  ~TechnicalAnalysis();

  // Main interface - processes single snapshot with richer info
  void ProcessSingleSnapshot(const Table::Snapshot_Record &snapshot);

  // Export functionality
  void DumpSnapshotCSV(const std::string &asset_code, const std::string &output_dir, size_t last_n = 0) const;
  void DumpBarCSV(const std::string &asset_code, const std::string &output_dir, size_t last_n = 0) const;

  // Access to internal data for size reporting
  size_t GetSnapshotCount() const { return snapshots_.size(); }
  size_t GetBarCount() const { return bars.size(); }

private:
  // Core unified processing function
  inline void ProcessSnapshotInternal(const Table::Snapshot_Record &snapshot);

  // Analysis functions
  inline void AnalyzeSnapshot(const Table::Snapshot_Record &snapshot);
  inline void AnalyzeRunBar(const Table::RunBar_Record &bar);

#ifdef FILL_GAP_SNAPSHOT
  // Gap filling
  inline void GetGapSnapshot(uint32_t timestamp);
  bool has_previous_snapshot_ = false;
  uint32_t last_processed_time_ = 0;
  Table::Snapshot_Record last_snapshot_;
  Table::Snapshot_Record gap_snapshot_;
#endif

  Table::RunBar_Record resampled_bar_;

  // Data storage
  std::vector<Table::Snapshot_Record> snapshots_;
  std::vector<Table::RunBar_Record> bars;

  // Check new session start
  uint8_t last_market_hour_ = 255;
  uint8_t last_market_minute_ = 255;
  uint8_t market_state_ = 0; // state: 0: market-close, 1: pre-market, 2: market, 3: post-market
  bool is_session_start_ = false;
  inline void UpdateMarketState(const Table::Snapshot_Record &snapshot);

  // Snapshot data ================================================================================
  LimitOrderBook<BLen> lob;
  CBuffer<uint16_t, 1> snapshot_year_;
  CBuffer<uint8_t, 1> snapshot_month_;
  CBuffer<uint8_t, 1> snapshot_day_;
  CBuffer<uint8_t, 1> snapshot_hour_;
  CBuffer<uint8_t, 1> snapshot_minute_;
  CBuffer<uint8_t, 1> snapshot_second_;
  CBuffer<uint16_t, BLen> snapshot_delta_t_;
  CBuffer<float, BLen> snapshot_prices_;
  CBuffer<float, BLen> snapshot_volumes_;
  CBuffer<float, BLen> snapshot_turnovers_;
  CBuffer<float, BLen> snapshot_vwaps_;
  CBuffer<uint8_t, BLen> snapshot_directions_; // 0: buy, 1: sell (vwap or last trade direction)
  CBuffer<float, BLen> snapshot_spreads_;
  CBuffer<float, BLen> snapshot_mid_prices_;

  // Resample data ================================================================================
  float resample_bar_volume_ = 0.0f;
  float resample_bar_turnover_ = 0.0f;
  ResampleRunBar<BLen> ResampleRunBar_;
  CBuffer<uint16_t, BLen> bar_delta_t_;
  CBuffer<float, BLen> bar_opens_;
  CBuffer<float, BLen> bar_highs_;
  CBuffer<float, BLen> bar_lows_;
  CBuffer<float, BLen> bar_closes_;
  CBuffer<float, BLen> bar_vwaps_;

  // daily data ===================================================================================
  CBuffer<uint16_t, 1> daily_year_;
  CBuffer<uint8_t, 1> daily_month_;
  CBuffer<uint8_t, 1> daily_day_;

  // features =============================================
  CBuffer<float, BLen> norm_spread_;
  CBuffer<std::array<float, 5>, BLen> norm_ofi_ask_;
  CBuffer<std::array<float, 5>, BLen> norm_ofi_bid_;
  // Analysis buffers for resample bar data

  // Limit Order Book for snapshot analysis
};

// Configuration constants
inline constexpr float PRICE_EPSILON = 1e-6f;
