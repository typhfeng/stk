#pragma once

#include "define/CBuffer.hpp"
#include "define/Dtype.hpp"
#include "math/feature/LimitOrderBook.hpp"
#include "math/sample/ResampleRunBar.hpp"
#include <cstdint>
#include <string>
#include <vector>

// #define FILL_GAP_SNAPSHOT

class AnalysisHighFrequency {
public:
  AnalysisHighFrequency(size_t capacity);
  ~AnalysisHighFrequency();

  // Main interface - processes single snapshot with richer info
  void ProcessSingleSnapshot(Table::Snapshot_Record &snapshot);

  // Export functionality
  void DumpSnapshotCSV(const std::string &asset_code, const std::string &output_dir, size_t last_n = 0) const;
  void DumpBarCSV(const std::string &asset_code, const std::string &output_dir, size_t last_n = 0) const;

  // Access to internal data for size reporting
  size_t GetSnapshotCount() const { return snapshots_.size(); }
  size_t GetBarCount() const { return bars.size(); }

private:
  // Core unified processing function
  inline void ProcessSnapshotInternal(Table::Snapshot_Record &snapshot);

  // Analysis functions
  inline void AnalyzeSnapshot(Table::Snapshot_Record &snapshot);
  inline void AnalyzeRunBar(const Table::RunBar_Record &bar);

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

// Configuration constants
inline constexpr float PRICE_EPSILON = 1e-6f;
