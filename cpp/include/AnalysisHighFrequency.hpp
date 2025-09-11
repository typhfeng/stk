#pragma once

// #include "define/CBuffer.hpp"
// #include "define/Dtype.hpp"
// #include "math/feature/LimitOrderBook.hpp"
// #include "math/sample/ResampleRunBar.hpp"
// #include <cstdint>
// #include <string>
// #include <vector>

// #define FILL_GAP_SNAPSHOT

#include <cstddef>
class AnalysisHighFrequency {
public:
  AnalysisHighFrequency(size_t capacity);
  ~AnalysisHighFrequency();

private:
  // // Data storage
  // std::vector<Table::Snapshot_Record> snapshots_;
  // std::vector<Table::RunBar_Record> bars;

};
