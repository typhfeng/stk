// #include <filesystem>
// #include <fstream>
// #include <iomanip>
// #include <iostream>
// #include <sstream>
// 
#include "define/Dtype.hpp"
#include "AnalysisHighFrequency.hpp"
// 
// #include "math/feature/LimitOrderBook.hpp" // LOB
// #include "math/sample/ResampleRunBar.hpp" // resample bar
// 
// #define PRINT_SNAPSHOT
// #define PRINT_BAR

#if defined(PRINT_BAR) || defined(PRINT_SNAPSHOT)
#include "misc/print.hpp"
#endif

AnalysisHighFrequency::AnalysisHighFrequency(size_t capacity)
 {
  // // Reserve memory for efficient operation - reduce reallocations
  // snapshots_.reserve(capacity);
  // bars.reserve(15 * 250 * trade_hrs_in_a_day * 3600 / RESAMPLE_BASE_PERIOD); // 15 years of resampled bars
}

AnalysisHighFrequency::~AnalysisHighFrequency() {
  // Cleanup completed
}
