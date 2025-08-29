#pragma once

#include <cstdint>

namespace L2 {
namespace Parallel {

// Simplified configuration for high-performance processing
struct ProcessingConfig {
  uint32_t decompression_threads = 1;
  uint32_t decompression_buffers = 3;
  
  const char *input_base = "/mnt/dev/sde/A_stock/L2";
  const char *output_base = "../../../output/L2_binary"; // "/mnt/dev/sde/A_stock/L2_binary";
  const char *temp_base = "../../../output/tmp";
  
  // Debug option to skip decompression and encode directly from input_base
  bool skip_decompression = true;
};

// Global configuration instance
extern ProcessingConfig g_config;

} // namespace Parallel
} // namespace L2
