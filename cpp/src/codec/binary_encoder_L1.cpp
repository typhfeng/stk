#include "codec/binary_encoder_L1.hpp"

#include <cassert>
#include <cstring>
#include <iostream>

extern "C" {
#include "package/miniz/miniz.h"
}

namespace BinaryEncoder {

// ============================================================================
// CONSTRUCTOR AND DESTRUCTOR
// ============================================================================

Encoder::Encoder() {
  // Pre-allocate buffers for efficiency
  buffer_.reserve(BUFFER_SIZE);
}

Encoder::~Encoder() {
  // Clean up completed
}

// ============================================================================
// CORE ENCODING FUNCTIONS
// ============================================================================

std::vector<uint8_t> Encoder::EncodeMonthSnapshots(const std::vector<L1::BinaryRecord> &records) {
  if (records.empty()) {
    return {};
  }

  // Create a copy for differential encoding
  std::vector<L1::BinaryRecord> encoded_records = records;

  // Apply differential encoding
  ApplyDifferentialEncoding(encoded_records);

  // Convert to raw bytes
  size_t total_size = encoded_records.size() * sizeof(L1::BinaryRecord);
  std::vector<uint8_t> raw_bytes(total_size);
  std::memcpy(raw_bytes.data(), encoded_records.data(), total_size);

  // Compress the data
  return CompressData(raw_bytes);
}

void Encoder::ApplyDifferentialEncoding(std::vector<L1::BinaryRecord> &records) {
  if (records.size() <= 1) {
    return;
  }

  // Apply differential encoding starting from the second record
  // Process in reverse order to avoid overwriting data we need
  for (size_t i = records.size() - 1; i > 0; --i) {
    // Apply differential encoding for day (DIFF_FIELDS[1] = true)
    records[i].day = static_cast<uint8_t>(records[i].day - records[i - 1].day);

    // Apply differential encoding for time_s (DIFF_FIELDS[2] = true)
    records[i].time_s = static_cast<uint16_t>(records[i].time_s - records[i - 1].time_s);

    // Apply differential encoding for latest_price_tick (DIFF_FIELDS[3] = true)
    records[i].latest_price_tick = static_cast<int16_t>(records[i].latest_price_tick - records[i - 1].latest_price_tick);

    // Apply differential encoding for bid_price_ticks (DIFF_FIELDS[7] = true)
    for (int j = 0; j < 5; ++j) {
      records[i].bid_price_ticks[j] = static_cast<int16_t>(records[i].bid_price_ticks[j] - records[i - 1].bid_price_ticks[j]);
    }

    // Apply differential encoding for ask_price_ticks (DIFF_FIELDS[9] = true)
    for (int j = 0; j < 5; ++j) {
      records[i].ask_price_ticks[j] = static_cast<int16_t>(records[i].ask_price_ticks[j] - records[i - 1].ask_price_ticks[j]);
    }

    // Note: Other fields (sync, trade_count, turnover, volume, bid_volumes, ask_volumes, direction)
    // are not differentially encoded according to DIFF_FIELDS configuration
  }
}

std::vector<uint8_t> Encoder::CompressData(const std::vector<uint8_t> &raw_data) {
  if (raw_data.empty()) {
    return {};
  }

  // Estimate compressed size (worst case: slightly larger than input)
  mz_ulong compressed_size = mz_compressBound(static_cast<mz_ulong>(raw_data.size()));
  std::vector<uint8_t> compressed_data(compressed_size);

  // Compress with level 6 (optimal for tick data according to Python implementation)
  int result = mz_compress2(compressed_data.data(), &compressed_size,
                            raw_data.data(), static_cast<mz_ulong>(raw_data.size()),
                            6); // compression level 6

  if (result != MZ_OK) {
    std::cerr << "Compression failed with error code: " << result << "\n";
    return {};
  }

  // Resize to actual compressed size
  compressed_data.resize(compressed_size);
  return compressed_data;
}

} // namespace BinaryEncoder
