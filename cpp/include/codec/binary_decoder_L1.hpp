#pragma once

#include "codec/L1_DataType.hpp"
#include <cstdint>
#include <string>
#include <vector>

namespace BinaryDecoder_L1 {

// ============================================================================
// MAIN DECODER CLASS
// ============================================================================

class Decoder {
public:
  // Constructor and destructor
  Decoder();
  ~Decoder();

  // Main public interface
  void ParseAsset(const std::string &asset_code,
                  const std::string &snapshot_dir,
                  const std::vector<std::string> &month_folders,
                  const std::string &output_dir);

private:
  // ========================================================================
  // CORE PARSING FUNCTIONS
  // ========================================================================

  // File I/O
  std::vector<uint8_t> ReadRawFile(const std::string &filepath, size_t /* record_count */);
  std::vector<L1::Snapshot> ParseBinaryData(const std::vector<uint8_t> &binary_data);
  void ReverseDifferentialEncoding(std::vector<L1::Snapshot> &records);

  // ========================================================================
  // DATA CONVERSION FUNCTIONS
  // ========================================================================

  void ProcessSnapshots(const std::vector<L1::Snapshot> &binary_records, uint16_t year, uint8_t month);

  // ========================================================================
  // FILE SYSTEM UTILITIES
  // ========================================================================

  std::string FindAssetFile(const std::string &month_folder,
                            const std::string &asset_code);
  size_t ExtractRecordCountFromFilename(const std::string &filename);
  size_t CalculateTotalRecordsForAsset(const std::string &asset_code,
                                       const std::string &snapshot_dir,
                                       const std::vector<std::string> &month_folders);

  // Helper function to extract record count and year/month from filename and folder
  std::tuple<size_t, uint16_t, uint8_t> ExtractRecordCountAndDateFromPath(const std::string &filepath);

  // ========================================================================
  // FORMATTING UTILITIES
  // ========================================================================

  // Use L1::TickToPrice from L1_DataType.hpp

  // ========================================================================
  // MEMBER VARIABLES
  // ========================================================================

  // Buffer configuration
  static constexpr size_t BUFFER_SIZE = 1024 * 1024; // 1MB buffer

  // I/O buffers
  std::vector<uint8_t> read_buffer_;
  std::vector<char> write_buffer_;

  // Progress tracking
  size_t total_records_ = 0;
  size_t records_count_ = 0;

};

} // namespace BinaryDecoder_L1