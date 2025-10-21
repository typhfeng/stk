#include "AnalysisHighFrequency.hpp"
#include "codec/L2_DataType.hpp"
#include "codec/binary_decoder_L2.hpp"
#include "codec/binary_encoder_L2.hpp"
#include "codec/json_config.hpp"
#include "misc/affinity.hpp"
#include "misc/misc.hpp"
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <future>
#include <iostream>
#include <regex>
#include <string>
#include <vector>

// ============================================================================
// CSV MODE - L2 Data Processing Architecture
// ============================================================================
// PROCESSING FLOW (Per Asset, Per Day):
//
//   1. Configuration Loading
//      - Read config.json: backtest period (start_month → end_month)
//      - Read stock_info.json: asset list with IPO/delist dates
//      - Adjust date ranges based on configuration
//
//   2. Thread Pool Initialization
//      - Create worker threads (1 per CPU core)
//      - Assign CPU affinity for cache optimization
//      - Queue assets for parallel processing
//
//   3. Archive Extraction (per trading day)
//      Input:  /mnt/dev/sde/A_stock/L2/YYYY/YYYYMM/YYYYMMDD.rar
//      Extract: YYYYMMDD/ASSET_CODE/*.csv
//      Output: output/tmp/YYYY/MM/DD/ASSET_CODE/(行情.csv, 逐笔成交.csv, 逐笔委托.csv)
//
//      Optimization: Skip if binary files already exist
//
//   4. CSV Parsing (BinaryEncoder_L2)
//      - Parse 行情.csv → CSVSnapshot structures
//      - Parse 逐笔委托.csv → CSVOrder structures
//      - Parse 逐笔成交.csv → CSVTrade structures
//
//      Data Format Handling:
//      - SZSE codes: Exchange code varies by year (2025+: "1", earlier: "000001")
//      - Order types: SZSE 2025+: 0/1/U (normal/special/cancel), SSE: A/D (add/delete)
//      - NaN values: Handle space (\x20) and null (\x00) characters
//      - Price units: 0.01 RMB (CSV in 0.0001 RMB, divide by 100)
//      - Volume units: 100 shares (CSV in shares, divide by 100)
//
//   5. Binary Encoding (BinaryEncoder_L2)
//      - Convert CSV structures → L2::Snapshot and L2::Order
//      - Merge orders and trades into unified timeline
//      - Sort by timestamp with priority: maker → taker → cancel
//      - Apply optional delta encoding (ENABLE_DELTA_ENCODING)
//      - Compress using Zstandard (level 6, ~3x ratio)
//      - Write to: ASSET_CODE_snapshots_COUNT.bin, ASSET_CODE_orders_COUNT.bin
//
//   6. Binary Decoding (BinaryDecoder_L2)
//      - Decompress binary files (Zstandard)
//      - Apply delta decoding if enabled
//      - Reconstruct L2::Snapshot and L2::Order structures
//
//   7. High-Frequency Analysis (AnalysisHighFrequency)
//      - Process order stream to reconstruct order book
//      - Track maker orders, cancellations, and trades
//      - Maintain 10-level bid/ask queues
//      - Generate analysis/signals per strategy requirements
//
// DATA FLOW DIAGRAM:
//
//   RAR Archive (compressed)
//        ↓
//   CSV Files (text, GBK encoding)
//        ↓ [Parser]
//   CSV Structures (intermediate)
//        ↓ [Converter]
//   L2 Structures (normalized)
//        ↓ [Encoder + Zstd]
//   Binary Files (compressed, optional delta)
//        ↓ [Zstd + Decoder]
//   L2 Structures (in-memory)
//        ↓ [Analysis]
//   Results / Signals
//
// THREAD SAFETY:
//   - Each thread processes one asset independently
//   - No shared state between asset processors
//   - Temporary extraction directories use unique names (extract_tmp_ASSET_CODE)
//   - CPU affinity prevents thread migration and cache thrashing
//
// OPTIMIZATION STRATEGIES:
//   - Binary caching: Skip extraction if binaries exist (SKIP_EXISTING_BINARIES)
//   - Parallel processing: Use all CPU cores with affinity
//   - Memory pre-allocation: Reserve vectors based on estimated sizes
//   - Delta encoding: Reduce data size for time-series fields
//   - Zstandard compression: Fast decompression (1300+ MB/s) with good ratio
//
// ============================================================================
// CONFIGURATION SECTION
// ============================================================================
// Modify these constants to adapt to different environments or data suppliers
namespace Config {
// Archive settings - modify these for different compression formats
constexpr const char *ARCHIVE_EXTENSION = ".rar"; // Archive file extension (.rar/.7z/.zip)
constexpr const char *ARCHIVE_TOOL = "unrar";     // Archive extraction tool (unrar/7z/unzip)
constexpr const char *ARCHIVE_EXTRACT_CMD = "x";  // Extract command (x for unrar, x for 7z)

// File extensions and names - standard CSV filenames from data supplier
constexpr const char *BIN_EXTENSION = ".bin";
// CSV filenames defined here for documentation and potential future use
// Currently the encoder auto-detects these files, but explicit names reserved for future API changes
[[maybe_unused]] constexpr const char *CSV_MARKET_DATA = "行情.csv";    // Market snapshot CSV filename
[[maybe_unused]] constexpr const char *CSV_TICK_TRADE = "逐笔成交.csv"; // Tick-by-tick trade CSV filename
[[maybe_unused]] constexpr const char *CSV_TICK_ORDER = "逐笔委托.csv"; // Tick-by-tick order CSV filename

// Path settings - modify these for your environment
constexpr const char *DEFAULT_CONFIG_FILE = "../../../../config/config.json";
constexpr const char *DEFAULT_STOCK_INFO_FILE = "../../../../config/daily_holding/stock_info_test.json";
constexpr const char *DEFAULT_L2_ARCHIVE_BASE = "/mnt/dev/sde/A_stock/L2";
constexpr const char *DEFAULT_TEMP_DIR = "../../../../output/tmp";

// Processing settings - modify for different behaviors
constexpr bool CLEANUP_AFTER_PROCESSING = false; // Clean up temp files after processing (saves disk space)
constexpr bool SKIP_EXISTING_BINARIES = true;    // Skip extraction/encoding if binary files exist (faster rerun)
constexpr bool VERBOSE_LOGGING = false;          // Enable detailed logging for debugging

} // namespace Config

// ============================================================================
// LAYER 1: LOW-LEVEL UTILITY CLASSES
// ============================================================================
// These classes provide basic utilities and have no dependencies on other classes

// Path and date utility functions
class PathUtils {
public:
  // Generate archive path from date and base directory
  // Format: base_dir/YYYY/YYYYMM/YYYYMMDD.rar
  static std::string generate_archive_path(const std::string &base_dir, const std::string &date_str) {
    const std::string year = date_str.substr(0, 4);
    const std::string year_month = date_str.substr(0, 6);
    return base_dir + "/" + year + "/" + year_month + "/" + date_str + Config::ARCHIVE_EXTENSION;
  }

  // Generate temporary asset directory path
  static std::string generate_temp_asset_dir(const std::string &TEMP_DIR, const std::string &date_str, const std::string &asset_code) {
    return TEMP_DIR + "/" + date_str.substr(0, 4) + "/" + date_str.substr(4, 2) + "/" + date_str.substr(6, 2) + "/" + asset_code;
  }

  // Check if date is valid leap year calculation
  static bool is_leap_year(int year) {
    return (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
  }

  // Get days in month
  static int get_days_in_month(int year, int month) {
    if (month == 2) {
      return is_leap_year(year) ? 29 : 28;
    } else if (month == 4 || month == 6 || month == 9 || month == 11) {
      return 30;
    } else {
      return 31;
    }
  }
};

// Date range generation utility - depends on PathUtils
class DateRangeGenerator {
public:
  static std::vector<std::string> generate_date_range(const std::string &start_month, const std::string &end_month, const std::string &l2_archive_base) {
    std::vector<std::string> dates;

    // Parse months (YYYY_MM format)
    std::regex month_regex(R"((\d{4})_(\d{2}))");
    std::smatch start_match, end_match;

    if (!std::regex_match(start_month, start_match, month_regex) || !std::regex_match(end_month, end_match, month_regex)) {
      return dates;
    }

    const int start_year = std::stoi(start_match[1]);
    const int start_mon = std::stoi(start_match[2]);
    const int end_year = std::stoi(end_match[1]);
    const int end_mon = std::stoi(end_match[2]);

    // Fast path: if RAR archive base doesn't exist, assume binary files are complete
    // Generate all dates in range without filesystem checks
    const bool has_archive_base = std::filesystem::exists(l2_archive_base);

    for (int year = start_year; year <= end_year; ++year) {
      const int mon_start = (year == start_year) ? start_mon : 1;
      const int mon_end = (year == end_year) ? end_mon : 12;

      for (int month = mon_start; month <= mon_end; ++month) {
        const int days_in_month = PathUtils::get_days_in_month(year, month);

        for (int day = 1; day <= days_in_month; ++day) {
          char date_buf[9];
          snprintf(date_buf, sizeof(date_buf), "%04d%02d%02d", year, month, day);
          const std::string date_str(date_buf);

          if (has_archive_base) {
            // Check if archive exists for this date
            const std::string archive_path = PathUtils::generate_archive_path(l2_archive_base, date_str);
            if (std::filesystem::exists(archive_path)) {
              dates.push_back(date_str);
            }
          } else {
            // No RAR base, assume binary files are complete - add all dates
            dates.push_back(date_str);
          }
        }
      }
    }

    return dates;
  }
};

// ============================================================================
// LAYER 2: FILE MANAGEMENT CLASSES
// ============================================================================
// These classes handle file extraction, checking, and cleanup

// File extraction and management
class FileManager {
public:
  // Extract specific asset CSV files from archive to temporary directory
  // Archive structure: YYYYMMDD.rar -> YYYYMMDD/ASSET_CODE/(行情.csv, 逐笔成交.csv, 逐笔委托.csv)
  static bool extract_asset_csv(const std::string &archive_path, const std::string &asset_code, const std::string &temp_dir) {
    if (!std::filesystem::exists(archive_path)) {
      return false;
    }

    std::filesystem::create_directories(temp_dir);

    // Get the date string from archive filename (e.g., 20250102.rar -> 20250102)
    const std::string archive_name = std::filesystem::path(archive_path).stem().string();
    const std::string asset_path_in_archive = archive_name + "/" + asset_code + "/*";

    // unrar command: unrar x archive.rar "path/to/extract" -o"output_dir" -y
    const std::string command = std::string(Config::ARCHIVE_TOOL) + " " +
                                std::string(Config::ARCHIVE_EXTRACT_CMD) + " \"" +
                                archive_path + "\" \"" +
                                asset_path_in_archive + "\" \"" +
                                temp_dir + "/\" -y > /dev/null 2>&1";

    return std::system(command.c_str()) == 0;
  }

  // Check if binary files already exist for optimization
  static std::pair<bool, std::pair<std::string, std::string>> check_existing_binaries(const std::string &temp_asset_dir, const std::string &asset_code) {
    std::string snapshots_file, orders_file;
    bool has_binaries = false;

    if (!std::filesystem::exists(temp_asset_dir)) {
      return {false, {snapshots_file, orders_file}};
    }

    for (const auto &entry : std::filesystem::directory_iterator(temp_asset_dir)) {
      const std::string filename = entry.path().filename().string();
      // Naming convention: {asset_code}_snapshots_{count}.bin and {asset_code}_orders_{count}.bin
      if (filename.starts_with(asset_code + "_snapshots_") && filename.ends_with(Config::BIN_EXTENSION)) {
        snapshots_file = entry.path().string();
        has_binaries = true;
      } else if (filename.starts_with(asset_code + "_orders_") && filename.ends_with(Config::BIN_EXTENSION)) {
        orders_file = entry.path().string();
        has_binaries = true;
      }
    }

    return {has_binaries, {snapshots_file, orders_file}};
  }

  // Clean up temporary files
  static void cleanup_temp_files(const std::string &temp_dir) {
    if (std::filesystem::exists(temp_dir)) {
      std::filesystem::remove_all(temp_dir);
    }
  }
};

// ============================================================================
// LAYER 3: DATA PROCESSING CLASSES
// ============================================================================
// These classes handle CSV parsing and binary encoding/decoding

// CSV data processing - uses encoder's process_stock_data function
class CSVProcessor {
public:
  // Process CSV files using the encoder's standardized process_stock_data function
  static bool process_csv_files(const std::string &temp_asset_dir, const std::string &asset_code, L2::BinaryEncoder_L2 &encoder, std::vector<L2::Snapshot> &snapshots, std::vector<L2::Order> &orders) {
    // Use the encoder's standardized process_stock_data function for consistency
    return encoder.process_stock_data(temp_asset_dir, temp_asset_dir, asset_code, &snapshots, &orders);
  }
};

// Binary data decoding management
class BinaryManager {
public:
  // Decode binary files and feed to analysis engine
  static bool decode_binary_files(const std::string &snapshots_file, const std::string &orders_file, L2::BinaryDecoder_L2 &decoder, AnalysisHighFrequency *HFA_) {
    bool decode_success = true;
    (void)snapshots_file; // Reserved for future snapshot processing

    // // Decode snapshots (currently disabled, enable when needed)
    // if (!snapshots_file.empty()) {
    //   std::vector<L2::Snapshot> decoded_snapshots;
    //   decode_success &= decoder.decode_snapshots(snapshots_file, decoded_snapshots);
    //   // decoder.print_all_snapshots(decoded_snapshots);
    // }

    // Decode orders and process through high-frequency analysis
    if (!orders_file.empty()) {
      std::vector<L2::Order> decoded_orders;
      decode_success &= decoder.decode_orders(orders_file, decoded_orders);
      // decoder.print_all_orders(decoded_orders);

      if (HFA_) {
        for (const auto &ord : decoded_orders) {
          HFA_->process(ord);
        }
        HFA_->clear();
      }
    }
    return decode_success;
  }
};

// ============================================================================
// LAYER 4: BUSINESS LOGIC CLASSES
// ============================================================================
// These classes orchestrate the high-level processing workflow

// Main asset processing workflow - single asset, single day
class AssetProcessor {
public:
  // Process CSV files for a single asset on a single day with optimization for existing binaries
  static bool process_asset_day_csv(const std::string &asset_code, const std::string &date_str, const std::string &temp_base_dir, const std::string &l2_archive_base, L2::BinaryEncoder_L2 &encoder, L2::BinaryDecoder_L2 &decoder, AnalysisHighFrequency &HFA_) {
    // Generate paths
    const std::string archive_path = PathUtils::generate_archive_path(l2_archive_base, date_str);
    const std::string temp_asset_dir = PathUtils::generate_temp_asset_dir(temp_base_dir, date_str, asset_code);

    // Check if binary files already exist - optimization to skip extraction/encoding
    const auto [has_existing_binaries, binary_files] = FileManager::check_existing_binaries(temp_asset_dir, asset_code);
    const auto &[existing_snapshots_file, existing_orders_file] = binary_files;

    if (has_existing_binaries && Config::SKIP_EXISTING_BINARIES) {
      if (Config::VERBOSE_LOGGING) {
        std::cout << "    Found existing binaries for " << asset_code << " on " << date_str << ", skipping extraction/encoding\n";
      }
      return BinaryManager::decode_binary_files(existing_snapshots_file, existing_orders_file, decoder, &HFA_);
    }

    // Extract CSV files from archive - use unique directory per asset to avoid race conditions
    const std::string temp_extract_dir = temp_base_dir + "/extract_tmp_" + asset_code;
    if (!FileManager::extract_asset_csv(archive_path, asset_code, temp_extract_dir)) {
      return false; // Archive doesn't exist or extraction failed
    }

    // Check if asset directory was created after extraction
    const std::string extracted_asset_dir = temp_extract_dir + "/" + date_str + "/" + asset_code;
    if (!std::filesystem::exists(extracted_asset_dir)) {
      FileManager::cleanup_temp_files(temp_extract_dir);
      return false; // Asset not traded on this day
    }

    // Move entire asset directory to final location (more efficient than moving individual files)
    // Ensure parent directory exists
    std::filesystem::create_directories(std::filesystem::path(temp_asset_dir).parent_path());

    // Direct directory rename is faster than moving individual files
    std::filesystem::rename(extracted_asset_dir, temp_asset_dir);

    // Clean up the now-empty temporary extraction structure
    FileManager::cleanup_temp_files(temp_extract_dir);

    // Process CSV files using the encoder's standardized function (parses and encodes in one step)
    std::vector<L2::Snapshot> snapshots;
    std::vector<L2::Order> orders;

    if (!CSVProcessor::process_csv_files(temp_asset_dir, asset_code, encoder, snapshots, orders)) {
      return false; // No data found or processing failed
    }

    // Debug: uncomment to inspect parsed data
    // decoder.print_all_snapshots(snapshots);
    // decoder.print_all_orders(orders);
    // exit(1);

    // Determine binary file paths (using naming convention)
    const std::string snapshots_file = snapshots.empty() ? "" : temp_asset_dir + "/" + asset_code + "_snapshots_" + std::to_string(snapshots.size()) + Config::BIN_EXTENSION;
    const std::string orders_file = orders.empty() ? "" : temp_asset_dir + "/" + asset_code + "_orders_" + std::to_string(orders.size()) + Config::BIN_EXTENSION;

    // Delete CSV files immediately after successful binary generation to save disk space
    for (const auto &entry : std::filesystem::directory_iterator(temp_asset_dir)) {
      const std::string filename = entry.path().string();
      if (filename.ends_with(".csv")) {
        std::filesystem::remove(entry.path());
      }
    }

    return BinaryManager::decode_binary_files(snapshots_file, orders_file, decoder, &HFA_);
  }
};

// Application configuration management
class AppConfiguration {
public:
  struct Paths {
    std::string config_file;
    std::string stock_info_file;
    std::string l2_archive_base;
    std::string TEMP_DIR;
  };

  static Paths get_default_paths() {
    return {
        Config::DEFAULT_CONFIG_FILE,
        Config::DEFAULT_STOCK_INFO_FILE,
        Config::DEFAULT_L2_ARCHIVE_BASE,
        Config::DEFAULT_TEMP_DIR};
  }

  static void print_configuration_info(const Paths &paths, const JsonConfig::AppConfig &app_config, size_t total_assets) {
    std::cout << "Configuration loaded successfully:" << "\n";
    std::cout << "  Archive tool: " << Config::ARCHIVE_TOOL << " (extension: " << Config::ARCHIVE_EXTENSION << ")\n";
    std::cout << "  L2 Archive base: " << paths.l2_archive_base << "\n";
    std::cout << "  Temporary directory: " << paths.TEMP_DIR << "\n";
    std::cout << "  Data period: " << JsonConfig::FormatYearMonth(app_config.start_month)
              << " to " << JsonConfig::FormatYearMonth(app_config.end_month) << "\n";
    std::cout << "  Total assets found: " << total_assets << "\n";
    std::cout << "  Skip existing binaries: " << (Config::SKIP_EXISTING_BINARIES ? "Yes" : "No") << "\n";
    std::cout << "  Cleanup after processing: " << (Config::CLEANUP_AFTER_PROCESSING ? "Yes" : "No") << "\n";
    std::cout << "  Verbose logging: " << (Config::VERBOSE_LOGGING ? "Yes" : "No") << "\n\n";
  }

  static void adjust_stock_dates(std::unordered_map<std::string, JsonConfig::StockInfo> &stock_info_map, const JsonConfig::AppConfig &app_config) {
    for (auto &pair : stock_info_map) {
      // If IPO date is earlier than start_month, use start_month
      if (pair.second.start_date < app_config.start_month) {
        pair.second.start_date = app_config.start_month;
      }
      // Override delist_date for active stocks using configured end_month
      if (!pair.second.is_delisted) {
        pair.second.end_date = app_config.end_month;
      }
    }
  }
};

// Forward declaration of ProcessAsset function
void ProcessAsset(const std::string &asset_code, const JsonConfig::StockInfo &stock_info, const std::string &l2_archive_base, const std::string &TEMP_DIR, unsigned int core_id);

// Thread pool management - parallel asset processing
class ThreadPoolManager {
public:
  static void process_assets_in_parallel(const std::unordered_map<std::string, JsonConfig::StockInfo> &stock_info_map, const AppConfiguration::Paths &paths) {
    const unsigned int num_threads = misc::Affinity::core_count();

    std::cout << "Using " << num_threads << " threads for parallel processing";
    if (misc::Affinity::supported()) {
      std::cout << " (with CPU affinity)";
    } else {
      std::cout << " (CPU affinity not supported on this platform)";
    }
    std::cout << "\n\n";

    // Process assets in batches using thread pool
    std::vector<std::future<void>> futures;
    auto stock_iter = stock_info_map.begin();

    while (stock_iter != stock_info_map.end()) {
      // Wait for any completed threads if we're at capacity
      if (futures.size() >= num_threads) {
        // Find and remove completed futures
        auto is_completed = [](std::future<void> &f) {
          return f.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
        };
        futures.erase(
            std::remove_if(futures.begin(), futures.end(), is_completed),
            futures.end());

        // If still at capacity, wait for one to complete
        if (futures.size() >= num_threads && !futures.empty()) {
          futures.front().wait();
          futures.erase(futures.begin());
        }
      }

      // Launch new task
      const std::string &asset_code = stock_iter->first;
      const JsonConfig::StockInfo &stock_info = stock_iter->second;

      std::cout << "Queuing asset: " << asset_code << " (" << stock_info.name << ") "
                << "(" << JsonConfig::FormatYearMonth(stock_info.start_date) << " - "
                << JsonConfig::FormatYearMonth(stock_info.end_date) << ")\n";

      // Calculate core ID for this thread (round-robin assignment)
      const unsigned int core_id = futures.size() % num_threads;

      futures.push_back(
          std::async(
              std::launch::async,
              ProcessAsset,
              asset_code,
              stock_info,
              paths.l2_archive_base,
              paths.TEMP_DIR,
              core_id));

      ++stock_iter;
    }

    // Wait for all remaining tasks to complete
    for (auto &future : futures) {
      future.wait();
    }
  }
};

// ============================================================================
// MAIN PROCESSING FUNCTIONS
// ============================================================================

// Per-asset processing entry point (runs in separate thread with CPU affinity)
void ProcessAsset(const std::string &asset_code, const JsonConfig::StockInfo &stock_info, const std::string &l2_archive_base, const std::string &TEMP_DIR, unsigned int core_id) {
  // Set thread affinity to specific core once per thread (if supported)
  static thread_local bool affinity_set = false;
  if (!affinity_set && misc::Affinity::supported()) {
    affinity_set = misc::Affinity::pin_to_core(core_id);
  }

  std::cout << "Processing asset: " << asset_code << " (" << stock_info.name << ")\n";

  // Create encoder and decoder instances with configured capacity
  L2::BinaryEncoder_L2 encoder(L2::DEFAULT_ENCODER_SNAPSHOT_SIZE, L2::DEFAULT_ENCODER_ORDER_SIZE);
  L2::BinaryDecoder_L2 decoder(L2::DEFAULT_ENCODER_SNAPSHOT_SIZE, L2::DEFAULT_ENCODER_ORDER_SIZE);

  // Create a persistent LOB for this asset and reset after each day
  // Infer exchange type from asset code (SSE vs SZSE determines matching mechanism)
  L2::ExchangeType exchange_type = L2::infer_exchange_type(asset_code);
  AnalysisHighFrequency HFA_(L2::DEFAULT_ENCODER_ORDER_SIZE, exchange_type);

  // Generate date range for this asset
  const std::string start_formatted = JsonConfig::FormatYearMonth(stock_info.start_date);
  const std::string end_formatted = JsonConfig::FormatYearMonth(stock_info.end_date);
  std::cout << "  Debug: start_date = " << start_formatted << ", end_date = " << end_formatted << "\n";

  const std::vector<std::string> dates = DateRangeGenerator::generate_date_range(start_formatted, end_formatted, l2_archive_base);
  std::cout << "  Date range: " << dates.size() << " trading days\n";

  // Process each date
  int processed_days = 0;
  size_t current_date_index = 0;
  for (const std::string &date_str : dates) {
    misc::print_progress(current_date_index + 1, dates.size(), "Processing " + asset_code + " - " + date_str);
    if (AssetProcessor::process_asset_day_csv(asset_code, date_str, TEMP_DIR, l2_archive_base, encoder, decoder, HFA_)) {
      processed_days++;
      if (Config::VERBOSE_LOGGING) {
        std::cout << "  Processed: " << date_str << " (day " << processed_days << ")\n";
      }

      // Optionally clean up day-specific temp files to save disk space
      if (Config::CLEANUP_AFTER_PROCESSING) {
        const std::string day_temp_dir = PathUtils::generate_temp_asset_dir(TEMP_DIR, date_str, asset_code);
        FileManager::cleanup_temp_files(day_temp_dir);
      }
    }
    current_date_index++;
  }

  std::cout << "  Completed: " << asset_code << " (" << processed_days << " days processed)\n";
}

// Program entry point
int main() {
  try {
    // Get configuration paths
    const AppConfiguration::Paths paths = AppConfiguration::get_default_paths();

    std::cout << "=== CSV Debugging Mode - L2 Data Processor ===================" << "\n";
    std::cout << "Loading configuration..." << "\n";

    // Parse configuration files
    const JsonConfig::AppConfig app_config = JsonConfig::ParseAppConfig(paths.config_file);
    auto stock_info_map = JsonConfig::ParseStockInfo(paths.stock_info_file);

    // Adjust stock dates based on configuration
    AppConfiguration::adjust_stock_dates(stock_info_map, app_config);

    // Display configuration information
    AppConfiguration::print_configuration_info(paths, app_config, stock_info_map.size());

    // Prepare temporary directory
    std::filesystem::create_directories(paths.TEMP_DIR);

    // Process assets using thread pool
    ThreadPoolManager::process_assets_in_parallel(stock_info_map, paths);

    // Optional: Final cleanup of all temp files
    if (Config::CLEANUP_AFTER_PROCESSING) {
      if (std::filesystem::exists(paths.TEMP_DIR)) {
        std::filesystem::remove_all(paths.TEMP_DIR);
      }
    }

    std::cout << "\n=== CSV Debugging Processing completed successfully! ===" << "\n";
    std::cout << "All assets have been processed using CSV → Binary → Decode workflow\n";

    return 0;

  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << "\n";
    std::cerr << "Make sure all configuration files exist and contain valid data.\n";
    std::cerr << "Also ensure '" << Config::ARCHIVE_TOOL << "' command is available and archive files exist at the specified path.\n";
    std::cerr << "Archive path format: " << Config::DEFAULT_L2_ARCHIVE_BASE << "/YYYY/YYYYMM/YYYYMMDD" << Config::ARCHIVE_EXTENSION << "\n";
    return 1;
  }
}
