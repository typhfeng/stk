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
//      - Read config.json: backtest period (start_date → end_date)
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

// Path utilities
class PathUtils {
public:
  static std::string generate_archive_path(const std::string &base_dir, const std::string &date_str) {
    const std::string year = date_str.substr(0, 4);
    const std::string year_month = date_str.substr(0, 6);
    return base_dir + "/" + year + "/" + year_month + "/" + date_str + Config::ARCHIVE_EXTENSION;
  }

  static std::string generate_temp_asset_dir(const std::string &temp_dir, const std::string &date_str, const std::string &asset_code) {
    return temp_dir + "/" + date_str.substr(0, 4) + "/" + date_str.substr(4, 2) + "/" + date_str.substr(6, 2) + "/" + asset_code;
  }

  static bool is_leap_year(int year) {
    return (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
  }

  static int get_days_in_month(int year, int month) {
    if (month == 2) {
      return is_leap_year(year) ? 29 : 28;
    }
    if (month == 4 || month == 6 || month == 9 || month == 11) {
      return 30;
    }
    return 31;
  }
};

// Date range generator
class DateRangeGenerator {
public:
  static std::vector<std::string> generate_date_range(const std::string &start_date, const std::string &end_date, const std::string &l2_archive_base) {
    std::vector<std::string> dates;

    std::regex date_regex(R"((\d{4})_(\d{2})_(\d{2}))");
    std::smatch start_match, end_match;

    if (!std::regex_match(start_date, start_match, date_regex) || !std::regex_match(end_date, end_match, date_regex)) {
      return dates;
    }

    const int start_year = std::stoi(start_match[1]);
    const int start_mon = std::stoi(start_match[2]);
    const int start_day = std::stoi(start_match[3]);
    const int end_year = std::stoi(end_match[1]);
    const int end_mon = std::stoi(end_match[2]);
    const int end_day = std::stoi(end_match[3]);

    const bool has_archive_base = std::filesystem::exists(l2_archive_base);

    for (int year = start_year; year <= end_year; ++year) {
      const int mon_start = (year == start_year) ? start_mon : 1;
      const int mon_end = (year == end_year) ? end_mon : 12;

      for (int month = mon_start; month <= mon_end; ++month) {
        const int day_start = (year == start_year && month == start_mon) ? start_day : 1;
        const int day_end = (year == end_year && month == end_mon) ? end_day : PathUtils::get_days_in_month(year, month);

        for (int day = day_start; day <= day_end; ++day) {
          char date_buf[9];
          snprintf(date_buf, sizeof(date_buf), "%04d%02d%02d", year, month, day);
          const std::string date_str(date_buf);

          if (!has_archive_base || std::filesystem::exists(PathUtils::generate_archive_path(l2_archive_base, date_str))) {
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

// Archive extraction and file management
class FileManager {
public:
  // Extract asset CSV files from archive
  static bool extract_asset_csv(const std::string &archive_path, const std::string &asset_code, const std::string &temp_dir) {
    if (!std::filesystem::exists(archive_path)) {
      return false;
    }

    std::filesystem::create_directories(temp_dir);

    const std::string archive_name = std::filesystem::path(archive_path).stem().string();
    const std::string asset_path_in_archive = archive_name + "/" + asset_code + "/*";
    const std::string command = std::string(Config::ARCHIVE_TOOL) + " " +
                                std::string(Config::ARCHIVE_EXTRACT_CMD) + " \"" +
                                archive_path + "\" \"" + asset_path_in_archive + "\" \"" +
                                temp_dir + "/\" -y > /dev/null 2>&1";

    return std::system(command.c_str()) == 0;
  }

  // Check for existing binary files
  static std::pair<bool, std::pair<std::string, std::string>> check_existing_binaries(const std::string &temp_asset_dir, const std::string &asset_code) {
    if (!std::filesystem::exists(temp_asset_dir)) {
      return {false, {"", ""}};
    }

    std::string snapshots_file, orders_file;
    for (const auto &entry : std::filesystem::directory_iterator(temp_asset_dir)) {
      const std::string filename = entry.path().filename().string();
      if (filename.starts_with(asset_code + "_snapshots_") && filename.ends_with(Config::BIN_EXTENSION)) {
        snapshots_file = entry.path().string();
      } else if (filename.starts_with(asset_code + "_orders_") && filename.ends_with(Config::BIN_EXTENSION)) {
        orders_file = entry.path().string();
      }
    }

    return {!snapshots_file.empty() || !orders_file.empty(), {snapshots_file, orders_file}};
  }

  // Remove temporary directory
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

// CSV parsing and binary encoding
class DataEncoder {
public:
  // Parse CSV files and encode to binary structures
  static bool parse_and_encode_csv(const std::string &csv_dir, const std::string &asset_code, L2::BinaryEncoder_L2 &encoder, std::vector<L2::Snapshot> &snapshots, std::vector<L2::Order> &orders) {
    return encoder.process_stock_data(csv_dir, csv_dir, asset_code, &snapshots, &orders);
  }
};

// Binary data loading and analysis
class DataAnalyzer {
public:
  // Load binary data and run high-frequency analysis
  static bool load_and_analyze(const std::string &snapshots_file, const std::string &orders_file, L2::BinaryDecoder_L2 &decoder, AnalysisHighFrequency *analyzer) {
    (void)snapshots_file; // Reserved for future snapshot processing

    // // Decode snapshots (currently disabled, enable when needed)
    // if (!snapshots_file.empty()) {
    //   std::vector<L2::Snapshot> decoded_snapshots;
    //   decode_success &= decoder.decode_snapshots(snapshots_file, decoded_snapshots);
    //   // decoder.print_all_snapshots(decoded_snapshots);
    // }
    if (!orders_file.empty()) {
      std::vector<L2::Order> decoded_orders;
      if (!decoder.decode_orders(orders_file, decoded_orders)) {
        return false;
      }

      if (analyzer) {
        for (const auto &ord : decoded_orders) {
          analyzer->process(ord);
        }
        analyzer->clear();
      }
    }
    return true;
  }
};

// ============================================================================
// LAYER 4: BUSINESS LOGIC CLASSES
// ============================================================================
// These classes orchestrate the high-level processing workflow

// Asset processing workflow
class AssetProcessor {
public:
  static bool process_single_day(const std::string &asset_code, const std::string &date_str, const std::string &temp_base_dir, const std::string &l2_archive_base, L2::BinaryEncoder_L2 &encoder, L2::BinaryDecoder_L2 &decoder, AnalysisHighFrequency &analyzer) {
    const std::string archive_path = PathUtils::generate_archive_path(l2_archive_base, date_str);
    const std::string temp_asset_dir = PathUtils::generate_temp_asset_dir(temp_base_dir, date_str, asset_code);

    const auto [has_binaries, binary_files] = FileManager::check_existing_binaries(temp_asset_dir, asset_code);
    const auto &[snapshots_file, orders_file] = binary_files;

    if (has_binaries && Config::SKIP_EXISTING_BINARIES) {
      return DataAnalyzer::load_and_analyze(snapshots_file, orders_file, decoder, &analyzer);
    }

    // Extract from archive
    const std::string temp_extract_dir = temp_base_dir + "/extract_tmp_" + asset_code;
    if (!FileManager::extract_asset_csv(archive_path, asset_code, temp_extract_dir)) {
      return false;
    }

    const std::string extracted_dir = temp_extract_dir + "/" + date_str + "/" + asset_code;
    if (!std::filesystem::exists(extracted_dir)) {
      FileManager::cleanup_temp_files(temp_extract_dir);
      return false;
    }

    std::filesystem::create_directories(std::filesystem::path(temp_asset_dir).parent_path());
    std::filesystem::rename(extracted_dir, temp_asset_dir);
    FileManager::cleanup_temp_files(temp_extract_dir);

    // Parse and encode
    std::vector<L2::Snapshot> snapshots;
    std::vector<L2::Order> orders;
    if (!DataEncoder::parse_and_encode_csv(temp_asset_dir, asset_code, encoder, snapshots, orders)) {
      return false;
    }
    // Debug: uncomment to inspect parsed data
    // decoder.print_all_snapshots(snapshots);
    // decoder.print_all_orders(orders);
    // exit(1);

    const std::string snap_file = snapshots.empty() ? "" : temp_asset_dir + "/" + asset_code + "_snapshots_" + std::to_string(snapshots.size()) + Config::BIN_EXTENSION;
    const std::string ord_file = orders.empty() ? "" : temp_asset_dir + "/" + asset_code + "_orders_" + std::to_string(orders.size()) + Config::BIN_EXTENSION;

    // Clean up CSV files
    for (const auto &entry : std::filesystem::directory_iterator(temp_asset_dir)) {
      if (entry.path().string().ends_with(".csv")) {
        std::filesystem::remove(entry.path());
      }
    }

    return DataAnalyzer::load_and_analyze(snap_file, ord_file, decoder, &analyzer);
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

  static void print_summary(const Paths &paths, const JsonConfig::AppConfig &app_config, size_t total_assets) {
    std::cout << "Configuration:" << "\n";
    std::cout << "  Archive: " << Config::ARCHIVE_TOOL << " (" << Config::ARCHIVE_EXTENSION << ")\n";
    std::cout << "  L2 base: " << paths.l2_archive_base << "\n";
    std::cout << "  Temp dir: " << paths.TEMP_DIR << "\n";
    std::cout << "  Period: " << JsonConfig::FormatYearMonthDay(app_config.start_date)
              << " → " << JsonConfig::FormatYearMonthDay(app_config.end_date) << "\n";
    std::cout << "  Assets: " << total_assets << "\n";
    std::cout << "  Skip existing: " << (Config::SKIP_EXISTING_BINARIES ? "Yes" : "No") << "\n";
    std::cout << "  Auto cleanup: " << (Config::CLEANUP_AFTER_PROCESSING ? "Yes" : "No") << "\n";
    std::cout << "  Verbose: " << (Config::VERBOSE_LOGGING ? "Yes" : "No") << "\n\n";
  }

  static void adjust_stock_dates(std::unordered_map<std::string, JsonConfig::StockInfo> &stock_info_map, const JsonConfig::AppConfig &app_config) {
    const std::chrono::year_month config_start{app_config.start_date.year(), app_config.start_date.month()};
    const std::chrono::year_month config_end{app_config.end_date.year(), app_config.end_date.month()};
    
    for (auto &[code, info] : stock_info_map) {
      if (info.start_date < config_start) {
        info.start_date = config_start;
      }
      if (!info.is_delisted) {
        info.end_date = config_end;
      }
    }
  }
};

// Forward declaration of ProcessAsset function
void ProcessAsset(const std::string &asset_code, const JsonConfig::StockInfo &stock_info, const JsonConfig::AppConfig &app_config, const std::string &l2_archive_base, const std::string &TEMP_DIR, unsigned int core_id);

// Parallel asset processing with thread pool
class ParallelProcessor {
public:
  static void run(const std::unordered_map<std::string, JsonConfig::StockInfo> &stock_info_map, const JsonConfig::AppConfig &app_config, const AppConfiguration::Paths &paths) {
    const unsigned int num_threads = misc::Affinity::core_count();

    std::cout << "Threads: " << num_threads;
    if (misc::Affinity::supported()) {
      std::cout << " (CPU affinity enabled)";
    }
    std::cout << "\n\n";

    // Process all assets using thread pool
    std::vector<std::future<void>> futures;
    auto stock_iter = stock_info_map.begin();

    while (stock_iter != stock_info_map.end()) {
      // Wait for slot if at capacity
      if (futures.size() >= num_threads) {
        auto is_done = [](std::future<void> &f) { return f.wait_for(std::chrono::seconds(0)) == std::future_status::ready; };
        futures.erase(std::remove_if(futures.begin(), futures.end(), is_done), futures.end());

        if (futures.size() >= num_threads && !futures.empty()) {
          futures.front().wait();
          futures.erase(futures.begin());
        }
      }

      // Queue next asset
      const std::string &asset_code = stock_iter->first;
      const JsonConfig::StockInfo &stock_info = stock_iter->second;

      std::cout << "Queue: " << asset_code << " (" << stock_info.name << ") "
                << JsonConfig::FormatYearMonth(stock_info.start_date) << " → "
                << JsonConfig::FormatYearMonth(stock_info.end_date) << "\n";

      const unsigned int core_id = futures.size() % num_threads;
      futures.push_back(std::async(std::launch::async, ProcessAsset, asset_code, stock_info, app_config, paths.l2_archive_base, paths.TEMP_DIR, core_id));
      ++stock_iter;
    }

    // Wait for completion
    for (auto &f : futures) {
      f.wait();
    }
  }
};

// ============================================================================
// MAIN PROCESSING FUNCTIONS
// ============================================================================

// Per-asset processing (thread entry point)
void ProcessAsset(const std::string &asset_code, const JsonConfig::StockInfo &stock_info, const JsonConfig::AppConfig &app_config, const std::string &l2_archive_base, const std::string &TEMP_DIR, unsigned int core_id) {
  static thread_local bool affinity_set = false;
  if (!affinity_set && misc::Affinity::supported()) {
    affinity_set = misc::Affinity::pin_to_core(core_id);
  }

  std::cout << "Start: " << asset_code << " (" << stock_info.name << ")\n";

  L2::BinaryEncoder_L2 encoder(L2::DEFAULT_ENCODER_SNAPSHOT_SIZE, L2::DEFAULT_ENCODER_ORDER_SIZE);
  L2::BinaryDecoder_L2 decoder(L2::DEFAULT_ENCODER_SNAPSHOT_SIZE, L2::DEFAULT_ENCODER_ORDER_SIZE);
  L2::ExchangeType exchange_type = L2::infer_exchange_type(asset_code);
  AnalysisHighFrequency HFA_(L2::DEFAULT_ENCODER_ORDER_SIZE, exchange_type);

  // Calculate effective date range
  std::chrono::year_month_day stock_start_ymd{stock_info.start_date / std::chrono::day{1}};
  std::chrono::year_month_day stock_end_ymd{stock_info.end_date / std::chrono::last};
  std::chrono::year_month_day effective_start = (stock_start_ymd > app_config.start_date) ? stock_start_ymd : app_config.start_date;
  std::chrono::year_month_day effective_end = (stock_end_ymd < app_config.end_date) ? stock_end_ymd : app_config.end_date;

  const std::string start_formatted = JsonConfig::FormatYearMonthDay(effective_start);
  const std::string end_formatted = JsonConfig::FormatYearMonthDay(effective_end);
  const std::vector<std::string> dates = DateRangeGenerator::generate_date_range(start_formatted, end_formatted, l2_archive_base);
  
  std::cout << "  Range: " << start_formatted << " → " << end_formatted << " (" << dates.size() << " days)\n";

  // Process each trading day
  int success_count = 0;
  for (size_t i = 0; i < dates.size(); ++i) {
    const std::string &date_str = dates[i];
    misc::print_progress(i + 1, dates.size(), "Processing " + asset_code + " - " + date_str);
    
    if (AssetProcessor::process_single_day(asset_code, date_str, TEMP_DIR, l2_archive_base, encoder, decoder, HFA_)) {
      success_count++;
      if (Config::CLEANUP_AFTER_PROCESSING) {
        FileManager::cleanup_temp_files(PathUtils::generate_temp_asset_dir(TEMP_DIR, date_str, asset_code));
      }
    }
  }

  std::cout << "  Done: " << asset_code << " (" << success_count << "/" << dates.size() << " days)\n";
}

// Program entry point
int main() {
  try {
    const AppConfiguration::Paths paths = AppConfiguration::get_default_paths();

    std::cout << "=== L2 Data Processor (CSV Mode) ===" << "\n";

    const JsonConfig::AppConfig app_config = JsonConfig::ParseAppConfig(paths.config_file);
    auto stock_info_map = JsonConfig::ParseStockInfo(paths.stock_info_file);
    AppConfiguration::adjust_stock_dates(stock_info_map, app_config);
    AppConfiguration::print_summary(paths, app_config, stock_info_map.size());

    std::filesystem::create_directories(paths.TEMP_DIR);
    ParallelProcessor::run(stock_info_map, app_config, paths);

    if (Config::CLEANUP_AFTER_PROCESSING) {
      if (std::filesystem::exists(paths.TEMP_DIR)) {
        std::filesystem::remove_all(paths.TEMP_DIR);
      }
    }

    std::cout << "\n=== Processing Complete ===" << "\n";
    return 0;

  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << "\n";
    std::cerr << "Check: config files, " << Config::ARCHIVE_TOOL << " availability, archive path\n";
    std::cerr << "Format: " << Config::DEFAULT_L2_ARCHIVE_BASE << "/YYYY/YYYYMM/YYYYMMDD" << Config::ARCHIVE_EXTENSION << "\n";
    return 1;
  }
}
