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

#include "lob/lob_deduct.hpp"

// this mode is for debugging without preparing the whole L2 binary database
// instead of binary, we parse data directly from csv, encode them, then decode them (so we can follow standard flow without rewriting)
// 1. get the target assets from config files, and the backtest period
// 2. prepare a thread pool, each responsible for 1 single asset each time, with affinity tied to no_asset%no_thread
// 3. the .7z csv files path are like these (.7z supports selective extraction): /mnt/dev/sde/A_stock/L2/2017/01/20170103.7z -> 20170103/603993.SH/(逐笔成交.csv,逐笔委托.csv,行情.csv)
// 4. we extract the csv files to /tmp/2017/01/03/603993.SH/(逐笔成交.csv,逐笔委托.csv,行情.csv)
// 5. after the extraction is done, we can reuse the path iterator(ordered by date) to go through 1-by-1 on all csv files, use L2:encoder to generate snapshots and orders binary in the very same folder
// 6. after all binaries are generated, use L2:decoder to decode them, then process them doing backtesting

// Configuration constants
namespace Config {

constexpr const char *BIN_EXTENSION = ".bin";
constexpr const char *SEVEN_ZIP_CMD = "7z";
} // namespace Config

// CSV debugging mode for L2 data processing without full binary database
// Workflow: CSV extraction → Binary encoding → Binary decoding → Backtesting
// Thread pool processes assets in parallel with CPU affinity optimization

// Path and date utility functions
class PathUtils {
public:
  // Generate 7z archive path from date and base directory
  static std::string generate_archive_path(const std::string &base_dir, const std::string &date_str) {
    const std::string year = date_str.substr(0, 4);
    const std::string month = date_str.substr(4, 2);
    return base_dir + "/" + year + "/" + month + "/" + date_str + ".7z";
  }

  // Generate temporary asset directory path
  static std::string generate_temp_asset_dir(const std::string &temp_base, const std::string &date_str, const std::string &asset_code) {
    return temp_base + "/" + date_str.substr(0, 4) + "/" + date_str.substr(4, 2) + "/" + date_str.substr(6, 2) + "/" + asset_code;
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

// File extraction and management
class FileManager {
public:
  // Extract specific asset CSV files from 7z archive to temporary directory
  static bool extract_asset_csv_from_7z(const std::string &archive_path, const std::string &asset_code, const std::string &temp_dir) {
    if (!std::filesystem::exists(archive_path)) {
      return false;
    }

    std::filesystem::create_directories(temp_dir);

    const std::string archive_name = std::filesystem::path(archive_path).stem().string();
    const std::string asset_path_in_archive = archive_name + "/" + asset_code + "/*";
    const std::string command = std::string(Config::SEVEN_ZIP_CMD) + " x \"" + archive_path + "\" \"" + asset_path_in_archive + "\" -o\"" + temp_dir + "\" -y > /dev/null 2>&1";

    return std::system(command.c_str()) == 0;
  }

  // Check if binary files already exist for optimization (updated for new naming convention)
  static std::pair<bool, std::pair<std::string, std::string>> check_existing_binaries(const std::string &temp_asset_dir, const std::string &asset_code) {
    std::string snapshots_file, orders_file;
    bool has_binaries = false;

    if (!std::filesystem::exists(temp_asset_dir)) {
      return {false, {snapshots_file, orders_file}};
    }

    for (const auto &entry : std::filesystem::directory_iterator(temp_asset_dir)) {
      const std::string filename = entry.path().filename().string();
      // New naming convention: {asset_code}_snapshots_{count}.bin and {asset_code}_orders_{count}.bin
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

// CSV data processing and conversion (simplified to use encoder's process_stock_data)
class CSVProcessor {
public:
  // Process CSV files using the encoder's standardized process_stock_data function
  static bool process_csv_files(const std::string &temp_asset_dir, const std::string &asset_code, L2::BinaryEncoder_L2 &encoder, std::vector<L2::Snapshot> &snapshots, std::vector<L2::Order> &orders) {
    // Use the encoder's standardized process_stock_data function for consistency
    return encoder.process_stock_data(temp_asset_dir, temp_asset_dir, asset_code, &snapshots, &orders);
  }
};

// Binary data decoding management (encoding now handled by process_stock_data)
class BinaryManager {
public:
  // Decode binary files for processing
  static bool decode_binary_files(const std::string &snapshots_file, const std::string &orders_file, L2::BinaryDecoder_L2 &decoder, lob::LOB *book) {
    bool decode_success = true;

    if (!snapshots_file.empty()) {
      std::vector<L2::Snapshot> decoded_snapshots;
      decode_success &= decoder.decode_snapshots(snapshots_file, decoded_snapshots);
      // decoder.print_all_snapshots(decoded_snapshots);
      // exit(1);
    }

    if (!orders_file.empty()) {
      std::vector<L2::Order> decoded_orders;
      decode_success &= decoder.decode_orders(orders_file, decoded_orders);
      // decoder.print_all_orders(decoded_orders);
      // exit(1);
      if (book) {
        for (const auto &ord : decoded_orders) {
          book->process(ord);
        }
        book->clear();
      }
    }
    return decode_success;
  }
};

// Date range generation utility
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

    // Check all possible dates in range and add only those with existing 7z files
    for (int year = start_year; year <= end_year; ++year) {
      const int mon_start = (year == start_year) ? start_mon : 1;
      const int mon_end = (year == end_year) ? end_mon : 12;

      for (int month = mon_start; month <= mon_end; ++month) {
        const int days_in_month = PathUtils::get_days_in_month(year, month);

        for (int day = 1; day <= days_in_month; ++day) {
          char date_buf[9];
          snprintf(date_buf, sizeof(date_buf), "%04d%02d%02d", year, month, day);
          const std::string date_str(date_buf);

          // Check if 7z archive exists for this date
          const std::string archive_path = PathUtils::generate_archive_path(l2_archive_base, date_str);
          if (std::filesystem::exists(archive_path)) {
            dates.push_back(date_str);
          }
        }
      }
    }

    return dates;
  }
};

// Main asset processing workflow
class AssetProcessor {
public:
  // Process CSV files for a single asset on a single day with optimization for existing binaries
  static bool process_asset_day_csv(const std::string &asset_code, const std::string &date_str, const std::string &temp_base_dir, const std::string &l2_archive_base, L2::BinaryEncoder_L2 &encoder, L2::BinaryDecoder_L2 &decoder, lob::LOB &book) {
    // Generate paths
    const std::string archive_path = PathUtils::generate_archive_path(l2_archive_base, date_str);
    const std::string temp_asset_dir = PathUtils::generate_temp_asset_dir(temp_base_dir, date_str, asset_code);

    // Create target directory
    std::filesystem::create_directories(temp_asset_dir);

    // Check if binary files already exist - optimization to skip extraction/encoding
    const auto [has_existing_binaries, binary_files] = FileManager::check_existing_binaries(temp_asset_dir, asset_code);
    const auto &[existing_snapshots_file, existing_orders_file] = binary_files;

    if (has_existing_binaries) {
      // std::cout << "    Found existing binaries for " << asset_code << " on " << date_str << ", skipping extraction/encoding\n";
      return BinaryManager::decode_binary_files(existing_snapshots_file, existing_orders_file, decoder, &book);
    }

    // Extract CSV files from archive - use unique directory per asset to avoid race conditions
    const std::string temp_extract_dir = temp_base_dir + "/extract_tmp_" + asset_code;
    if (!FileManager::extract_asset_csv_from_7z(archive_path, asset_code, temp_extract_dir)) {
      return false; // Archive doesn't exist or extraction failed
    }

    // Check if asset directory was created after extraction
    const std::string extracted_asset_dir = temp_extract_dir + "/" + date_str + "/" + asset_code;
    if (!std::filesystem::exists(extracted_asset_dir)) {
      FileManager::cleanup_temp_files(temp_extract_dir);
      return false; // Asset not traded on this day
    }

    // Move extracted files to final structure
    for (const auto &entry : std::filesystem::directory_iterator(extracted_asset_dir)) {
      const std::string dest = temp_asset_dir + "/" + entry.path().filename().string();
      std::filesystem::rename(entry.path(), dest);
    }

    FileManager::cleanup_temp_files(temp_extract_dir);

    // Process CSV files using the encoder's standardized function (parses and encodes in one step)
    std::vector<L2::Snapshot> snapshots;
    std::vector<L2::Order> orders;

    if (!CSVProcessor::process_csv_files(temp_asset_dir, asset_code, encoder, snapshots, orders)) {
      return false; // No data found or processing failed
    }

    // decoder.print_all_snapshots(snapshots);
    // decoder.print_all_orders(orders);
    // exit(1);

    // Determine binary file paths (using new naming convention)
    const std::string snapshots_file = snapshots.empty() ? "" : temp_asset_dir + "/" + asset_code + "_snapshots_" + std::to_string(snapshots.size()) + Config::BIN_EXTENSION;
    const std::string orders_file = orders.empty() ? "" : temp_asset_dir + "/" + asset_code + "_orders_" + std::to_string(orders.size()) + Config::BIN_EXTENSION;

    return BinaryManager::decode_binary_files(snapshots_file, orders_file, decoder, &book);
  }
};

// Configuration management class
class AppConfiguration {
public:
  struct Paths {
    std::string config_file;
    std::string stock_info_file;
    std::string l2_archive_base;
    std::string temp_base;
  };

  static Paths get_default_paths() {
    return {
        "../../../config/config.json",
        "../../../config/daily_holding/stock_info_test.json",
        "/mnt/dev/sde/A_stock/L2",
        "../../../output/tmp"};
  }

  static void print_configuration_info(const Paths &paths, const JsonConfig::AppConfig &app_config, size_t total_assets) {
    std::cout << "Configuration loaded successfully:" << "\n";
    std::cout << "  L2 Archive base: " << paths.l2_archive_base << "\n";
    std::cout << "  Temporary directory: " << paths.temp_base << "\n";
    std::cout << "  Data period: " << JsonConfig::FormatYearMonth(app_config.start_month)
              << " to " << JsonConfig::FormatYearMonth(app_config.end_month) << "\n";
    std::cout << "  Total assets found: " << total_assets << "\n\n";
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
void ProcessAsset(const std::string &asset_code, const JsonConfig::StockInfo &stock_info, const std::string &l2_archive_base, const std::string &temp_base, unsigned int core_id);

// Thread pool management class
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
              paths.temp_base,
              core_id));

      ++stock_iter;
    }

    // Wait for all remaining tasks to complete
    for (auto &future : futures) {
      future.wait();
    }
  }
};

// Main asset processing function with thread affinity
void ProcessAsset(const std::string &asset_code, const JsonConfig::StockInfo &stock_info, const std::string &l2_archive_base, const std::string &temp_base, unsigned int core_id) {
  // Set thread affinity to specific core once per thread (if supported)
  static thread_local bool affinity_set = false;
  if (!affinity_set && misc::Affinity::supported()) {
    affinity_set = misc::Affinity::pin_to_core(core_id);
  }

  std::cout << "Processing asset: " << asset_code << " (" << stock_info.name << ")\n";

  // Create encoder and decoder instances with configured capacity
  L2::BinaryEncoder_L2 encoder(L2::DEFAULT_ENCODER_CAPACITY, L2::DEFAULT_ENCODER_MAX_SIZE);
  L2::BinaryDecoder_L2 decoder(L2::DEFAULT_ENCODER_CAPACITY, L2::DEFAULT_ENCODER_MAX_SIZE);

  // Create a persistent LOB for this asset and reset after each day
  lob::LOB book;

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
    if (AssetProcessor::process_asset_day_csv(asset_code, date_str, temp_base, l2_archive_base, encoder, decoder, book)) {
      processed_days++;
      // std::cout << "  Processed: " << date_str << " (day " << processed_days << ")\n";

      // Optionally clean up day-specific temp files to save disk space
      // const std::string day_temp_dir = PathUtils::generate_temp_asset_dir(temp_base, date_str, asset_code);
      // FileManager::cleanup_temp_files(day_temp_dir);

      // Exit after first successful day for debugging purposes
      // break;
    }
    current_date_index++;
  }

  std::cout << "  Completed: " << asset_code << " (" << processed_days << " days processed)\n";
}

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

    // // Prepare temporary directory
    // if (std::filesystem::exists(paths.temp_base)) {
    //     std::filesystem::remove_all(paths.temp_base);
    // }
    std::filesystem::create_directories(paths.temp_base);

    // Process assets using thread pool
    ThreadPoolManager::process_assets_in_parallel(stock_info_map, paths);

    // Optional: Final cleanup
    // if (std::filesystem::exists(paths.temp_base)) {
    //     std::filesystem::remove_all(paths.temp_base);
    // }

    std::cout << "\n=== CSV Debugging Processing completed successfully! ===" << "\n";
    std::cout << "All assets have been processed using CSV → Binary → Decode workflow\n";

    return 0;

  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << "\n";
    std::cerr << "Make sure all configuration files exist and contain valid data.\n";
    std::cerr << "Also ensure 7z command is available and archive files exist at the specified path.\n";
    return 1;
  }
}