#include "codec/binary_decoder_L2.hpp"
#include "codec/binary_encoder_L2.hpp"
#include "codec/json_config.hpp"
#include "misc/affinity.hpp"
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

// this mode is for debugging without preparing the whole L2 binary database
// instead of binary, we parse data directly from csv, encode them, then decode them (so we can follow standard flow without rewriting)
// 1. get the target assets from config files, and the backtest period
// 2. prepare a thread pool, each responsible for 1 single asset each time, with affinity tied to no_asset%no_thread
// 3. the .7z csv files path are like these (.7z supports selective extraction): /mnt/dev/sde/A_stock/L2/2017/01/20170103.7z -> 20170103/603993.SH/(逐笔成交.csv,逐笔委托.csv,行情.csv)
// 4. we extract the csv files to /tmp/2017/01/03/603993.SH/(逐笔成交.csv,逐笔委托.csv,行情.csv)
// 5. after the extraction is done, we can reuse the path iterator(ordered by date) to go through 1-by-1 on all csv files, use L2:encoder to generate snapshots and orders binary in the very same folder
// 6. after all binaries are generated, use L2:decoder to decode them, then process them doing backtesting

// Utility functions for CSV debugging workflow
namespace CSVDebug {

// Extract specific asset CSV files from 7z archive to temporary directory
bool extract_asset_csv_from_7z(const std::string &archive_path,
                               const std::string &asset_code,
                               const std::string &temp_dir) {
  if (!std::filesystem::exists(archive_path)) {
    return false;
  }

  std::filesystem::create_directories(temp_dir);

  // Extract specific asset directory from archive
  // 7z archive structure: YYYYMMDD/ASSET_CODE/(逐笔成交.csv,逐笔委托.csv,行情.csv)
  std::string archive_name = std::filesystem::path(archive_path).stem().string();
  std::string asset_path_in_archive = archive_name + "/" + asset_code + "/*";

  std::string command = "7z x \"" + archive_path + "\" \"" + asset_path_in_archive +
                        "\" -o\"" + temp_dir + "\" -y > /dev/null 2>&1";

  int result = std::system(command.c_str());
  return result == 0;
}

// Generate 7z archive path from date and base directory
std::string generate_archive_path(const std::string &base_dir, const std::string &date_str) {
  // Convert YYYYMMDD to YYYY/MM/YYYYMMDD.7z
  std::string year = date_str.substr(0, 4);
  std::string month = date_str.substr(4, 2);
  return base_dir + "/" + year + "/" + month + "/" + date_str + ".7z";
}

// Process CSV files for a single asset on a single day
bool process_asset_day_csv(const std::string &asset_code,
                           const std::string &date_str,
                           const std::string &temp_base_dir,
                           const std::string &l2_archive_base,
                           L2::BinaryEncoder_L2 &encoder,
                           L2::BinaryDecoder_L2 &decoder) {
  // Generate paths
  std::string archive_path = generate_archive_path(l2_archive_base, date_str);
  std::string temp_asset_dir = temp_base_dir + "/" + date_str.substr(0, 4) + "/" +
                               date_str.substr(4, 2) + "/" + date_str.substr(6, 2) + "/" + asset_code;

  // Extract CSV files from archive
  if (!extract_asset_csv_from_7z(archive_path, asset_code, temp_base_dir)) {
    return false; // Archive doesn't exist or extraction failed
  }

  // Check if asset directory was created after extraction
  std::string extracted_asset_dir = temp_base_dir + "/" + date_str + "/" + asset_code;
  if (!std::filesystem::exists(extracted_asset_dir)) {
    return false; // Asset not traded on this day
  }

  // Move extracted files to our standard structure
  std::filesystem::create_directories(temp_asset_dir);
  if (std::filesystem::exists(extracted_asset_dir)) {
    for (const auto &entry : std::filesystem::directory_iterator(extracted_asset_dir)) {
      std::string dest = temp_asset_dir + "/" + entry.path().filename().string();
      std::filesystem::rename(entry.path(), dest);
    }
    std::filesystem::remove_all(extracted_asset_dir);
    std::filesystem::remove_all(temp_base_dir + "/" + date_str);
  }

  // Parse CSV files and encode to binary
  std::vector<L2::Snapshot> snapshots;
  std::vector<L2::Order> orders;

  // Look for CSV files in the asset directory
  std::string market_csv = temp_asset_dir + "/行情.csv";
  std::string order_csv = temp_asset_dir + "/逐笔委托.csv";
  std::string trade_csv = temp_asset_dir + "/逐笔成交.csv";

  bool has_data = false;

  // Parse market data (snapshots)
  if (std::filesystem::exists(market_csv)) {
    std::vector<L2::CSVSnapshot> csv_snapshots;
    if (encoder.parse_snapshot_csv(market_csv, csv_snapshots)) {
      for (const auto &csv_snap : csv_snapshots) {
        snapshots.push_back(L2::BinaryEncoder_L2::csv_to_snapshot(csv_snap));
      }
      has_data = true;
    }
  }

  // Parse order data
  if (std::filesystem::exists(order_csv)) {
    std::vector<L2::CSVOrder> csv_orders;
    if (encoder.parse_order_csv(order_csv, csv_orders)) {
      for (const auto &csv_order : csv_orders) {
        orders.push_back(L2::BinaryEncoder_L2::csv_to_order(csv_order));
      }
      has_data = true;
    }
  }

  // Parse trade data
  if (std::filesystem::exists(trade_csv)) {
    std::vector<L2::CSVTrade> csv_trades;
    if (encoder.parse_trade_csv(trade_csv, csv_trades)) {
      for (const auto &csv_trade : csv_trades) {
        orders.push_back(L2::BinaryEncoder_L2::csv_to_trade(csv_trade));
      }
      has_data = true;
    }
  }

  if (!has_data) {
    return false;
  }

  // Encode to binary files
  bool encode_success = true;
  if (!snapshots.empty()) {
    std::string snapshots_file = temp_asset_dir + "/snapshots_" + std::to_string(snapshots.size()) + ".bin";
    encode_success &= encoder.encode_snapshots(snapshots, snapshots_file);
  }

  if (!orders.empty()) {
    std::string orders_file = temp_asset_dir + "/orders_" + std::to_string(orders.size()) + ".bin";
    encode_success &= encoder.encode_orders(orders, orders_file);
  }

  if (!encode_success) {
    return false;
  }

  // Now decode and process (for debugging/backtesting)
  if (!snapshots.empty()) {
    std::vector<L2::Snapshot> decoded_snapshots;
    std::string snapshots_file = temp_asset_dir + "/snapshots_" + std::to_string(snapshots.size()) + ".bin";
    if (decoder.decode_snapshots(snapshots_file, decoded_snapshots)) {
      // decoder.print_all_snapshots(decoded_snapshots);
    }
  }

  if (!orders.empty()) {
    std::vector<L2::Order> decoded_orders;
    std::string orders_file = temp_asset_dir + "/orders_" + std::to_string(orders.size()) + ".bin";
    if (decoder.decode_orders(orders_file, decoded_orders)) {
      // decoder.print_all_orders(decoded_orders);
      return true; // Exit after first successful processing for debugging
    }
  }

  return true;
}

// Generate date strings for iteration
std::vector<std::string> generate_date_range(const std::string &start_month, const std::string &end_month) {
  std::vector<std::string> dates;

  // Parse months (YYYY_MM format)
  std::regex month_regex(R"((\d{4})_(\d{2}))");
  std::smatch start_match, end_match;

  if (!std::regex_match(start_month, start_match, month_regex) ||
      !std::regex_match(end_month, end_match, month_regex)) {
    return dates;
  }

  int start_year = std::stoi(start_match[1]);
  int start_mon = std::stoi(start_match[2]);
  int end_year = std::stoi(end_match[1]);
  int end_mon = std::stoi(end_match[2]);

  // Generate all trading days in range (simplified - add only weekdays)
  for (int year = start_year; year <= end_year; ++year) {
    int mon_start = (year == start_year) ? start_mon : 1;
    int mon_end = (year == end_year) ? end_mon : 12;

    for (int month = mon_start; month <= mon_end; ++month) {
      int days_in_month;
      if (month == 2) {
        days_in_month = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 29 : 28;
      } else if (month == 4 || month == 6 || month == 9 || month == 11) {
        days_in_month = 30;
      } else {
        days_in_month = 31;
      }

      for (int day = 1; day <= days_in_month; ++day) {
        // Simple weekday filter (exclude weekends)
        struct tm timeinfo = {};
        timeinfo.tm_year = year - 1900;
        timeinfo.tm_mon = month - 1;
        timeinfo.tm_mday = day;
        mktime(&timeinfo);

        if (timeinfo.tm_wday != 0 && timeinfo.tm_wday != 6) { // Not Sunday or Saturday
          char date_buf[9];
          snprintf(date_buf, sizeof(date_buf), "%04d%02d%02d", year, month, day);
          dates.push_back(std::string(date_buf));
        }
      }
    }
  }

  return dates;
}

// Clean up temporary files for an asset
void cleanup_asset_temp_files(const std::string &temp_asset_dir) {
  if (std::filesystem::exists(temp_asset_dir)) {
    std::filesystem::remove_all(temp_asset_dir);
  }
}

} // namespace CSVDebug

void ProcessAsset(const std::string &asset_code,
                  const JsonConfig::StockInfo &stock_info,
                  const std::string &l2_archive_base,
                  const std::string &temp_base,
                  unsigned int core_id) {
  // Set thread affinity to specific core once per thread (if supported)
  static thread_local bool affinity_set = false;
  if (!affinity_set && misc::Affinity::supported()) {
    affinity_set = misc::Affinity::pin_to_core(core_id);
  }

  std::cout << "Processing asset: " << asset_code << " (" << stock_info.name << ")\n";

  // Create encoder and decoder instances
  L2::BinaryEncoder_L2 encoder(5000, 1000000); // Estimated capacity for daily data
  L2::BinaryDecoder_L2 decoder(5000, 1000000);

  // Generate date range for this asset
  std::string start_formatted = JsonConfig::FormatYearMonth(stock_info.start_date);
  std::string end_formatted = JsonConfig::FormatYearMonth(stock_info.end_date);
  std::cout << "  Debug: start_date = " << start_formatted << ", end_date = " << end_formatted << "\n";
  
  std::vector<std::string> dates = CSVDebug::generate_date_range(start_formatted, end_formatted);

  std::cout << "  Date range: " << dates.size() << " trading days\n";

  // Create temporary directory for this asset
  std::string asset_temp_base = temp_base + "/" + asset_code + "_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
  std::filesystem::create_directories(asset_temp_base);

  // Process each date
  int processed_days = 0;
  for (const std::string &date_str : dates) {
    if (CSVDebug::process_asset_day_csv(asset_code, date_str, asset_temp_base,
                                        l2_archive_base, encoder, decoder)) {
      processed_days++;
      std::cout << "  Processed: " << date_str << " (day " << processed_days << ")\n";

      // Clean up day-specific temp files to save disk space
      std::string day_temp_dir = asset_temp_base + "/" + date_str.substr(0, 4) + "/" +
                                 date_str.substr(4, 2) + "/" + date_str.substr(6, 2) + "/" + asset_code;
      // CSVDebug::cleanup_asset_temp_files(day_temp_dir);

      // Exit after first successful day for debugging purposes
      break;
    }
  }

  // Clean up asset temporary directory
  // CSVDebug::cleanup_asset_temp_files(asset_temp_base);

  std::cout << "  Completed: " << asset_code << " (" << processed_days << " days processed)\n";
}

int main() {
  try {
    // Configuration file paths (relative to project root)
    std::string config_file = "../../../config/config.json";
    std::string stock_info_file = "../../../config/daily_holding/stock_info_test.json";
    std::string l2_archive_base = "/mnt/dev/sde/A_stock/L2"; // Base directory for .7z archives
    std::string temp_base = "../../../output/tmp";            // Temporary directory for extracted CSV files

    std::cout << "=== CSV Debugging Mode - L2 Data Processor ===================" << "\n";
    std::cout << "Loading configuration..." << "\n";

    // Parse configuration files
    JsonConfig::AppConfig app_config = JsonConfig::ParseAppConfig(config_file);
    auto stock_info_map = JsonConfig::ParseStockInfo(stock_info_file);

    // Override IPO date if earlier than start_month, and delist_date for active stocks using configured end_month
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

    std::cout << "Configuration loaded successfully:" << "\n";
    std::cout << "  L2 Archive base: " << l2_archive_base << "\n";
    std::cout << "  Temporary directory: " << temp_base << "\n";
    std::cout << "  Data period: " << JsonConfig::FormatYearMonth(app_config.start_month)
              << " to " << JsonConfig::FormatYearMonth(app_config.end_month) << "\n";
    std::cout << "  Total assets found: " << stock_info_map.size() << "\n\n";

    // Clean up and create temporary directory
    if (std::filesystem::exists(temp_base)) {
      std::filesystem::remove_all(temp_base);
    }
    std::filesystem::create_directories(temp_base);

    // Determine optimal thread count
    unsigned int num_threads = misc::Affinity::core_count();

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
      unsigned int core_id = futures.size() % num_threads;

      futures.push_back(
          std::async(
              std::launch::async,
              ProcessAsset,
              asset_code,
              stock_info,
              l2_archive_base,
              temp_base,
              core_id));

      ++stock_iter;
    }

    // Wait for all remaining tasks to complete
    for (auto &future : futures) {
      future.wait();
    }

    // Final cleanup
    if (std::filesystem::exists(temp_base)) {
      std::filesystem::remove_all(temp_base);
    }

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