#include "codec/binary_decoder_L2.hpp"
#include "codec/json_config.hpp"
#include "misc/affinity.hpp"
#include <algorithm>
#include <filesystem>
#include <future>
#include <iostream>
#include <string>
#include <vector>

void ProcessAsset(const std::string &asset_code,
                  const JsonConfig::StockInfo &stock_info,
                  const std::string &l2_binary_dir,
                  const std::string &output_dir,
                  unsigned int core_id) {
  // Set thread affinity to specific core (if supported on this platform)
  if (misc::Affinity::supported()) {
    if (!misc::Affinity::pin_to_core(core_id)) {
      std::cerr << "Warning: Failed to set thread affinity for asset " << asset_code << " to core " << core_id << "\n";
    }
  }
  try {
    // Get month range for this asset
    auto month_range = JsonConfig::GetMonthRange(stock_info.start_date, stock_info.end_date);

    // Create an L2 decoder instance for this asset
    L2::BinaryDecoder_L2 decoder;

    // Process each month
    for (const auto &ym : month_range) {
      std::string year_month = JsonConfig::FormatYearMonth(ym);
      std::string year = year_month.substr(0, 4);
      std::string month = year_month.substr(5, 2);
      
      std::string month_path = l2_binary_dir + "/" + year + "/" + month;
      
      // Check if month directory exists
      if (!std::filesystem::exists(month_path)) {
        std::cout << "  No directory found for " << asset_code << " in " << year_month << "\n";
        continue;
      }
      
      // Iterate through all days in this month
      for (const auto &day_entry : std::filesystem::directory_iterator(month_path)) {
        if (!day_entry.is_directory()) continue;
        
        std::string day = day_entry.path().filename().string();
        std::string asset_path = month_path + "/" + day + "/" + asset_code;
        
        // Check if asset directory exists for this day
        if (!std::filesystem::exists(asset_path)) {
          continue; // Skip if asset wasn't traded this day
        }
        
        // Process snapshots and orders for this day
        std::string snapshots_file, orders_file;
        
        // Find the snapshot and order files in the asset directory
        for (const auto &file_entry : std::filesystem::directory_iterator(asset_path)) {
          if (!file_entry.is_regular_file()) continue;
          
          std::string filename = file_entry.path().filename().string();
          if (filename.find("snapshots_") == 0 && filename.ends_with(".bin")) {
            snapshots_file = file_entry.path().string();
          } else if (filename.find("orders_") == 0 && filename.ends_with(".bin")) {
            orders_file = file_entry.path().string();
          }
        }
        
        // Process the files if they exist
        if (!snapshots_file.empty()) {
          std::vector<L2::Snapshot> snapshots;
          if (decoder.decode_snapshots(snapshots_file, snapshots)) {
            std::cout << "  Processed " << snapshots.size() << " snapshots for " << asset_code 
                      << " on " << year << "-" << month << "-" << day << "\n";
          }
        }
        
        if (!orders_file.empty()) {
          std::vector<L2::Order> orders;
          if (decoder.decode_orders(orders_file, orders)) {
            std::cout << "  Processed " << orders.size() << " orders for " << asset_code 
                      << " on " << year << "-" << month << "-" << day << "\n";
          }
        }
      }
    }

  } catch (const std::exception &e) {
    std::cerr << "Error processing asset " << asset_code << ": " << e.what() << "\n";
  }
}

int main() {
  try {
    // Configuration file paths (relative to project root)
    std::string config_file = "../../../config/config.json";
    std::string stock_info_file = "../../../config/daily_holding/stock_info_test.json";
    std::string input_dir = "../../../output/L2_binary";
    std::string output_dir = "../../../output";

    std::cout << "=== L2 Asset Parser =================================================" << "\n";
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
    std::cout << "  L2 Binary directory: " << input_dir << "\n";
    std::cout << "  Data available through: " << JsonConfig::FormatYearMonth(app_config.end_month) << "\n";
    std::cout << "  Total assets found: " << stock_info_map.size() << "\n";
    std::cout << "  Output directory: " << output_dir << "\n\n";

    // Create output directory if it doesn't exist
    std::filesystem::create_directories(output_dir);

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

      std::cout << "Queuing asset: " << asset_code << " (" << stock_info.name << ") (" << stock_info.start_date << " - " << stock_info.end_date << ")\n";

      // Calculate core ID for this thread (round-robin assignment)
      unsigned int core_id = futures.size() % num_threads;

      futures.push_back(
          std::async(
              std::launch::async,
              ProcessAsset,
              asset_code,
              stock_info,
              input_dir,
              output_dir,
              core_id));

      ++stock_iter;
    }

    // Wait for all remaining tasks to complete
    for (auto &future : futures) {
      future.wait();
    }

    std::cout << "\n=== L2 Processing completed successfully! ===" << "\n";
    std::cout << "All L2 assets (snapshots and orders) have been processed from: " << input_dir << "\n";

    return 0;

  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << "\n";
    std::cerr
        << "Make sure all configuration files exist and contain valid data.\n";
    return 1;
  }
}