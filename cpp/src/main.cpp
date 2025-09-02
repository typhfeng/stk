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
  // Set thread affinity to specific core once per thread (if supported)
  static thread_local bool affinity_set = false;
  if (!affinity_set && misc::Affinity::supported()) {
    affinity_set = misc::Affinity::pin_to_core(core_id);
  }
  
  // Get month range for this asset - compute once
  const auto month_range = JsonConfig::GetMonthRange(stock_info.start_date, stock_info.end_date);
  
  // Pre-allocate decoder with estimated capacity to avoid reallocations
  L2::BinaryDecoder_L2 decoder(50000, 250000); // Estimated snapshots and orders per asset
  
  // Pre-allocate reusable containers to avoid repeated allocations
  std::vector<L2::Snapshot> snapshots;
  std::vector<L2::Order> orders;
  snapshots.reserve(10000);  // Reserve space for typical daily snapshots
  orders.reserve(50000);     // Reserve space for typical daily orders
  
  // Pre-allocate string buffers for path construction
  std::string month_path, asset_path, snapshots_file, orders_file;
  month_path.reserve(l2_binary_dir.size() + 16);    // Reserve for base path + "/YYYY/MM"
  asset_path.reserve(l2_binary_dir.size() + 32);    // Reserve for full asset path
  snapshots_file.reserve(256);                      // Reserve for full file path
  orders_file.reserve(256);                         // Reserve for full file path
  
  // Cache commonly used string literals to avoid repeated allocations
  static constexpr std::string_view snapshots_prefix = "snapshots_";
  static constexpr std::string_view orders_prefix = "orders_";
  static constexpr std::string_view bin_suffix = ".bin";
  static constexpr char path_sep = '/';
  
  // Process each month
  for (const auto &ym : month_range) {
    const std::string year_month = JsonConfig::FormatYearMonth(ym);
    const std::string_view year = std::string_view(year_month).substr(0, 4);
    const std::string_view month = std::string_view(year_month).substr(5, 2);
    
    // Construct month path efficiently using single allocation
    month_path.clear();
    month_path.reserve(l2_binary_dir.size() + 1 + year.size() + 1 + month.size());
    month_path.append(l2_binary_dir).push_back(path_sep);
    month_path.append(year).push_back(path_sep);
    month_path.append(month);
    
    // Check if month directory exists once
    const std::filesystem::path month_fs_path(month_path);
    if (!std::filesystem::exists(month_fs_path)) {
      continue; // Skip silently for performance
    }
    
    // Pre-compute asset path prefix for efficiency
    const size_t month_path_size = month_path.size();
    month_path.push_back(path_sep);
    const size_t day_prefix_size = month_path.size();
    
    // Use error_code version to avoid exceptions in directory iteration
    std::error_code ec;
    for (const auto &day_entry : std::filesystem::directory_iterator(month_fs_path, ec)) {
      if (ec || !day_entry.is_directory(ec)) continue;
      
      const auto day_filename = day_entry.path().filename().string();
      
      // Construct asset path efficiently by reusing month_path buffer
      month_path.resize(day_prefix_size);
      asset_path.clear();
      asset_path.reserve(month_path.size() + day_filename.size() + 1 + asset_code.size());
      asset_path.append(month_path).append(day_filename);
      asset_path.push_back(path_sep);
      asset_path.append(asset_code);
      
      // Check if asset directory exists for this day
      const std::filesystem::path asset_fs_path(asset_path);
      if (!std::filesystem::exists(asset_fs_path)) {
        continue; // Skip if asset wasn't traded this day
      }
      
      // Reset file paths and flags for early exit optimization
      snapshots_file.clear();
      orders_file.clear();
      bool found_snapshots = false, found_orders = false;
      
      // Find the snapshot and order files in the asset directory
      for (const auto &file_entry : std::filesystem::directory_iterator(asset_fs_path, ec)) {
        if (ec || !file_entry.is_regular_file(ec)) continue;
        
        const auto filename = file_entry.path().filename().string();
        const std::string_view filename_view(filename);
        
        if (!found_snapshots && filename_view.starts_with(snapshots_prefix) && filename_view.ends_with(bin_suffix)) {
          snapshots_file = file_entry.path().string();
          found_snapshots = true;
          if (found_orders) break; // Both files found, exit early
        } else if (!found_orders && filename_view.starts_with(orders_prefix) && filename_view.ends_with(bin_suffix)) {
          orders_file = file_entry.path().string();
          found_orders = true;
          if (found_snapshots) break; // Both files found, exit early
        }
      }
      
      // Process the files if they exist - decode directly without intermediate checks
      if (found_snapshots) {
        snapshots.clear(); // Reuse vector, just clear contents
        decoder.decode_snapshots(snapshots_file, snapshots, L2::ENABLE_DELTA_ENCODING);
        // decoder.print_all_snapshots(snapshots);
        // exit(1);
      }
      
      if (found_orders) {
        orders.clear(); // Reuse vector, just clear contents  
        decoder.decode_orders(orders_file, orders, L2::ENABLE_DELTA_ENCODING);
        decoder.print_all_orders(orders);
        exit(1);
      }
    }
    
    // Restore month_path to original size for next iteration
    month_path.resize(month_path_size);
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