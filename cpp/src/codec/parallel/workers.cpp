#include "codec/parallel/workers.hpp"
#include "codec/binary_encoder_L2.hpp"
#include "codec/parallel/processing_config.hpp"
#include "misc/affinity.hpp"
#include "misc/misc.hpp"

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <unordered_map>

namespace L2 {
namespace Parallel {

// Global day-by-day statistics
static std::unordered_map<std::string, int> assets_per_date;
static std::unordered_map<std::string, int> completed_per_date;
static std::mutex stats_mutex;

// Helper function to scan directory and create encoding tasks
static void scan_and_queue_assets(const std::string &base_dir,
                                  const std::string &date_str,
                                  TaskQueue &task_queue,
                                  const std::string &output_base,
                                  std::atomic<int> &total_assets) {
  if (!std::filesystem::exists(base_dir))
    return;

  int assets_found = 0;
  for (const auto &entry : std::filesystem::directory_iterator(base_dir)) {
    if (entry.is_directory()) {
      std::string asset_code = entry.path().filename().string();
      EncodingTask task{
          entry.path().string(),
          asset_code,
          date_str,
          output_base};
      task_queue.push(task);
      total_assets.fetch_add(1);
      assets_found++;
    }
  }

  // Update per-date statistics
  {
    std::lock_guard<std::mutex> lock(stats_mutex);
    assets_per_date[date_str] += assets_found;
  }

  std::cout << "Date " << date_str << ": Found " << assets_found
            << " assets in " << base_dir << std::endl;
}

bool process_stock_data(const std::string &asset_dir,
                        const std::string &asset_code,
                        const std::string &date_str,
                        const std::string &output_base) {
  // Parse date components for directory structure
  std::string year = date_str.substr(0, 4);
  std::string month = date_str.substr(4, 2);
  std::string day = date_str.substr(6, 2);

  // Create output directory structure
  std::string output_dir = output_base + "/" + year + "/" + month + "/" + day + "/" + asset_code;
  std::filesystem::create_directories(output_dir);

  // Create encoder instance with optimized buffer sizes
  L2::BinaryEncoder_L2 encoder(500000, 2000000);

  if (!std::filesystem::exists(asset_dir)) {
    return false;
  }

  std::vector<L2::Snapshot> snapshots;
  std::vector<L2::Order> all_orders;

  // Process snapshots from 行情.csv
  std::string snapshot_file = asset_dir + "/行情.csv";
  if (std::filesystem::exists(snapshot_file)) {
    std::vector<L2::CSVSnapshot> csv_snapshots;
    if (encoder.parse_snapshot_csv(snapshot_file, csv_snapshots)) {
      snapshots.reserve(csv_snapshots.size());
      for (const auto &csv_snap : csv_snapshots) {
        snapshots.push_back(L2::BinaryEncoder_L2::csv_to_snapshot(csv_snap));
      }
    }
  }

  // Process orders from 逐笔委托.csv
  std::string order_file = asset_dir + "/逐笔委托.csv";
  if (std::filesystem::exists(order_file)) {
    std::vector<L2::CSVOrder> csv_orders;
    if (encoder.parse_order_csv(order_file, csv_orders)) {
      all_orders.reserve(csv_orders.size());
      for (const auto &csv_order : csv_orders) {
        all_orders.push_back(L2::BinaryEncoder_L2::csv_to_order(csv_order));
      }
    }
  }

  // Process trades from 逐笔成交.csv
  std::string trade_file = asset_dir + "/逐笔成交.csv";
  if (std::filesystem::exists(trade_file)) {
    std::vector<L2::CSVTrade> csv_trades;
    if (encoder.parse_trade_csv(trade_file, csv_trades)) {
      size_t original_size = all_orders.size();
      all_orders.reserve(original_size + csv_trades.size());
      for (const auto &csv_trade : csv_trades) {
        all_orders.push_back(L2::BinaryEncoder_L2::csv_to_trade(csv_trade));
      }
    }
  }

  // Sort orders by time
  if (!all_orders.empty()) {
    std::sort(all_orders.begin(), all_orders.end(), [](const L2::Order &a, const L2::Order &b) {
      if (a.hour != b.hour)
        return a.hour < b.hour;
      if (a.minute != b.minute)
        return a.minute < b.minute;
      if (a.second != b.second)
        return a.second < b.second;
      return a.millisecond < b.millisecond;
    });
  }

  // Encode and save data directly in asset directory
  double compression_ratio = 1.0;
  if (!snapshots.empty()) {
    std::string output_file = output_dir + "/snapshots_" + std::to_string(snapshots.size()) + ".bin";
    encoder.encode_snapshots(snapshots, output_file, L2::ENABLE_DELTA_ENCODING);
    compression_ratio = encoder.get_compression_stats().ratio;
  }

  if (!all_orders.empty()) {
    std::string output_file = output_dir + "/orders_" + std::to_string(all_orders.size()) + ".bin";
    encoder.encode_orders(all_orders, output_file, L2::ENABLE_DELTA_ENCODING);
    // Use the latest compression ratio if orders were processed
    compression_ratio = encoder.get_compression_stats().ratio;
  }

  // An asset is successfully processed if we have snapshots (market data)
  // Orders and trades are optional - some assets may only have snapshot data
  bool success = !snapshots.empty();

  if (success) {
    // Update per-date completion statistics
    std::lock_guard<std::mutex> lock(stats_mutex);
    completed_per_date[date_str]++;

    // Show progress for current date
    int current_completed = completed_per_date[date_str];
    int total_for_date = assets_per_date[date_str];
    if (total_for_date > 0) {
      std::ostringstream msg;
      msg << asset_code << " -> " << output_dir << " (" << std::fixed << std::setprecision(2) << compression_ratio << "x)";
      misc::print_progress(current_completed, total_for_date, msg.str());
    }
  }

  return success;
}

bool decompress_7z(const std::string &archive_path, const std::string &output_dir) {
  if (!std::filesystem::exists(archive_path)) {
    return false;
  }

  std::filesystem::create_directories(output_dir);

  // Fast 7z decompression with minimal output
  std::string command = "7z x \"" + archive_path + "\" -o\"" + output_dir + "\" -y > /dev/null 2>&1";
  return std::system(command.c_str()) == 0;
}

void decompression_worker(BufferState &buffer_state,
                          TaskQueue &task_queue,
                          const std::string &output_base,
                          std::atomic<int> &total_assets,
                          unsigned int worker_id) {
  // Set thread affinity
  if (misc::Affinity::supported()) {
    misc::Affinity::pin_to_core(worker_id);
  }

  std::cout << "[Decompression Worker " << worker_id << "] Started" << std::endl;

  std::string archive_path;
  while (buffer_state.get_next_archive(archive_path)) {
    std::string archive_name = std::filesystem::path(archive_path).stem().string();
    std::string date_folder = buffer_state.get_date_folder(archive_path);
    
    std::cout << "[Decompression Worker " << worker_id << "] Processing: " << archive_name << std::endl;

    // Clean up any existing date folder before decompression
    if (std::filesystem::exists(date_folder)) {
      std::filesystem::remove_all(date_folder);
    }

    // Decompress archive to temp_base (7z will create the YYYYMMDD folder)
    if (!decompress_7z(archive_path, g_config.temp_base)) {
      std::cout << "[Decompression Worker " << worker_id << "] Failed to decompress: " 
                << archive_name << std::endl;
      continue;
    }

    // 7z always creates the YYYYMMDD folder, use it directly
    assert(std::filesystem::exists(date_folder));
    scan_and_queue_assets(date_folder, archive_name, task_queue, output_base, total_assets);

    // Signal buffer ready for encoding
    buffer_state.signal_folder_ready(date_folder);
    
    std::cout << "[Decompression Worker " << worker_id << "] Completed: " << archive_name << std::endl;
  }
  
  std::cout << "[Decompression Worker " << worker_id << "] Finished" << std::endl;
}

void encoding_worker(TaskQueue &task_queue,
                     BufferState &buffer_state,
                     unsigned int core_id,
                     std::atomic<int> &completed_tasks) {
  // Set thread affinity
  if (misc::Affinity::supported()) {
    misc::Affinity::pin_to_core(core_id);
  }

  std::string current_date_folder;

  EncodingTask task;
  while (task_queue.pop(task)) {
    // Get date folder from task path
    std::string task_date_folder = task.asset_dir.substr(0, task.asset_dir.find_last_of('/'));
    task_date_folder = task_date_folder.substr(0, task_date_folder.find_last_of('/'));

    // Switch to new date folder if needed
    if (current_date_folder != task_date_folder) {
      current_date_folder = buffer_state.get_ready_folder();
      if (current_date_folder.empty()) {
        break;
      }
    }

    // Process task if it belongs to current date folder
    if (task_date_folder == current_date_folder) {
      if (process_stock_data(task.asset_dir, task.asset_code, task.date_str, task.output_base)) {
        completed_tasks.fetch_add(1);
      }
    } else {
      // Put task back and try to get correct date folder
      task_queue.push(task);
    }
  }

  // Encoding worker finished
}

} // namespace Parallel
} // namespace L2
