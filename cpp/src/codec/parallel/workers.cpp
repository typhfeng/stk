/*
 * PARALLEL PROCESSING WORKERS - SIMPLE & ROBUST DESIGN
 * 
 * =============================================================================
 * CORE PRINCIPLE: ASSET QUEUE + FOLDER REFCOUNT (NO COMPLEX COORDINATION)
 * =============================================================================
 * 
 * Decompressors produce per-asset work items. Encoders consume assets independently.
 * Folder cleanup is driven by a per-folder atomic refcount that reaches zero.
 * 
 * GLOBAL STATE (ALL ATOMIC OR SIMPLE CONTAINERS):
 * - std::atomic<int> active_temp_folders = 0
 *     Current number of temp folders occupying disk. Limits decompressor concurrency.
 * - std::atomic<bool> producers_done = false
 *     Set true when all archives are decompressed and all assets have been enqueued.
 * - asset_queue (bounded MPMC)
 *     A simple bounded queue of work items. Implementation can be mutex + deque.
 *     Each item: {folder_id, asset_dir, asset_code, date_str, temp_root}.
 * - folder_meta (map: folder_id → metadata)
 *     Protected by a simple mutex for create/lookup; atomics inside for hot fields.
 *     Metadata fields:
 *       - std::atomic<int> remaining      // decremented once per completed asset
 *       - int total                        // fixed at enqueue-time (for progress)
 *       - std::atomic<int> processed      // increments once per completed asset
 *       - std::string temp_root           // folder to delete on completion
 *       - std::string date_str            // e.g., 20170104 (for progress line)
 * 
 * =============================================================================
 * DECOMPRESSION WORKER LOOP (PRODUCER):
 * =============================================================================
 * 
 * STEP 1: DISK BACKPRESSURE (SIMPLE POLLING)
 *   - If active_temp_folders >= MAX_TEMP_FOLDERS, sleep ~100ms and recheck.
 * 
 * STEP 2: TAKE NEXT ARCHIVE AND RESERVE FOLDER SLOT
 *   - Pop next archive from archive_queue (brief mutex lock).
 *   - Atomically increment active_temp_folders.
 *   - Create unique temp_root for this archive (per-day folder).
 * 
 * STEP 3: DECOMPRESS
 *   - Extract archive into temp_root (may contain nested assets directory).
 * 
 * STEP 4: DISCOVER ASSETS AND INITIALIZE FOLDER META
 *   - Enumerate asset directories under the day folder.
 *   - Compute total_assets = discovered assets.size().
 *   - Create folder_meta[folder_id] with:
 *       remaining = total_assets, total = total_assets,
 *       processed = 0, temp_root, date_str.
 * 
 * STEP 5: ENQUEUE ONE WORK ITEM PER ASSET
 *   - For each asset, push {folder_id, asset_dir, asset_code, date_str, temp_root}
 *     into asset_queue. If queue is full, poll/sleep briefly until space.
 *   - If total_assets == 0: delete temp_root immediately; decrement active_temp_folders.
 * 
 * STEP 6: LOOP
 *   - Repeat until no archives remain. After all enqueues complete, set producers_done = true.
 * 
 * =============================================================================
 * ENCODING WORKER LOOP (CONSUMER):
 * =============================================================================
 * 
 * STEP 1: POP WORK ITEM
 *   - Pop from asset_queue. If empty and producers_done == false, sleep briefly and retry.
 *   - If empty and producers_done == true: exit worker.
 * 
 * STEP 2: PROCESS ONE ASSET
 *   - Encode CSVs → binaries for the given asset_dir.
 *   - Update progress for this folder: processed++ (atomic fetch_add).
 *   - Optionally print progress line using folder_meta[folder_id]:
 *       "[=====>                                  ]  14% (430/3033) 20170104 002789.SZ (2.5x)"
 *     where percent = processed / total for that date_str.
 * 
 * STEP 3: DECREMENT FOLDER REFCOUNT AND CLEANUP ON ZERO
 *   - prev = remaining.fetch_sub(1). If prev == 1, this thread cleans up:
 *       a) Delete temp_root.
 *       b) active_temp_folders.fetch_sub(1).
 *       c) Remove folder_id from folder_meta (brief mutex).
 * 
 * STEP 4: LOOP
 *   - Continue until termination condition in STEP 1.
 * 
 * =============================================================================
 * WHY THIS DESIGN IS SIMPLE & ROBUST:
 * =============================================================================
 * 
 * 1. MINIMAL SHARED STATE: Only an asset queue and per-folder refcounts.
 * 2. NO CENTRAL ASSIGNMENT: Encoders do not coordinate; they just pop and work.
 * 3. CLEANUP IS OBVIOUS: The thread that decrements remaining from 1→0 cleans up.
 * 4. CLEAR BACKPRESSURE: Disk via active_temp_folders; CPU via bounded asset_queue.
 * 5. GOOD LOAD BALANCE: Uniform assets distribute naturally across encoders.
 * 6. EASY PROGRESS: folder_meta gives total/processed for precise per-day progress.
 * 7. SIMPLE BLOCKING: Polling with short sleeps; no condition variables required.
 * 
 * MEMORY USAGE: Max MAX_TEMP_FOLDERS temp folders on disk (e.g., 4 × ~2GB ≈ ~8GB).
 * PERFORMANCE: Asset-parallelism across encoders; minimal contention (queue + atomics).
 * ROBUSTNESS: If a worker dies, others continue; cleanup still happens on refcount zero.
 * 
 * =============================================================================
 * ASSET-LEVEL QUEUE DETAILS:
 * =============================================================================
 * 
 * WORK ITEM FORMAT:
 *   {folder_id, asset_dir, asset_code, date_str, temp_root}
 * 
 * QUEUE SEMANTICS (MPMC):
 *   - Bounded to cap memory and encourage steady-state processing.
 *   - Producers push N items per folder; consumers pop independently.
 *   - If empty: consumers poll/sleep briefly. If full: producers poll/sleep briefly.
 * 
 * FOLDER PROGRESS & DISPLAY:
 *   - folder_meta[folder_id].total      → constant per day
 *   - folder_meta[folder_id].processed  → increments per completed asset
 *   - percent = (processed * 100) / total
 *   - Example: "[=====>                                  ]  14% (430/3033) 20170104 002789.SZ (2.5x)"
 * 
 * COMPLETION CONDITION:
 *   - A folder is complete when remaining hits zero (single-thread cleanup).
 *   - All encoders exit when producers_done == true and asset_queue is empty.
 */

#include "codec/parallel/workers.hpp"
#include "codec/binary_encoder_L2.hpp"
#include "codec/parallel/processing_config.hpp"
#include "codec/parallel/processing_types.hpp"
#include "misc/affinity.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <thread>

// External shutdown flag
extern std::atomic<bool> g_shutdown_requested;

namespace L2 {
namespace Parallel {

// Decompression logging
static std::ofstream decomp_log;
static std::mutex decomp_log_mutex;
static std::atomic<int> total_archives{0};
static std::atomic<int> completed_archives{0};



// Initialize decompression logging
static void init_decomp_log() {
  // Create absolute path to ensure log file is in the correct location
  std::filesystem::path temp_path = std::filesystem::absolute(g_config.temp_base);
  std::filesystem::create_directories(temp_path);
  std::filesystem::path log_path = temp_path / "decompression.log";

  decomp_log.open(log_path);
  if (decomp_log.is_open()) {
    decomp_log << "Decompression Log Started at: " << log_path << std::endl;
    decomp_log.flush();
    std::cout << "Decompression log: " << log_path << std::endl;
  } else {
    std::cerr << "Failed to create decompression log at: " << log_path << std::endl;
  }
}

// Close decompression logging
static void close_decomp_log() {
  if (decomp_log.is_open()) {
    decomp_log << "Decompression Log Ended" << std::endl;
    decomp_log.close();
  }
}

// Log to decompression file with progress
static void log_decomp(const std::string &message) {
  std::lock_guard<std::mutex> lock(decomp_log_mutex);
  if (decomp_log.is_open()) {
    int completed = completed_archives.load();
    int total = total_archives.load();
    if (total > 0) {
      int progress = (completed * 100) / total;
      decomp_log << "[" << progress << "%] " << message << std::endl;
      decomp_log.flush();
    } else {
      decomp_log << message << std::endl;
      decomp_log.flush();
    }
  }
}



bool process_stock_data(const std::string &asset_dir,
                        const std::string &asset_code,
                        const std::string &date_str,
                        const std::string &output_base,
                        double &compression_ratio) {
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
  compression_ratio = 1.0;
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

  // Asset must have snapshots (market data) - fail immediately if missing
  if (snapshots.empty()) {
    std::cerr << "[ERROR] " << asset_code << " (" << date_str << ") - no snapshot data found, terminating" << std::endl;
    std::exit(1);
  }

  return true;
}

bool decompress_7z(const std::string &archive_path, const std::string &output_dir) {
  if (!std::filesystem::exists(archive_path)) {
    std::cerr << "[ERROR] Archive not found: " << archive_path << std::endl;
    return false;
  }

  std::filesystem::create_directories(output_dir);

  // Use 7z decompression with error output visible
  std::string command = "7z x \"" + archive_path + "\" -o\"" + output_dir + "\" -y";
  int result = std::system(command.c_str());
  if (result != 0) {
    std::cerr << "[ERROR] Failed to decompress: " << archive_path << " (exit code: " << result << ")" << std::endl;
    return false;
  }
  return true;
}

void decompression_worker(unsigned int worker_id) {
  log_decomp("Decompression Worker " + std::to_string(worker_id) + " Started");

  while (!g_shutdown_requested.load()) {
    // STEP 1: DISK BACKPRESSURE (SIMPLE POLLING)
    if (active_temp_folders.load() >= static_cast<int>(g_config.max_temp_folders)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }

    // STEP 2: TAKE NEXT ARCHIVE AND RESERVE FOLDER SLOT
    std::string archive_path;
    {
      std::lock_guard<std::mutex> lock(archive_queue_mutex);
      if (archive_queue.empty()) {
        break; // no more archives
      }
      archive_path = archive_queue.front();
      archive_queue.pop();
    }
    
    // Atomically increment active_temp_folders
    active_temp_folders.fetch_add(1);
    
    // Create unique temp_root for this archive (per-day folder)
    std::string archive_name = std::filesystem::path(archive_path).stem().string();
    std::string temp_root = std::string(g_config.temp_base) + "/" + archive_name + "_" + 
                           std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());

    log_decomp("Decompression Worker " + std::to_string(worker_id) + " Processing: " + archive_name);

    // STEP 3: DECOMPRESS
    if (!decompress_7z(archive_path, temp_root)) {
      std::cerr << "[ERROR] Failed to decompress: " << archive_path << std::endl;
      active_temp_folders.fetch_sub(1);
      continue;
    }

    // STEP 4: DISCOVER ASSETS AND INITIALIZE FOLDER META
    std::string assets_folder = temp_root;
    std::string nested_assets = temp_root + "/" + archive_name;
    if (std::filesystem::exists(nested_assets)) {
      assets_folder = nested_assets;
    }

    if (!std::filesystem::exists(assets_folder)) {
      std::cerr << "[ERROR] No assets found in " << temp_root << " after decompression" << std::endl;
      std::filesystem::remove_all(temp_root);
      active_temp_folders.fetch_sub(1);
      continue;
    }

    // Enumerate asset directories under the day folder
    std::vector<std::pair<std::string, std::string>> discovered_assets;
    try {
      for (const auto &asset_entry : std::filesystem::directory_iterator(assets_folder)) {
        if (asset_entry.is_directory()) {
          std::string asset_code = asset_entry.path().filename().string();
          std::string asset_dir = asset_entry.path().string();
          discovered_assets.emplace_back(asset_dir, asset_code);
        }
      }
    } catch (const std::exception &e) {
      std::cerr << "[ERROR] Failed to scan assets in " << assets_folder << ": " << e.what() << std::endl;
      std::filesystem::remove_all(temp_root);
      active_temp_folders.fetch_sub(1);
      continue;
    }

    int total_assets = discovered_assets.size();
    std::string folder_id = archive_name;
    std::string date_str = archive_name; // e.g., 20170104

    // Create folder_meta with remaining = total_assets
    {
      std::lock_guard<std::mutex> lock(folder_meta_mutex);
      folder_meta.emplace(folder_id, FolderMeta(total_assets, temp_root, date_str));
    }

    // STEP 5: ENQUEUE ONE WORK ITEM PER ASSET
    if (total_assets == 0) {
      // delete temp_root immediately; decrement active_temp_folders
      std::filesystem::remove_all(temp_root);
      active_temp_folders.fetch_sub(1);
      completed_archives.fetch_add(1);
      continue;
    }

    for (const auto &[asset_dir, asset_code] : discovered_assets) {
      AssetWorkItem item{folder_id, asset_dir, asset_code, date_str, temp_root};
      // If queue is full, poll/sleep briefly until space
      while (!asset_queue.try_push(item) && !g_shutdown_requested.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }

    log_decomp("Decompression Worker " + std::to_string(worker_id) + " Completed: " + archive_name + 
               " (" + std::to_string(total_assets) + " assets)");
    completed_archives.fetch_add(1);
  }

  log_decomp("Decompression Worker " + std::to_string(worker_id) + " Finished");
}

void encoding_worker(unsigned int core_id) {
  // Set thread affinity
  if (misc::Affinity::supported()) {
    misc::Affinity::pin_to_core(core_id);
  }

  while (!g_shutdown_requested.load()) {
    // STEP 1: POP WORK ITEM
    AssetWorkItem item;
    if (!asset_queue.try_pop(item)) {
      // If empty and producers_done == false, sleep briefly and retry
      if (!producers_done.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        continue;
      } else {
        // If empty and producers_done == true: exit worker
        break;
      }
    }

    // STEP 2: PROCESS ONE ASSET
    double compression_ratio = 1.0;
    if (process_stock_data(item.asset_dir, item.asset_code, item.date_str, g_config.output_base, compression_ratio)) {
      // Update progress for this folder: processed++ (atomic fetch_add)
      int processed_count;
      {
        std::lock_guard<std::mutex> lock(folder_meta_mutex);
        auto it = folder_meta.find(item.folder_id);
        if (it != folder_meta.end()) {
          processed_count = it->second.processed.fetch_add(1) + 1;
          int total = it->second.total;
          // Optionally print progress line using folder_meta
          if (processed_count % 10 == 0 || processed_count == total) { // show every 10th or final
            int percent = (processed_count * 100) / total;
            std::string progress_bar;
            int bar_width = 40;
            int filled = (percent * bar_width) / 100;
            progress_bar = "[" + std::string(filled, '=') + (filled < bar_width ? ">" : "") + 
                          std::string(bar_width - filled - (filled < bar_width ? 1 : 0), ' ') + "]";
            std::cout << progress_bar << " " << std::setw(3) << percent << "% (" 
                     << processed_count << "/" << total << ") " << item.date_str << " " 
                     << item.asset_code << " (" << std::fixed << std::setprecision(1) 
                     << compression_ratio << "x)" << std::endl;
          }
        }
      }
    }

    // STEP 3: DECREMENT FOLDER REFCOUNT AND CLEANUP ON ZERO
    bool should_cleanup = false;
    std::string temp_root_to_delete;
    {
      std::lock_guard<std::mutex> lock(folder_meta_mutex);
      auto it = folder_meta.find(item.folder_id);
      if (it != folder_meta.end()) {
        int prev_remaining = it->second.remaining.fetch_sub(1);
        if (prev_remaining == 1) {
          // This thread cleans up
          should_cleanup = true;
          temp_root_to_delete = it->second.temp_root;
          folder_meta.erase(it);
        }
      }
    }

    if (should_cleanup) {
      // a) Delete temp_root
      if (std::filesystem::exists(temp_root_to_delete)) {
        std::filesystem::remove_all(temp_root_to_delete);
      }
      // b) active_temp_folders.fetch_sub(1)
      active_temp_folders.fetch_sub(1);
    }
  }
}

void init_decompression_logging(int total_archive_count) {
  total_archives.store(total_archive_count);
  completed_archives.store(0);
  init_decomp_log();
}

void close_decompression_logging() {
  close_decomp_log();
}

void init_encoding_progress() {
  // Daily progress tracking - no global state needed
}

} // namespace Parallel
} // namespace L2
