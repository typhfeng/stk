#include "codec/parallel/workers.hpp"
#include "codec/parallel/processing_config.hpp"
#include "codec/binary_encoder_L2.hpp"
#include "misc/affinity.hpp"

#include <filesystem>
#include <algorithm>
#include <cassert>
#include <cstring>
#include <cstdlib>

namespace L2 {
namespace Parallel {

// Helper function to scan directory and create encoding tasks
static void scan_and_queue_assets(const std::string& base_dir, 
                                  const std::string& date_str,
                                  TaskQueue& task_queue,
                                  const std::string& output_base,
                                  std::atomic<int>& total_assets) {
    if (!std::filesystem::exists(base_dir)) return;
    
    for (const auto& entry : std::filesystem::directory_iterator(base_dir)) {
        if (entry.is_directory()) {
            EncodingTask task{
                entry.path().string(),
                entry.path().filename().string(),
                date_str,
                output_base
            };
            task_queue.push(task);
            total_assets.fetch_add(1);
        }
    }
}

bool process_stock_data(const std::string& asset_dir, 
                       const std::string& asset_code,
                       const std::string& date_str,
                       const std::string& output_base) {
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
            for (const auto& csv_snap : csv_snapshots) {
                snapshots.push_back(L2::BinaryEncoder_L2::csv_to_snapshot(csv_snap));
            }
        }
    }
    
    // Process orders from 委托队列.csv
    std::string order_file = asset_dir + "/委托队列.csv";
    if (std::filesystem::exists(order_file)) {
        std::vector<L2::CSVOrder> csv_orders;
        if (encoder.parse_order_csv(order_file, csv_orders)) {
            all_orders.reserve(csv_orders.size());
            for (const auto& csv_order : csv_orders) {
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
            for (const auto& csv_trade : csv_trades) {
                all_orders.push_back(L2::BinaryEncoder_L2::csv_to_trade(csv_trade));
            }
        }
    }
    
    // Sort orders by time
    if (!all_orders.empty()) {
        std::sort(all_orders.begin(), all_orders.end(), [](const L2::Order& a, const L2::Order& b) {
            if (a.hour != b.hour) return a.hour < b.hour;
            if (a.minute != b.minute) return a.minute < b.minute;
            if (a.second != b.second) return a.second < b.second;
            return a.millisecond < b.millisecond;
        });
    }
    
    // Encode and save data
    bool success = false;
    if (!snapshots.empty()) {
        std::string snapshots_dir = output_dir + "/snapshots";
        std::filesystem::create_directories(snapshots_dir);
        std::string output_file = snapshots_dir + "/snapshots_" + std::to_string(snapshots.size()) + ".bin";
        success |= encoder.encode_snapshots(snapshots, output_file, L2::ENABLE_DELTA_ENCODING);
    }
    
    if (!all_orders.empty()) {
        std::string orders_dir = output_dir + "/orders";
        std::filesystem::create_directories(orders_dir);
        std::string output_file = orders_dir + "/orders_" + std::to_string(all_orders.size()) + ".bin";
        success |= encoder.encode_orders(all_orders, output_file, L2::ENABLE_DELTA_ENCODING);
    }
    
    return success;
}

void encoding_worker(TaskQueue& task_queue, unsigned int core_id, std::atomic<int>& completed_tasks) {
    if (misc::Affinity::supported()) {
        misc::Affinity::pin_to_core(core_id);
    }
    
    EncodingTask task;
    while (task_queue.pop(task)) {
        if (process_stock_data(task.asset_dir, task.asset_code, task.date_str, task.output_base)) {
            completed_tasks.fetch_add(1);
        }
    }
}

bool decompress_7z(const std::string& archive_path, const std::string& output_dir) {
    if (!std::filesystem::exists(archive_path)) {
        return false;
    }
    
    std::filesystem::create_directories(output_dir);
    
    // Fast 7z decompression with minimal output
    std::string command = "7z x \"" + archive_path + "\" -o\"" + output_dir + "\" -y > /dev/null 2>&1";
    return std::system(command.c_str()) == 0;
}

void decompression_worker(const std::vector<std::string>& archive_subset, 
                         MultiBufferState& multi_buffer, 
                         TaskQueue& task_queue,
                         const std::string& output_base,
                         std::atomic<int>& total_assets,
                         unsigned int worker_id) {
    // Set thread affinity
    if (misc::Affinity::supported()) {
        misc::Affinity::pin_to_core(worker_id);
    }
    
    if (g_config.skip_decompression) {
        // Debug mode: only worker 0 scans existing buffer directories to avoid redundancy
        if (worker_id == 0) {
            for (const auto& buffer_entry : std::filesystem::directory_iterator(g_config.temp_base)) {
                if (!buffer_entry.is_directory()) continue;
                
                std::string buffer_name = buffer_entry.path().filename().string();
                if (buffer_name.find("buffer_") != 0) continue;
                
                std::string decomp_dir = buffer_entry.path().string();
                
                // Scan for date directories in this buffer
                for (const auto& date_entry : std::filesystem::directory_iterator(decomp_dir)) {
                    if (!date_entry.is_directory()) continue;
                    
                    std::string date_str = date_entry.path().filename().string();
                    if (date_str.length() != 8) continue; // Skip non-date directories
                    
                    // Find and queue assets
                    scan_and_queue_assets(date_entry.path().string(), date_str, task_queue, output_base, total_assets);
                }
                
                // Signal this buffer is ready for encoding
                multi_buffer.signal_ready(decomp_dir);
            }
        }
    } else {
        // Normal mode: decompress archives
        for (const std::string& archive_path : archive_subset) {
            std::string archive_name = std::filesystem::path(archive_path).stem().string();
            std::string decomp_dir = multi_buffer.get_available_decomp_dir();
            
            // Prepare directory
            if (std::filesystem::exists(decomp_dir)) {
                std::filesystem::remove_all(decomp_dir);
            }
            std::filesystem::create_directories(decomp_dir);
            
            // Decompress archive
            if (!decompress_7z(archive_path, decomp_dir)) {
                continue;
            }
            
            // Find and queue assets
            std::string date_dir = decomp_dir + "/" + archive_name;
            scan_and_queue_assets(date_dir, archive_name, task_queue, output_base, total_assets);
            
            multi_buffer.signal_ready(decomp_dir);
        }
    }
}

void encoding_worker_with_multibuffer(TaskQueue& task_queue, 
                                      MultiBufferState& multi_buffer,
                                      unsigned int core_id, 
                                      std::atomic<int>& completed_tasks) {
    // Set thread affinity
    if (misc::Affinity::supported()) {
        misc::Affinity::pin_to_core(core_id);
    }
    
    std::string current_working_dir;
    
    EncodingTask task;
    while (task_queue.pop(task)) {
        // Extract buffer directory from task path
        std::string task_base_dir = task.asset_dir.substr(0, task.asset_dir.find_last_of('/'));
        task_base_dir = task_base_dir.substr(0, task_base_dir.find_last_of('/'));
        
        if (current_working_dir.empty()) {
            current_working_dir = multi_buffer.wait_for_ready_dir();
            if (current_working_dir.empty()) break;
        }
        
        // Process task if it belongs to current working directory
        if (task_base_dir == current_working_dir) {
            if (process_stock_data(task.asset_dir, task.asset_code, task.date_str, task.output_base)) {
                completed_tasks.fetch_add(1);
            }
        } else {
            // Switch directories
            task_queue.push(task);
            if (!current_working_dir.empty()) {
                multi_buffer.finish_with_dir(current_working_dir);
            }
            current_working_dir = multi_buffer.wait_for_ready_dir();
            if (current_working_dir.empty()) break;
        }
    }
    
    if (!current_working_dir.empty()) {
        multi_buffer.finish_with_dir(current_working_dir);
    }
}



} // namespace Parallel
} // namespace L2
