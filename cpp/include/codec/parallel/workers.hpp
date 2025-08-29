#pragma once

#include "processing_types.hpp"
#include <string>
#include <vector>
#include <atomic>

namespace L2 {
namespace Parallel {

/**
 * Process a single stock's data files and encode to binary format
 * @param asset_dir Directory containing CSV files for the asset
 * @param asset_code Stock code (e.g., 600000.SH)
 * @param date_str Date string in YYYYMMDD format
 * @param output_base Base output directory
 * @return true if processing succeeded
 */
bool process_stock_data(const std::string& asset_dir, 
                       const std::string& asset_code,
                       const std::string& date_str,
                       const std::string& output_base);

/**
 * Encoding worker function for multi-threaded processing
 * @param task_queue Queue to receive encoding tasks from
 * @param core_id CPU core to bind this thread to
 * @param completed_tasks Counter for completed tasks
 */
void encoding_worker(TaskQueue& task_queue, 
                    unsigned int core_id, 
                    std::atomic<int>& completed_tasks);

/**
 * Enhanced encoding worker that respects multi-buffer coordination
 * @param task_queue Queue to receive encoding tasks from
 * @param multi_buffer Multi-buffer state coordinator
 * @param core_id CPU core to bind this thread to
 * @param completed_tasks Counter for completed tasks
 */
void encoding_worker_with_multibuffer(TaskQueue& task_queue, 
                                      MultiBufferState& multi_buffer,
                                      unsigned int core_id, 
                                      std::atomic<int>& completed_tasks);

/**
 * Archive decompression worker function
 * @param archive_subset Vector of archive file paths for this worker to process
 * @param multi_buffer Multi-buffer state coordinator
 * @param task_queue Queue to send encoding tasks to
 * @param output_base Base output directory
 * @param total_assets Counter for total assets discovered
 * @param worker_id ID of this decompression worker thread
 */
void decompression_worker(const std::vector<std::string>& archive_subset, 
                         MultiBufferState& multi_buffer, 
                         TaskQueue& task_queue,
                         const std::string& output_base,
                         std::atomic<int>& total_assets,
                         unsigned int worker_id);

/**
 * Decompress 7z file using system command
 * @param archive_path Path to the 7z archive
 * @param output_dir Directory to extract to
 * @return true if decompression succeeded
 */
bool decompress_7z(const std::string& archive_path, const std::string& output_dir);



} // namespace Parallel
} // namespace L2
