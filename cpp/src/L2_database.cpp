// L2 Database High-Performance Processing Pipeline
// Optimized for processing millions of compressed stock data files

#include "codec/parallel/processing_config.hpp"
#include "codec/parallel/processing_types.hpp"
#include "codec/parallel/workers.hpp"
#include "misc/affinity.hpp"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <iostream>
#include <thread>
#include <vector>

int main() {
  using namespace L2::Parallel;

  // Simple core-based configuration
  unsigned int num_cores = misc::Affinity::core_count();
  // Use configured decompression threads, all remaining cores for encoding
  unsigned int encoding_threads = num_cores - g_config.decompression_threads;

  assert(g_config.decompression_threads > 0 && encoding_threads > 0 && g_config.decompression_buffers > g_config.decompression_threads);

  std::cout << "L2 Processing" << (g_config.skip_decompression ? " (Debug mode)" : "") 
            << ": " << g_config.decompression_threads
            << " decomp, " << encoding_threads << " enc threads" << std::endl;

  // Clean previous runs (but not temp in debug mode)
  if (std::filesystem::exists(g_config.output_base)) {
    std::filesystem::remove_all(g_config.output_base);
  }
  if (!g_config.skip_decompression && std::filesystem::exists(g_config.temp_base)) {
    std::filesystem::remove_all(g_config.temp_base);
  }

  // Setup directories and buffers
  std::filesystem::create_directories(g_config.temp_base);
  std::filesystem::create_directories(g_config.output_base);
  MultiBufferState multi_buffer(g_config.temp_base, g_config.decompression_buffers);

  // Fast archive discovery with glob pattern (skip in debug mode)
  std::vector<std::string> all_archives;
  if (!g_config.skip_decompression) {
    std::filesystem::recursive_directory_iterator dir_iter(g_config.input_base);
    for (const auto &entry : dir_iter) {
      if (entry.is_regular_file() && entry.path().extension() == ".7z") {
        all_archives.push_back(entry.path().string());
      }
    }
    std::sort(all_archives.begin(), all_archives.end());
    std::cout << "Processing " << all_archives.size() << " archives" << std::endl;
  } else {
    // In debug mode, create dummy archive entries to maintain worker structure
    all_archives.push_back(""); // Empty entry for debug mode
    std::cout << "Debug mode: Processing existing temp data" << std::endl;
  }

  // Initialize simple work coordination
  TaskQueue task_queue;
  std::atomic<int> completed_tasks{0};
  std::atomic<int> total_assets{0};

  // Round-robin archive distribution
  std::vector<std::vector<std::string>> archive_subsets(g_config.decompression_threads);
  for (size_t i = 0; i < all_archives.size(); ++i) {
    archive_subsets[i % g_config.decompression_threads].push_back(all_archives[i]);
  }

  // Launch all workers
  std::vector<std::thread> all_workers;

  // Decompression workers
  for (unsigned int i = 0; i < g_config.decompression_threads; ++i) {
    if (!archive_subsets[i].empty()) {
      all_workers.emplace_back(decompression_worker,
                               std::cref(archive_subsets[i]),
                               std::ref(multi_buffer),
                               std::ref(task_queue),
                               std::string(g_config.output_base),
                               std::ref(total_assets),
                               i);
    }
  }

  // Encoding workers
  for (unsigned int i = 1; i <= encoding_threads; ++i) {
    all_workers.emplace_back(encoding_worker_with_multibuffer,
                             std::ref(task_queue),
                             std::ref(multi_buffer),
                             i,
                             std::ref(completed_tasks));
  }

  // Wait for decompression to finish, then signal completion
  for (unsigned int i = 0; i < g_config.decompression_threads; ++i) {
    if (i < all_workers.size()) {
      all_workers[i].join();
    }
  }
  multi_buffer.signal_decompression_finished();
  task_queue.finish();

  // Wait for remaining encoding workers
  for (unsigned int i = g_config.decompression_threads; i < all_workers.size(); ++i) {
    all_workers[i].join();
  }

  std::cout << "Complete: " << completed_tasks.load() << "/" << total_assets.load()
            << " assets processed" << std::endl;
  return 0;
}
