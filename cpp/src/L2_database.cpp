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

  assert(g_config.decompression_threads > 0 && encoding_threads > 0);

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

  // Setup directories and simple buffer state
  std::filesystem::create_directories(g_config.temp_base);
  std::filesystem::create_directories(g_config.output_base);
  BufferState buffer_state(g_config.temp_base);

  // Fast archive discovery 
  std::vector<std::string> all_archives;
  std::filesystem::recursive_directory_iterator dir_iter(g_config.input_base);
  for (const auto &entry : dir_iter) {
    if (entry.is_regular_file() && entry.path().extension() == ".7z") {
      all_archives.push_back(entry.path().string());
    }
  }
  std::sort(all_archives.begin(), all_archives.end());
  std::cout << "Processing " << all_archives.size() << " archives" << std::endl;

  // Add all archives to buffer state
  for (const auto& archive : all_archives) {
    buffer_state.add_archive(archive);
  }

  // Initialize work coordination
  TaskQueue task_queue;
  std::atomic<int> completed_tasks{0};
  std::atomic<int> total_assets{0};

  // Launch all workers
  std::vector<std::thread> all_workers;

  // Decompression workers
  for (unsigned int i = 0; i < g_config.decompression_threads; ++i) {
    all_workers.emplace_back(decompression_worker,
                             std::ref(buffer_state),
                             std::ref(task_queue),
                             std::string(g_config.output_base),
                             std::ref(total_assets),
                             i);
  }

  // Encoding workers
  for (unsigned int i = 0; i < encoding_threads; ++i) {
    unsigned int core_id = g_config.decompression_threads + i;
    all_workers.emplace_back(encoding_worker,
                             std::ref(task_queue),
                             std::ref(buffer_state),
                             core_id,
                             std::ref(completed_tasks));
  }

  // Wait for decompression threads to finish
  for (unsigned int i = 0; i < g_config.decompression_threads; ++i) {
    all_workers[i].join();
  }
  buffer_state.signal_decompression_finished();
  task_queue.finish();

  // Wait for encoding workers
  for (unsigned int i = g_config.decompression_threads; i < all_workers.size(); ++i) {
    all_workers[i].join();
  }

  std::cout << "Processing complete: " << completed_tasks.load() << "/" << total_assets.load()
            << " assets processed across all dates" << std::endl;
  
  return 0;
}
