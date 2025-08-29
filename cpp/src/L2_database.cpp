// L2 Database - Asset Queue + Folder Refcount Design
// Simple & robust parallel processing following exact design from workers.cpp

#include "codec/parallel/processing_config.hpp"
#include "codec/parallel/processing_types.hpp"
#include "codec/parallel/workers.hpp"
#include "misc/affinity.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <thread>
#include <vector>

// Global shutdown flag
std::atomic<bool> g_shutdown_requested{false};

void signal_handler(int signal) {
  std::cout << "\nReceived signal " << signal << ", requesting graceful shutdown..." << std::endl;
  g_shutdown_requested.store(true);
}

// Discover all .7z archives
std::vector<std::string> discover_archives(const std::string& input_base) {
  std::vector<std::string> archives;
  for (const auto &entry : std::filesystem::recursive_directory_iterator(input_base)) {
    if (entry.is_regular_file() && entry.path().extension() == ".7z") {
      archives.push_back(entry.path().string());
    }
  }
  std::sort(archives.begin(), archives.end());
  return archives;
}

int main() {
  using namespace L2::Parallel;

  // Setup signal handlers
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  // Calculate threads
  const unsigned int num_cores = misc::Affinity::core_count();
  const unsigned int encoding_threads = num_cores - g_config.decompression_threads;
  
  assert(g_config.decompression_threads > 0 && encoding_threads > 0);

  std::cout << "L2 Processing (Asset Queue Design): " 
            << g_config.decompression_threads << " decomp, " 
            << encoding_threads << " enc threads (max " 
            << g_config.max_temp_folders << " temp folders)" << std::endl;

  // Clean and setup directories
  if (std::filesystem::exists(g_config.output_base)) {
    std::filesystem::remove_all(g_config.output_base);
  }
  std::filesystem::create_directories(g_config.output_base);
  
  if (std::filesystem::exists(g_config.temp_base)) {
    std::filesystem::remove_all(g_config.temp_base);
  }
  std::filesystem::create_directories(g_config.temp_base);

  // Discover archives and populate archive queue
  const auto archives = discover_archives(g_config.input_base);
  std::cout << "Processing " << archives.size() << " archives" << std::endl;

  {
    std::lock_guard<std::mutex> lock(archive_queue_mutex);
    for (const auto& archive : archives) {
      archive_queue.push(archive);
    }
  }

  // Initialize global state
  active_temp_folders.store(0);
  producers_done.store(false);

  // Initialize logging
  init_decompression_logging(archives.size());

  // Launch decompression workers (producers)
  std::vector<std::thread> decompression_workers;
  for (unsigned int i = 0; i < g_config.decompression_threads; ++i) {
    decompression_workers.emplace_back(decompression_worker, i);
  }

  // Initialize encoding progress tracking
  init_encoding_progress();

  // Launch encoding workers (consumers)
  std::vector<std::thread> encoding_workers;
  for (unsigned int i = 0; i < encoding_threads; ++i) {
    const unsigned int core_id = g_config.decompression_threads + i;
    encoding_workers.emplace_back(encoding_worker, core_id);
  }

  // Wait for decompression workers to finish
  for (auto& worker : decompression_workers) {
    if (worker.joinable()) {
      worker.join();
    }
  }

  // After all enqueues complete, set producers_done = true
  producers_done.store(true);

  close_decompression_logging();
  std::cout << "Decompression phase complete" << std::endl;

  // Wait for encoding workers to finish
  for (auto& worker : encoding_workers) {
    if (worker.joinable()) {
      worker.join();
    }
  }

  std::cout << "Processing complete - all assets processed" << std::endl;
  
  return 0;
}