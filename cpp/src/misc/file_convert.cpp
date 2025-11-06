#include "misc/file_convert.hpp"
#include "misc/affinity.hpp"
#include "misc/progress_parallel.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <string>
#include <vector>

#ifdef __linux__
#include <sys/statvfs.h>
#include <sys/sysinfo.h>
#endif

namespace FileConvert {

namespace {

// Get available RAM in GB
inline double get_available_ram_gb() {
#ifdef __linux__
  struct sysinfo info;
  if (sysinfo(&info) == 0) {
    // Available RAM = free + buffers + cached
    unsigned long available_kb = info.freeram * info.mem_unit / 1024;
    return available_kb / (1024.0 * 1024.0);
  }
#endif
  return 0.0;
}

// Get available disk space in GB for a given path
inline double get_available_disk_gb(const std::string &path) {
#ifdef __linux__
  struct statvfs stat;
  // Always check the parent directory since the target path might not exist yet
  std::string check_path = std::filesystem::path(path).parent_path().string();

  if (statvfs(check_path.c_str(), &stat) == 0) {
    unsigned long available_bytes = stat.f_bavail * stat.f_frsize;
    return available_bytes / (1024.0 * 1024.0 * 1024.0);
  }
#endif
  return 0.0;
}

// Calculate optimal number of concurrent workers
inline unsigned int calculate_concurrency(double ram_per_task_gb,
                                          double disk_per_task_gb,
                                          const std::string &temp_dir) {
  const double available_ram = get_available_ram_gb();
  const double available_disk = get_available_disk_gb(temp_dir);
  const unsigned int cpu_cores = misc::Affinity::core_count();

  // Calculate limits
  const unsigned int ram_limit = static_cast<unsigned int>(available_ram / ram_per_task_gb);
  const unsigned int disk_limit = static_cast<unsigned int>(available_disk / disk_per_task_gb);

  // For I/O-heavy tasks, we can use more workers than CPU cores
  // since most time is spent waiting for disk I/O
  const unsigned int cpu_limit = std::max(2u, cpu_cores / 2);

  // Take minimum (most restrictive)
  unsigned int concurrency = std::min({ram_limit, disk_limit, cpu_limit});

  return concurrency;
}

// Detect archive format by reading file magic bytes
bool is_7zip_format(const std::string &archive_path) {
  std::ifstream file(archive_path, std::ios::binary);
  if (!file.is_open()) {
    return false;
  }

  char magic[6] = {0};
  file.read(magic, 6);

  // 7z signature: "7z\xbc\xaf\x27\x1c"
  return (magic[0] == '7' && magic[1] == 'z' &&
          magic[2] == (char)0xbc && magic[3] == (char)0xaf &&
          magic[4] == (char)0x27 && magic[5] == (char)0x1c);
}

// Scan all RAR archives and detect which ones are actually 7z format
std::vector<std::string> scan_disguised_7z_archives(const std::string &archive_base_dir,
                                                    const std::string &archive_extension) {
  std::vector<std::string> disguised_files;

  if (!std::filesystem::exists(archive_base_dir)) {
    return disguised_files;
  }

  // Archive structure: archive_base/YYYY/YYYYMM/YYYYMMDD.rar
  for (const auto &year_entry : std::filesystem::directory_iterator(archive_base_dir)) {
    if (!year_entry.is_directory())
      continue;

    for (const auto &month_entry : std::filesystem::directory_iterator(year_entry.path())) {
      if (!month_entry.is_directory())
        continue;

      for (const auto &file_entry : std::filesystem::directory_iterator(month_entry.path())) {
        if (!file_entry.is_regular_file())
          continue;

        const std::string filepath = file_entry.path().string();
        if (filepath.ends_with(archive_extension)) {
          // Check if this .rar file is actually 7z format
          if (is_7zip_format(filepath)) {
            disguised_files.push_back(filepath);
          }
        }
      }
    }
  }

  return disguised_files;
}

// Convert a single 7z file to non-solid RAR format atomically
bool convert_7z_to_rar(const std::string &source_7z_path,
                       const std::string &temp_dir,
                       const std::string &backup_dir,
                       std::string &last_line,
                       std::function<void(const std::string &)> status_callback = nullptr) {
  const std::string filename = std::filesystem::path(source_7z_path).filename().string();
  const std::string stem = std::filesystem::path(source_7z_path).stem().string();

  const std::string temp_extract_dir = temp_dir + "/extract_" + stem;
  const std::string temp_rar_path = temp_dir + "/" + filename;
  const std::string backup_path = backup_dir + "/" + filename + ".7z.backup";

  // Step 1: Extract 7z archive
  std::filesystem::create_directories(temp_extract_dir);
  std::string extract_cmd = "7z x \"" + source_7z_path + "\" -o\"" + temp_extract_dir + "/\" -y 2>&1";
  FILE *pipe = popen(extract_cmd.c_str(), "r");
  assert(pipe);

  char buffer[256];
  while (fgets(buffer, sizeof(buffer), pipe)) {
    last_line = buffer;
    if (!last_line.empty() && last_line.back() == '\n') {
      last_line.pop_back();
    }
    if (status_callback)
      status_callback(last_line);
  }

  [[maybe_unused]] int extract_result = pclose(pipe);
  assert(extract_result == 0);

  // Step 2: Create non-solid RAR archive
  std::string create_cmd = "cd \"" + temp_extract_dir + "\" && rar a -m3 -ma5 -r \"" + temp_rar_path + "\" . 2>&1";
  pipe = popen(create_cmd.c_str(), "r");
  assert(pipe);

  while (fgets(buffer, sizeof(buffer), pipe)) {
    last_line = buffer;
    if (!last_line.empty() && last_line.back() == '\n') {
      last_line.pop_back();
    }
    if (status_callback)
      status_callback(last_line);
  }

  [[maybe_unused]] int create_result = pclose(pipe);
  assert(create_result == 0);

  // Step 3: Atomically replace original file with new RAR
  std::filesystem::create_directories(backup_dir);
  std::filesystem::rename(source_7z_path, backup_path);
  std::filesystem::rename(temp_rar_path, source_7z_path);

  // Step 4: Cleanup
  std::filesystem::remove_all(temp_extract_dir);

  last_line = "Completed";
  return true;
}

// Convert all disguised 7z files with parallel processing
bool convert_all_disguised_7z(const std::vector<std::string> &disguised_files,
                              const std::string &archive_base_dir,
                              unsigned int num_workers) {
  if (disguised_files.empty()) {
    return true;
  }

  // Create conversion directories
  const std::string temp_dir = archive_base_dir + "/temp_7z_conversion";
  const std::string backup_dir = archive_base_dir + "/backup_7z_files";

  std::filesystem::create_directories(temp_dir);
  std::filesystem::create_directories(backup_dir);

  // Clean up residual temp files
  if (std::filesystem::exists(temp_dir)) {
    for (const auto &entry : std::filesystem::directory_iterator(temp_dir)) {
      std::filesystem::remove_all(entry.path());
    }
  }

  // Shared state
  std::atomic<size_t> next_index(0);
  std::atomic<size_t> success_count(0);
  std::atomic<bool> has_failure(false);

  // Progress tracking
  auto progress = std::make_shared<misc::ParallelProgress>(num_workers);
  std::vector<std::future<void>> workers;

  // Worker function
  auto worker_func = [&](misc::ProgressHandle handle, unsigned int worker_id) {
    // Set CPU affinity: use M/N pattern to avoid hyperthreading siblings
    const unsigned int cpu_cores = misc::Affinity::core_count();
    const unsigned int physical_core = (worker_id * cpu_cores) / num_workers;
    misc::Affinity::pin_to_core(physical_core);

    size_t worker_processed = 0;

    while (!has_failure.load()) {
      size_t idx = next_index.fetch_add(1);
      if (idx >= disguised_files.size()) {
        break;
      }

      const auto &filepath = disguised_files[idx];
      const std::string filename = std::filesystem::path(filepath).filename().string();

      ++worker_processed;
      handle.update(worker_processed, disguised_files.size(), filename);

      // Perform conversion
      std::string last_line;
      auto status_callback = [&](const std::string &status) {
        handle.update(worker_processed, disguised_files.size(), filename + ": " + status);
      };

      if (convert_7z_to_rar(filepath, temp_dir, backup_dir, last_line, status_callback)) {
        success_count.fetch_add(1);
        handle.update(worker_processed, disguised_files.size(), filename + " - ✓");
      } else {
        has_failure.store(true);
        handle.update(worker_processed, disguised_files.size(), filename + " - ✗ FAILED");
        break;
      }
    }
  };

  // Launch workers
  for (unsigned int i = 0; i < num_workers; ++i) {
    workers.push_back(std::async(std::launch::async, worker_func, progress->acquire_slot(), i));
  }

  // Wait for completion
  for (auto &worker : workers) {
    worker.wait();
  }
  progress->stop();

  // Summary
  std::cout << "\nConversion summary:\n";
  std::cout << "  Success: " << success_count.load() << "/" << disguised_files.size() << "\n";

  // Cleanup temp directory
  if (std::filesystem::exists(temp_dir)) {
    std::filesystem::remove_all(temp_dir);
  }

  std::cout << "\n";
  return !has_failure.load();
}

} // anonymous namespace

// Check if required commands are available
bool check_required_commands() {
  // Check for 7z
  int result_7z = std::system("which 7z > /dev/null 2>&1");
  if (result_7z != 0) {
    std::cerr << "ERROR: 7z command not found. Please install p7zip-full package.\n";
    std::cerr << "       sudo apt install p7zip-full\n";
    return false;
  }

  // Check for rar
  int result_rar = std::system("which rar > /dev/null 2>&1");
  if (result_rar != 0) {
    std::cerr << "ERROR: rar command not found. Please install rar package.\n";
    std::cerr << "       sudo apt install rar\n";
    return false;
  }

  return true;
}

// Main entry point
bool validate_and_convert_archives(const std::string &archive_base_dir) {
  std::cout << "=== Stage 0: Archive Format Validation ===" << "\n";
  std::cout << "Scanning for .rar files that are actually 7z format...\n";

  const std::string archive_extension = ".rar";
  std::vector<std::string> disguised_7z_files = scan_disguised_7z_archives(archive_base_dir, archive_extension);

  if (!disguised_7z_files.empty()) {
    std::cout << "\n";
    std::cout << "========================================\n";
    std::cout << "WARNING: Found " << disguised_7z_files.size() << " .rar file(s) that are actually 7z format!\n";
    std::cout << "========================================\n";
    std::cout << "\n";

    // Show first 10 files as examples
    const size_t show_count = std::min(size_t(10), disguised_7z_files.size());
    for (size_t i = 0; i < show_count; ++i) {
      std::cout << "  " << disguised_7z_files[i] << "\n";
    }
    if (disguised_7z_files.size() > show_count) {
      std::cout << "  ... and " << (disguised_7z_files.size() - show_count) << " more\n";
    }

    std::cout << "\n";
    std::cout << "These files cannot be processed with unrar.\n";
    std::cout << "They will be converted to non-solid RAR format for partial extraction support.\n";
    std::cout << "\n";

    // Check required commands before proceeding
    std::cout << "Checking required commands...\n";
    if (!check_required_commands()) {
      return false;
    }
    std::cout << "All required commands are available.\n\n";

    // Calculate and display system resources
    const std::string temp_dir = archive_base_dir + "/temp_7z_conversion";
    const double ram_per_task = 10.0;
    const double disk_per_task = 20.0;
    const unsigned int num_workers = calculate_concurrency(ram_per_task, disk_per_task, temp_dir);

    const double available_ram = get_available_ram_gb();
    const double available_disk = get_available_disk_gb(temp_dir);
    const unsigned int cpu_cores = misc::Affinity::core_count();

    const unsigned int ram_limit = static_cast<unsigned int>(available_ram / ram_per_task);
    const unsigned int disk_limit = static_cast<unsigned int>(available_disk / disk_per_task);
    const unsigned int cpu_limit = std::max(2u, cpu_cores / 2);

    std::cout << "System resources:\n";
    std::cout << "  Available RAM: " << available_ram << " GB (supports " << ram_limit << " workers)\n";
    std::cout << "  Available Disk: " << available_disk << " GB (supports " << disk_limit << " workers)\n";
    std::cout << "  CPU cores: " << cpu_cores << " (I/O-heavy: use " << cpu_limit << " workers)\n";
    std::cout << "  --> Concurrent workers: " << num_workers << " (limited by ";

    if (num_workers == ram_limit) {
      std::cout << "RAM";
    } else if (num_workers == disk_limit) {
      std::cout << "Disk";
    } else if (num_workers == cpu_limit) {
      std::cout << "CPU";
    } else {
      std::cout << "unknown";
    }
    std::cout << ")\n";

    std::cout << "  Estimated time: " << (disguised_7z_files.size() / num_workers * 10) << "-"
              << (disguised_7z_files.size() / num_workers * 15) << " minutes\n";
    std::cout << "\n";

    std::cout << "Conversion process:\n";
    std::cout << "  1. Extract 7z archive to temp directory\n";
    std::cout << "  2. Create non-solid RAR archive (supports partial extraction)\n";
    std::cout << "  3. Atomically replace original file\n";
    std::cout << "  4. Move original to backup directory: " << archive_base_dir << "/backup_7z_files/\n";
    std::cout << "\n";
    std::cout << "IMPORTANT:\n";
    std::cout << "  - This process will take a long time for large archives\n";
    std::cout << "  - Please do NOT interrupt the conversion process\n";
    std::cout << "  - Original files will be backed up (can be deleted later to save space)\n";
    std::cout << "\n";
    std::cout << "Type 'yes' to start conversion: ";
    std::cout.flush();

    std::string user_input;
    std::getline(std::cin, user_input);

    if (user_input != "yes") {
      std::cout << "Aborted by user.\n";
      return false;
    }

    std::cout << "\n";
    std::cout << "Starting conversion...\n";
    std::cout << "\n";

    // Perform conversion with progress tracking
    if (!convert_all_disguised_7z(disguised_7z_files, archive_base_dir, num_workers)) {
      std::cerr << "Some conversions failed. Please check errors above.\n";
      return false;
    }

    std::cout << "All files converted successfully!\n";
  } else {
    std::cout << "All .rar files have correct format.\n";
  }
  std::cout << "\n";

  return true;
}

} // namespace FileConvert
