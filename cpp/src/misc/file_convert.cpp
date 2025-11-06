#include "misc/file_convert.hpp"

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace FileConvert {

namespace {

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
// Returns true on success, false on failure
bool convert_7z_to_rar(const std::string &source_7z_path, 
                        const std::string &temp_dir,
                        const std::string &backup_dir) {
  const std::string filename = std::filesystem::path(source_7z_path).filename().string();
  const std::string stem = std::filesystem::path(source_7z_path).stem().string();
  
  // Temp paths for atomic operation
  const std::string temp_extract_dir = temp_dir + "/extract_" + stem;
  const std::string temp_rar_path = temp_dir + "/" + filename;
  const std::string backup_path = backup_dir + "/" + filename + ".7z.backup";
  
  // Step 1: Extract 7z archive to temp directory
  std::filesystem::create_directories(temp_extract_dir);
  std::string extract_cmd = "7z x \"" + source_7z_path + "\" -o\"" + temp_extract_dir + "/\" -y";
  int extract_result = std::system(extract_cmd.c_str());
  if (extract_result != 0) {
    std::filesystem::remove_all(temp_extract_dir);
    return false;
  }
  
  // Step 2: Create non-solid RAR archive in temp location
  // -m3: normal compression, -ma5: RAR 5.0 format
  // -r: recurse subdirectories
  // NOT using -s flag = non-solid archive (supports partial extraction)
  std::string create_cmd = "cd \"" + temp_extract_dir + "\" && rar a -m3 -ma5 -r \"" + temp_rar_path + "\" .";
  int create_result = std::system(create_cmd.c_str());
  if (create_result != 0) {
    std::filesystem::remove_all(temp_extract_dir);
    if (std::filesystem::exists(temp_rar_path)) {
      std::filesystem::remove(temp_rar_path);
    }
    return false;
  }
  
  // Verify RAR file was created
  if (!std::filesystem::exists(temp_rar_path)) {
    std::filesystem::remove_all(temp_extract_dir);
    return false;
  }
  
  // Step 3: Atomically replace original file with new RAR
  // Move original to backup (atomic)
  std::filesystem::create_directories(backup_dir);
  std::filesystem::rename(source_7z_path, backup_path);
  
  // Move new RAR to original location (atomic)
  std::filesystem::rename(temp_rar_path, source_7z_path);
  
  // Step 4: Cleanup temp extraction directory
  std::filesystem::remove_all(temp_extract_dir);
  
  return true;
}

// Convert all disguised 7z files to proper RAR format with progress tracking
bool convert_all_disguised_7z(const std::vector<std::string> &disguised_files,
                               const std::string &archive_base_dir) {
  if (disguised_files.empty()) {
    return true;
  }
  
  // Create conversion directories
  const std::string temp_dir = archive_base_dir + "/temp_7z_conversion";
  const std::string backup_dir = archive_base_dir + "/backup_7z_files";
  
  std::filesystem::create_directories(temp_dir);
  std::filesystem::create_directories(backup_dir);
  
  // Clean up any residual temp files from previous interrupted runs
  if (std::filesystem::exists(temp_dir)) {
    for (const auto &entry : std::filesystem::directory_iterator(temp_dir)) {
      std::filesystem::remove_all(entry.path());
    }
  }
  
  std::cout << "Converting " << disguised_files.size() << " file(s) from 7z to non-solid RAR format...\n";
  std::cout << "Temp directory: " << temp_dir << "\n";
  std::cout << "Backup directory: " << backup_dir << "\n\n";
  
  size_t success_count = 0;
  size_t failed_count = 0;
  
  for (size_t i = 0; i < disguised_files.size(); ++i) {
    const auto &filepath = disguised_files[i];
    const std::string filename = std::filesystem::path(filepath).filename().string();
    
    // Progress indicator
    std::cout << "\n[" << (i + 1) << "/" << disguised_files.size() << "] ";
    std::cout << "Converting " << filename << "...\n";
    
    // Check if file still needs conversion (might have been converted in previous run)
    if (!std::filesystem::exists(filepath)) {
      std::cout << "SKIPPED (already converted or missing)\n";
      success_count++;
      continue;
    }
    
    if (!is_7zip_format(filepath)) {
      std::cout << "SKIPPED (already RAR format)\n";
      success_count++;
      continue;
    }
    
    if (convert_7z_to_rar(filepath, temp_dir, backup_dir)) {
      std::cout << "✓ Conversion successful\n";
      success_count++;
    } else {
      std::cout << "\n✗ Conversion FAILED - Stopping immediately\n";
      failed_count++;
      break;  // Stop on first failure
    }
  }
  
  std::cout << "\nConversion summary:\n";
  std::cout << "  Success: " << success_count << "\n";
  std::cout << "  Failed: " << failed_count << "\n";
  
  if (failed_count > 0) {
    const size_t remaining = disguised_files.size() - success_count - failed_count;
    if (remaining > 0) {
      std::cout << "  Remaining (not processed): " << remaining << "\n";
    }
  }
  
  // Cleanup temp directory if all conversions succeeded
  if (failed_count == 0 && std::filesystem::exists(temp_dir)) {
    std::filesystem::remove_all(temp_dir);
    std::cout << "  Temp directory cleaned up.\n";
  } else if (failed_count > 0) {
    std::cout << "  Temp directory kept: " << temp_dir << "\n";
  }
  
  std::cout << "\n";
  return (failed_count == 0);
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
    if (!convert_all_disguised_7z(disguised_7z_files, archive_base_dir)) {
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

