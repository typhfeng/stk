#include "misc/file_check.hpp"

#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace FileCheck {

namespace {

// ============================================================================
// CHECK 1: REQUIRED COMMANDS
// ============================================================================

bool check_required_commands() {
  struct CommandInfo {
    std::string command;
    std::string install_hint;
  };

  std::vector<CommandInfo> required_commands = {
      {"unrar", "sudo apt install unrar"},
      {"7z", "sudo apt install p7zip-full"},
      {"rar", "sudo apt install rar"}};

  bool ok = true;
  for (const auto &cmd : required_commands) {
    int result = std::system(("which " + cmd.command + " > /dev/null 2>&1").c_str());
    if (result != 0) {
      std::cout << "✗ Missing command: " << cmd.command << "\n";
      std::cout << "  Install: " << cmd.install_hint << "\n";
      ok = false;
    }
  }

  if (ok) {
    std::cout << "✓ All required commands available\n";
  }
  return ok;
}

// ============================================================================
// CHECK 2: ARCHIVE FORMAT (7Z AND SOLID RAR)
// ============================================================================

bool detect_7z_format(const std::string &archive_path) {
  std::ifstream file(archive_path, std::ios::binary);
  if (!file.is_open()) {
    return false;
  }

  char magic[6] = {0};
  file.read(magic, 6);

  return (magic[0] == '7' && magic[1] == 'z' &&
          magic[2] == (char)0xbc && magic[3] == (char)0xaf &&
          magic[4] == (char)0x27 && magic[5] == (char)0x1c);
}

bool detect_solid_rar(const std::string &archive_path) {
  std::ifstream file(archive_path, std::ios::binary);
  if (!file.is_open()) {
    return false;
  }

  // RAR 5.x signature: "Rar!\x1a\x07\x01\x00"
  // RAR 4.x signature: "Rar!\x1a\x07\x00"
  char header[8] = {0};
  file.read(header, 8);

  // Check RAR signature
  if (header[0] != 'R' || header[1] != 'a' || header[2] != 'r' || header[3] != '!') {
    return false;
  }

  // RAR 5.x format
  if (header[4] == 0x1a && header[5] == 0x07 && header[6] == 0x01 && header[7] == 0x00) {
    // Read archive header to check solid flag
    // In RAR 5.x, need to parse vint and flags
    // Skip for now - RAR 5.x is less common and checking is complex
    return false;
  }

  // RAR 4.x format
  if (header[4] == 0x1a && header[5] == 0x07 && header[6] == 0x00) {
    // Skip to main header block (after marker block)
    file.seekg(7, std::ios::beg);
    
    // Read block header: HEAD_CRC(2) + HEAD_TYPE(1) + HEAD_FLAGS(2) + HEAD_SIZE(2)
    char block[7];
    file.read(block, 7);
    
    unsigned char head_type = block[2];
    unsigned short head_flags = (unsigned char)block[3] | ((unsigned char)block[4] << 8);
    
    // Archive header has type 0x73
    if (head_type == 0x73) {
      // Solid flag is bit 3 (0x0008) in archive header flags
      return (head_flags & 0x0008) != 0;
    }
  }

  return false;
}

bool check_archive_format(const std::vector<std::string> &archives) {
  std::vector<std::string> disguised_7z;
  std::vector<std::string> solid_rar;
  
  for (const auto &archive : archives) {
    if (detect_7z_format(archive)) {
      disguised_7z.push_back(archive);
    } else if (detect_solid_rar(archive)) {
      solid_rar.push_back(archive);
    }
  }

  bool ok = true;

  // Report disguised 7z files
  if (!disguised_7z.empty()) {
    std::cout << "✗ Found " << disguised_7z.size() << " .rar file(s) that are actually 7z format\n";
    for (const auto &file : disguised_7z) {
      std::cout << "  " << file << "\n";
    }
    std::cout << "\n";
    std::cout << "  Fix: Run py/app/FileRepair/fix_7z_to_rar.py\n";
    std::cout << "\n";
    ok = false;
  }

  // Report solid RAR files
  if (!solid_rar.empty()) {
    std::cout << "✗ Found " << solid_rar.size() << " solid RAR file(s)\n";
    for (const auto &file : solid_rar) {
      std::cout << "  " << file << "\n";
    }
    std::cout << "\n";
    std::cout << "  Solid archives don't support partial extraction\n";
    std::cout << "  Fix: Run py/app/FileRepair/fix_solid_to_nonsolid.py\n";
    std::cout << "\n";
    ok = false;
  }

  if (ok) {
    std::cout << "✓ All archives have correct format (RAR non-solid)\n";
  }

  return ok;
}

// ============================================================================
// CHECK 3: INCOMPATIBLE ARCHIVE STRUCTURE
// ============================================================================

std::string detect_incompatible_structure(const std::string &archive_path) {
  std::string cmd = "unrar lb \"" + archive_path + "\" 2>/dev/null | head -1";
  FILE *pipe = popen(cmd.c_str(), "r");
  if (!pipe) {
    return "";
  }

  char buffer[512];
  std::string first_entry;
  if (fgets(buffer, sizeof(buffer), pipe)) {
    first_entry = buffer;
    if (!first_entry.empty() && first_entry.back() == '\n') {
      first_entry.pop_back();
    }
  }
  pclose(pipe);

  if (first_entry.empty()) {
    return "";
  }

  // Expected: YYYYMMDD/asset_code/*.csv (e.g., 20240925/000001.SZ/行情.csv)
  if (first_entry.length() >= 9 && first_entry[8] == '/' &&
      std::isdigit(first_entry[0]) && std::isdigit(first_entry[1]) &&
      std::isdigit(first_entry[2]) && std::isdigit(first_entry[3]) &&
      std::isdigit(first_entry[4]) && std::isdigit(first_entry[5]) &&
      std::isdigit(first_entry[6]) && std::isdigit(first_entry[7])) {
    return "";
  }

  return first_entry;
}

bool check_incompatible_structure(const std::vector<std::string> &archives) {
  std::vector<std::pair<std::string, std::string>> problematic_files;
  for (const auto &archive : archives) {
    std::string detected_path = detect_incompatible_structure(archive);
    if (!detected_path.empty()) {
      problematic_files.push_back({archive, detected_path});
    }
  }

  if (problematic_files.empty()) {
    std::cout << "✓ All archives have correct internal hierarchy\n";
    return true;
  }

  std::cout << "✗ Found " << problematic_files.size() << " archive(s) with incorrect structure\n";
  for (const auto &[archive, detected] : problematic_files) {
    std::cout << "  " << archive << "\n";
    std::cout << "    Detected: " << detected << "\n";
  }
  std::cout << "\n";
  std::cout << "  Expected: YYYYMMDD/asset_code/*.csv\n";
  std::cout << "  Example: 20240925/000001.SZ/行情.csv\n";
  std::cout << "\n";
  std::cout << "  Fix: Run py/app/FileRepair/fix_archive_structure.py\n";
  return false;
}

// ============================================================================
// CHECK 0: ARCHIVE PATH AND NAMING
// ============================================================================

bool is_valid_number_string(const std::string &str, size_t expected_len) {
  if (str.length() != expected_len)
    return false;
  for (char c : str) {
    if (!std::isdigit(c))
      return false;
  }
  return true;
}

bool check_archive_naming(const std::string &archive_base_dir) {
  if (!std::filesystem::exists(archive_base_dir)) {
    std::cout << "✗ Archive base directory does not exist: " << archive_base_dir << "\n";
    return false;
  }

  std::vector<std::string> problems;
  int valid_count = 0;

  // Expected structure: archive_base/YYYY/YYYYMM/YYYYMMDD.rar
  for (const auto &year_entry : std::filesystem::directory_iterator(archive_base_dir)) {
    if (!year_entry.is_directory()) {
      problems.push_back("Non-directory in base: " + year_entry.path().filename().string());
      continue;
    }

    std::string year_name = year_entry.path().filename().string();
    if (!is_valid_number_string(year_name, 4)) {
      problems.push_back("Invalid year directory: " + year_name + " (expected YYYY)");
      continue;
    }

    for (const auto &month_entry : std::filesystem::directory_iterator(year_entry.path())) {
      if (!month_entry.is_directory()) {
        problems.push_back("Non-directory in " + year_name + ": " + month_entry.path().filename().string());
        continue;
      }

      std::string month_name = month_entry.path().filename().string();
      if (!is_valid_number_string(month_name, 6)) {
        problems.push_back("Invalid month directory: " + year_name + "/" + month_name + " (expected YYYYMM)");
        continue;
      }

      // Check month directory name matches year
      if (month_name.substr(0, 4) != year_name) {
        problems.push_back("Month directory mismatch: " + year_name + "/" + month_name);
        continue;
      }

      for (const auto &file_entry : std::filesystem::directory_iterator(month_entry.path())) {
        if (!file_entry.is_regular_file()) {
          problems.push_back("Non-file in " + year_name + "/" + month_name + ": " + file_entry.path().filename().string());
          continue;
        }

        std::string filename = file_entry.path().filename().string();
        
        // Check file extension
        if (!filename.ends_with(".rar")) {
          std::string ext = filename.substr(filename.find_last_of('.'));
          problems.push_back("Invalid file extension: " + year_name + "/" + month_name + "/" + filename + " (found " + ext + ", expected .rar)");
          continue;
        }

        // Check filename format (YYYYMMDD.rar)
        std::string stem = filename.substr(0, filename.length() - 4);
        if (!is_valid_number_string(stem, 8)) {
          problems.push_back("Invalid filename: " + year_name + "/" + month_name + "/" + filename + " (expected YYYYMMDD.rar)");
          continue;
        }

        // Check filename matches parent directories
        if (stem.substr(0, 4) != year_name || stem.substr(0, 6) != month_name) {
          problems.push_back("Filename path mismatch: " + year_name + "/" + month_name + "/" + filename);
          continue;
        }

        valid_count++;
      }
    }
  }

  if (problems.empty()) {
    std::cout << "✓ All " << valid_count << " archive paths and names are correct\n";
    return true;
  }

  std::cout << "✗ Found " << problems.size() << " naming/path problem(s)\n";
  
  // Count .zip files
  int zip_count = 0;
  for (const auto &problem : problems) {
    std::cout << "  " << problem << "\n";
    if (problem.find(".zip") != std::string::npos) {
      zip_count++;
    }
  }
  
  std::cout << "\n";
  std::cout << "  Expected structure: YYYY/YYYYMM/YYYYMMDD.rar\n";
  std::cout << "  Example: 2024/202411/20241119.rar\n";
  
  if (zip_count > 0) {
    std::cout << "\n";
    std::cout << "  Fix .zip files: Run py/app/FileRepair/fix_zip_to_rar.py\n";
  }
  
  return false;
}

std::vector<std::string> scan_all_archives(const std::string &archive_base_dir) {
  std::vector<std::string> all_archives;

  if (!std::filesystem::exists(archive_base_dir)) {
    return all_archives;
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
        if (filepath.ends_with(".rar")) {
          all_archives.push_back(filepath);
        }
      }
    }
  }

  return all_archives;
}

} // anonymous namespace

// ============================================================================
// PUBLIC API
// ============================================================================

bool check_src_archives(const std::string &archive_base_dir) {
  std::cout << "=== Archive Validation (comment to skip) ===" << "\n\n";

  bool ok = true;

  // Check 1: Required commands
  bool cmd_ok = check_required_commands();
  ok = cmd_ok && ok;
  std::cout << "\n";

  // Only continue if commands are available
  if (!cmd_ok) {
    return false;
  }

  // Check 2: Archive naming and paths
  bool naming_ok = check_archive_naming(archive_base_dir);
  ok = naming_ok && ok;
  std::cout << "\n";

  // Continue checking even if naming has issues
  std::vector<std::string> all_archives = scan_all_archives(archive_base_dir);

  // Check 3: Archive format (7z and solid RAR)
  bool format_ok = check_archive_format(all_archives);
  ok = format_ok && ok;
  std::cout << "\n";

  // Check 4: Archive internal structure
  bool structure_ok = check_incompatible_structure(all_archives);
  ok = structure_ok && ok;
  std::cout << "\n";

  // Summary
  if (ok) {
    std::cout << "========================================\n";
    std::cout << "✓ All checks passed\n";
    std::cout << "========================================\n";
  } else {
    std::cout << "========================================\n";
    std::cout << "✗ Some checks failed. Please fix the issues above.\n";
    std::cout << "========================================\n";
  }

  return ok;
}

} // namespace FileCheck
