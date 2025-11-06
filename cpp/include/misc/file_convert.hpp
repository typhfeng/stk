#pragma once

#include <string>

namespace FileConvert {

// Main entry point for Stage 0: Archive format validation and conversion
// Scans for .rar files that are actually 7z format and converts them
// to non-solid RAR format for partial extraction support
// Returns true if all operations succeeded (or no conversion needed)
bool validate_and_convert_archives(const std::string &archive_base_dir);

} // namespace FileConvert

