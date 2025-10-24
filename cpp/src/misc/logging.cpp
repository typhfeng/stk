#include "misc/logging.hpp"
#include <filesystem>
#include <iostream>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <fstream>
#include <mutex>

namespace Logger {

// Internal state
static std::ofstream decomp_log;
static std::ofstream parsing_log;
static std::ofstream analyze_log;
static std::mutex decomp_log_mutex;
static std::mutex parsing_log_mutex;
static std::mutex analyze_log_mutex;
static bool initialized = false;

// Helper function to get current timestamp
static std::string get_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    
    std::stringstream ss;
    ss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
    ss << '.' << std::setfill('0') << std::setw(3) << ms.count();
    return ss.str();
}

void init(const std::string& temp_base_path) {
    if (initialized) {
        return;
    }
    
    // Create log directory
    std::filesystem::path log_dir = std::filesystem::absolute(temp_base_path);
    std::filesystem::create_directories(log_dir);
    
    // Initialize decompression log
    std::filesystem::path decomp_log_path = log_dir / "decompression.log";
    decomp_log.open(decomp_log_path);
    if (decomp_log.is_open()) {
        decomp_log << "[" << get_timestamp() << "] Decompression Log Started at: " << decomp_log_path << std::endl;
        std::cout << "Decompression log: \"" << decomp_log_path << "\"" << std::endl;
    } else {
        std::cerr << "Failed to create decompression log at: " << decomp_log_path << std::endl;
    }
    
    // Initialize parsing error log  
    std::filesystem::path parsing_log_path = log_dir / "encoding.log";
    parsing_log.open(parsing_log_path);
    if (parsing_log.is_open()) {
        parsing_log << "[" << get_timestamp() << "] Parsing Error Log Started at: " << parsing_log_path << std::endl;
        std::cout << "Parsing error log: \"" << parsing_log_path << "\"" << std::endl;
    } else {
        std::cerr << "Failed to create parsing error log at: " << parsing_log_path << std::endl;
    }
    
    // Initialize analysis log
    std::filesystem::path analyze_log_path = log_dir / "analyzing.log";
    analyze_log.open(analyze_log_path);
    if (analyze_log.is_open()) {
        analyze_log << "[" << get_timestamp() << "] Analysis Log Started at: " << analyze_log_path << std::endl;
        std::cout << "Analysis log: \"" << analyze_log_path << "\"" << std::endl;
    } else {
        std::cerr << "Failed to create analysis log at: " << analyze_log_path << std::endl;
    }
    
    initialized = true;
}

void close() {
    if (!initialized) {
        return;
    }
    
    // Close decompression log
    if (decomp_log.is_open()) {
        decomp_log << "[" << get_timestamp() << "] Decompression Log Ended" << std::endl;
        decomp_log.close();
    }
    
    // Close parsing error log
    if (parsing_log.is_open()) {
        parsing_log << "[" << get_timestamp() << "] Parsing Error Log Ended" << std::endl;
        parsing_log.close();
    }
    
    // Close analysis log
    if (analyze_log.is_open()) {
        analyze_log << "[" << get_timestamp() << "] Analysis Log Ended" << std::endl;
        analyze_log.close();
    }
    
    initialized = false;
}

void log_decomp(const std::string& message) {
    if (!initialized) return;
    
    std::lock_guard<std::mutex> lock(decomp_log_mutex);
    if (decomp_log.is_open()) {
        decomp_log << "[" << get_timestamp() << "] " << message << std::endl;
        decomp_log.flush();
    }
}

void log_encode(const std::string& message) {
    if (!initialized) return;
    
    std::lock_guard<std::mutex> lock(parsing_log_mutex);
    if (parsing_log.is_open()) {
        parsing_log << "[" << get_timestamp() << "] " << message << std::endl;
        parsing_log.flush();
    }
}

void log_analyze(const std::string& message) {
    if (!initialized) return;
    
    std::lock_guard<std::mutex> lock(analyze_log_mutex);
    if (analyze_log.is_open()) {
        analyze_log << "[" << get_timestamp() << "] " << message << std::endl;
        analyze_log.flush();
    }
}

bool is_initialized() {
    return initialized;
}

} // namespace Logger
