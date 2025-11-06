#include "codec/L2_DataType.hpp"
#include "codec/binary_decoder_L2.hpp"
#include "codec/binary_encoder_L2.hpp"
#include "codec/json_config.hpp"
#include "features/backend/FeatureStore.hpp"
#include "lob/LimitOrderBook.hpp"
#include "misc/affinity.hpp"
#include "misc/logging.hpp"
#include "misc/progress_parallel.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <future>
#include <iostream>
#include <mutex>
#include <random>
#include <regex>
#include <set>
#include <string>
#include <vector>

// ============================================================================
// L2数据处理架构 - 两阶段并行处理
// ============================================================================
//
// 【架构概述】
//   Phase 1: Encoding（编码阶段）
//     - 多线程并行处理，每个线程负责一个完整的asset
//     - 单个asset内的日期乱序处理，通过RAR锁协调
//     - RAR锁采用阻塞模式：撞锁时等待，而非跳过
//     - 完成一个asset后再领取下一个
//
//   Phase 2: Analysis（分析阶段）
//     - 等待所有encoding完成后启动
//     - 多asset并行，每个asset内严格按日期顺序处理
//     - 无需锁机制，纯顺序读取binary文件
//
// 【数据流程】
//
//   RAR压缩包 → CSV文件（GBK编码）→ CSV结构体
//       ↓
//   L2标准结构 → 编码+压缩 → Binary文件
//       ↓
//   解压+解码 → L2结构体 → 高频分析 → 结果输出
//
// 【Phase 1: Encoding详细流程】
//
//   1. 配置加载
//      - config.json: 回测时间区间
//      - stock_info.json: 资产列表及上市/退市日期
//
//   2. 任务分配
//      - 按asset分配任务，每个任务包含该asset的所有交易日
//      - Worker从队列领取asset，处理完再领取下一个
//      - 同一时刻，一个Worker只处理一个asset
//
//   3. 单Asset处理（日期乱序）
//      - Shuffle日期列表，分散RAR访问压力
//      - 对每个日期：
//        a) 获取RAR锁（阻塞等待，确保同一RAR不被并发解压）
//        b) 解压: /path/YYYYMMDD.rar → YYYYMMDD/ASSET_CODE/*.csv
//        c) 解析CSV: 行情.csv + 逐笔委托.csv + 逐笔成交.csv
//        d) 编码: CSV → L2结构 → Zstd压缩 → .bin文件
//        e) 释放RAR锁
//
//   4. 数据格式处理
//      - SZSE代码: 2025+年用"1"，早期用"000001"
//      - 订单类型: SZSE用0/1/U，SSE用A/D
//      - 价格单位: 0.01元（CSV为0.0001元，除以100）
//      - 数量单位: 100股（CSV为股数，除以100）
//
//   5. 进度显示
//      - 每个Worker固定一行显示
//      - 显示: [进度条] 百分比 (当前日期/总日期) Asset代码 - 日期
//      - Asset代码固定不变，日期随处理进度更新
//
// 【Phase 2: Analysis详细流程】
//
//   1. 等待Encoding完成
//      - 确保所有binary文件已生成
//
//   2. 并行分析
//      - 每个Worker领取一个asset
//      - 严格按日期顺序读取binary文件
//      - 重建order book，生成分析信号
//
//   3. 订单簿重建
//      - 处理maker/taker/cancel订单
//      - 维护10档买卖队列
//      - 生成策略信号
//
// 【线程安全】
//   - Encoding阶段：
//     * 每个Worker独立处理一个asset，无状态共享
//     * RAR锁确保同一压缩包不被并发解压
//     * CPU亲和性绑定避免线程迁移
//
//   - Analysis阶段：
//     * 只读binary文件，无并发写入
//     * 无需锁机制
//
// 【性能优化】
//   - Binary缓存: 跳过已存在的binary文件
//   - 日期乱序: 分散RAR访问，提高并发度
//   - CPU亲和性: 减少cache miss
//   - Zstd压缩: 解压速度1300+ MB/s
//   - 内存预分配: 基于历史数据量
//
// ============================================================================
// CONFIGURATION SECTION
// ============================================================================
// Modify these constants to adapt to different environments or data suppliers
namespace Config {
// Archive settings - modify these for different compression formats
constexpr const char *ARCHIVE_EXTENSION = ".rar"; // Archive file extension (.rar/.7z/.zip)
constexpr const char *ARCHIVE_TOOL = "unrar";     // Archive extraction tool (unrar/7z/unzip)
constexpr const char *ARCHIVE_EXTRACT_CMD = "x";  // Extract command (x for unrar, x for 7z)

// File extensions and names - standard CSV filenames from data supplier
constexpr const char *BIN_EXTENSION = ".bin";
// CSV filenames defined here for documentation and potential future use
// Currently the encoder auto-detects these files, but explicit names reserved for future API changes
[[maybe_unused]] constexpr const char *CSV_MARKET_DATA = "行情.csv";    // Market snapshot CSV filename
[[maybe_unused]] constexpr const char *CSV_TICK_TRADE = "逐笔成交.csv"; // Tick-by-tick trade CSV filename
[[maybe_unused]] constexpr const char *CSV_TICK_ORDER = "逐笔委托.csv"; // Tick-by-tick order CSV filename

// Path settings - modify these for your environment
constexpr const char *DEFAULT_CONFIG_FILE = "../../../../config/config.json";
constexpr const char *DEFAULT_STOCK_INFO_FILE = "../../../../config/daily_holding/stock_info_test.json";
constexpr const char *DEFAULT_L2_ARCHIVE_BASE = "/mnt/dev/sde/A_stock/L2";
constexpr const char *DEFAULT_TEMP_DIR = "../../../../output/database";

// Processing settings - modify for different behaviors
constexpr bool CLEANUP_AFTER_PROCESSING = false; // Clean up temp files after processing (saves disk space)
constexpr bool SKIP_EXISTING_BINARIES = true;    // Skip extraction/encoding if binary files exist (faster rerun)

} // namespace Config

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

namespace Utils {
inline std::string generate_archive_path(const std::string &base_dir, const std::string &date_str) {
  return base_dir + "/" + date_str.substr(0, 4) + "/" + date_str.substr(0, 6) + "/" + date_str + Config::ARCHIVE_EXTENSION;
}

inline std::string generate_temp_asset_dir(const std::string &temp_dir, const std::string &date_str, const std::string &asset_code) {
  return temp_dir + "/" + date_str.substr(0, 4) + "/" + date_str.substr(4, 2) + "/" + date_str.substr(6, 2) + "/" + asset_code;
}

inline bool is_leap_year(int year) {
  return (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
}

inline int get_days_in_month(int year, int month) {
  if (month == 2) {
    return is_leap_year(year) ? 29 : 28;
  }
  if (month == 4 || month == 6 || month == 9 || month == 11) {
    return 30;
  }
  return 31;
}

inline std::vector<std::string> generate_date_range(const std::string &start_date, const std::string &end_date, const std::string &l2_archive_base) {
  std::vector<std::string> dates;

  std::regex date_regex(R"((\d{4})_(\d{2})_(\d{2}))");
  std::smatch start_match, end_match;

  if (!std::regex_match(start_date, start_match, date_regex) || !std::regex_match(end_date, end_match, date_regex)) {
    return dates;
  }

  const int start_year = std::stoi(start_match[1]);
  const int start_mon = std::stoi(start_match[2]);
  const int start_day = std::stoi(start_match[3]);
  const int end_year = std::stoi(end_match[1]);
  const int end_mon = std::stoi(end_match[2]);
  const int end_day = std::stoi(end_match[3]);

  const bool has_archive_base = std::filesystem::exists(l2_archive_base);

  for (int year = start_year; year <= end_year; ++year) {
    const int mon_start = (year == start_year) ? start_mon : 1;
    const int mon_end = (year == end_year) ? end_mon : 12;

    for (int month = mon_start; month <= mon_end; ++month) {
      const int day_start = (year == start_year && month == start_mon) ? start_day : 1;
      const int day_end = (year == end_year && month == end_mon) ? end_day : get_days_in_month(year, month);

      for (int day = day_start; day <= day_end; ++day) {
        char date_buf[9];
        snprintf(date_buf, sizeof(date_buf), "%04d%02d%02d", year, month, day);
        const std::string date_str(date_buf);

        // If archive base exists, only include dates with existing archives
        // If not, include all dates (will be filtered in encoding/analysis phase)
        if (!has_archive_base || std::filesystem::exists(generate_archive_path(l2_archive_base, date_str))) {
          dates.push_back(date_str);
        }
      }
    }
  }

  return dates;
}
} // namespace Utils

// ============================================================================
// RAR LOCK MANAGER
// ============================================================================
// Manages per-archive locks to prevent concurrent extraction from same RAR

class RarLockManager {
  inline static std::mutex map_mutex;
  inline static std::unordered_map<std::string, std::unique_ptr<std::mutex>> locks;

public:
  static std::mutex *get_or_create_lock(const std::string &archive_path) {
    std::lock_guard<std::mutex> lock(map_mutex);
    if (locks.find(archive_path) == locks.end()) {
      locks[archive_path] = std::make_unique<std::mutex>();
    }
    return locks[archive_path].get();
  }
};

// ============================================================================
// TASK AND DATA STRUCTURES
// ============================================================================

struct AssetTask {
  size_t asset_id;
  std::string asset_code;
  std::string asset_name;
  std::vector<std::string> dates;
};

struct BinaryFiles {
  std::string snapshots_file;
  std::string orders_file;
  bool exists() const { return !snapshots_file.empty() || !orders_file.empty(); }
};

// ============================================================================
// ENCODING FUNCTIONS
// ============================================================================

namespace Encoding {
inline BinaryFiles check_existing_binaries(const std::string &temp_asset_dir, const std::string &asset_code) {
  if (!std::filesystem::exists(temp_asset_dir)) {
    return {"", ""};
  }

  BinaryFiles result;
  for (const auto &entry : std::filesystem::directory_iterator(temp_asset_dir)) {
    const std::string filename = entry.path().filename().string();
    if (filename.starts_with(asset_code + "_snapshots_") && filename.ends_with(Config::BIN_EXTENSION)) {
      result.snapshots_file = entry.path().string();
    } else if (filename.starts_with(asset_code + "_orders_") && filename.ends_with(Config::BIN_EXTENSION)) {
      result.orders_file = entry.path().string();
    }
  }
  return result;
}

inline bool extract_and_encode(const std::string &archive_path, const std::string &asset_code, const std::string &date_str, const std::string &temp_dir, L2::BinaryEncoder_L2 &encoder) {
  // Acquire lock (blocking)
  std::mutex *archive_lock = RarLockManager::get_or_create_lock(archive_path);
  std::lock_guard<std::mutex> lock(*archive_lock);

  // Extract from archive
  const std::string temp_extract_dir = temp_dir + "/tmp_" + asset_code;
  std::filesystem::create_directories(temp_extract_dir);

  const std::string archive_name = std::filesystem::path(archive_path).stem().string();
  const std::string asset_path_in_archive = archive_name + "/" + asset_code + "/*";
  const std::string command = std::string(Config::ARCHIVE_TOOL) + " " +
                              std::string(Config::ARCHIVE_EXTRACT_CMD) + " \"" +
                              archive_path + "\" \"" + asset_path_in_archive + "\" \"" +
                              temp_extract_dir + "/\" -y > /dev/null 2>&1";

  if (std::system(command.c_str()) != 0) {
    std::filesystem::remove_all(temp_extract_dir);
    return false;
  }

  const std::string extracted_dir = temp_extract_dir + "/" + date_str + "/" + asset_code;
  if (!std::filesystem::exists(extracted_dir)) {
    std::filesystem::remove_all(temp_extract_dir);
    return false;
  }

  // Move to final location
  const std::string temp_asset_dir = Utils::generate_temp_asset_dir(temp_dir, date_str, asset_code);
  std::filesystem::create_directories(std::filesystem::path(temp_asset_dir).parent_path());

  // Remove target if exists to avoid rename failure
  if (std::filesystem::exists(temp_asset_dir)) {
    std::filesystem::remove_all(temp_asset_dir);
  }

  std::filesystem::rename(extracted_dir, temp_asset_dir);
  std::filesystem::remove_all(temp_extract_dir);

  // Parse and encode
  std::vector<L2::Snapshot> snapshots;
  std::vector<L2::Order> orders;
  if (!encoder.process_stock_data(temp_asset_dir, temp_asset_dir, asset_code, &snapshots, &orders)) {
    return false;
  }

  // Clean up CSV files
  for (const auto &entry : std::filesystem::directory_iterator(temp_asset_dir)) {
    if (entry.path().string().ends_with(".csv")) {
      std::filesystem::remove(entry.path());
    }
  }

  return true;
}
} // namespace Encoding

// ============================================================================
// ANALYSIS FUNCTIONS
// ============================================================================

namespace Analysis {
inline size_t process_binary_files(const BinaryFiles &files,
                                   L2::BinaryDecoder_L2 &decoder,
                                   LimitOrderBook &lob) {
  size_t order_count = 0;
  if (!files.orders_file.empty()) {
    std::vector<L2::Order> decoded_orders;
    if (!decoder.decode_orders(files.orders_file, decoded_orders)) {
      return 0;
    }

    order_count = decoded_orders.size();
    for (const auto &ord : decoded_orders) {
      lob.process(ord);
    }
    lob.clear();
  }
  return order_count;
}
} // namespace Analysis

// ============================================================================
// PHASE 1: ENCODING WORKER
// ============================================================================

void encoding_worker(std::vector<AssetTask> &asset_queue, std::mutex &queue_mutex, std::atomic<size_t> &completed_assets, const std::string &l2_archive_base, const std::string &temp_dir, unsigned int core_id, misc::ProgressHandle progress_handle) {
  static thread_local bool affinity_set = false;
  if (!affinity_set && misc::Affinity::supported()) {
    affinity_set = misc::Affinity::pin_to_core(core_id);
  }

  L2::BinaryEncoder_L2 encoder(L2::DEFAULT_ENCODER_SNAPSHOT_SIZE, L2::DEFAULT_ENCODER_ORDER_SIZE);

  while (true) {
    AssetTask asset_task;

    // Get an asset
    {
      std::lock_guard<std::mutex> lock(queue_mutex);
      if (asset_queue.empty()) {
        break; // No more assets
      }
      asset_task = asset_queue.back();
      asset_queue.pop_back();
    }

    // Set label to this asset (fixed for this asset's lifetime)
    progress_handle.set_label(asset_task.asset_code + " (" + asset_task.asset_name + ")");

    // Shuffle dates for this asset to spread RAR access
    std::vector<std::string> dates = asset_task.dates;
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(dates.begin(), dates.end(), g);

    // Process all dates for this asset
    for (size_t i = 0; i < dates.size(); ++i) {
      const std::string &date_str = dates[i];
      const std::string temp_asset_dir = Utils::generate_temp_asset_dir(temp_dir, date_str, asset_task.asset_code);
      const BinaryFiles existing = Encoding::check_existing_binaries(temp_asset_dir, asset_task.asset_code);

      // Skip if binary already exists
      if (existing.exists() && Config::SKIP_EXISTING_BINARIES) {
        progress_handle.update(i + 1, dates.size(), date_str);
        continue;
      }

      // Check archive exists before attempting extraction
      const std::string archive_path = Utils::generate_archive_path(l2_archive_base, date_str);
      if (!std::filesystem::exists(archive_path)) {
        progress_handle.update(i + 1, dates.size(), date_str);
        continue;
      }

      // Encode (blocking on RAR lock if needed)
      bool success = Encoding::extract_and_encode(archive_path, asset_task.asset_code, date_str, temp_dir, encoder);

      // Always update progress regardless of success/failure
      progress_handle.update(i + 1, dates.size(), date_str);

      if (success && Config::CLEANUP_AFTER_PROCESSING) {
        std::filesystem::remove_all(temp_asset_dir);
      }
    }

    // Increment completed assets counter after finishing all dates for this asset
    ++completed_assets;
  }
}

// ============================================================================
// PHASE 2: ANALYSIS PROCESSOR (DATE-FIRST TRAVERSAL)
// ============================================================================

// Worker context: manages multiple assets with date-first processing
struct AnalysisWorkerContext {
  std::vector<size_t> asset_ids;
  std::vector<std::string> asset_codes;
  std::vector<std::string> asset_names;
  std::vector<std::unique_ptr<LimitOrderBook>> lobs;
  std::vector<std::unique_ptr<L2::BinaryDecoder_L2>> decoders;
  
  AnalysisWorkerContext(const std::vector<AssetTask> &assigned_assets, GlobalFeatureStore *feature_store) {
    for (const auto &asset : assigned_assets) {
      asset_ids.push_back(asset.asset_id);
      asset_codes.push_back(asset.asset_code);
      asset_names.push_back(asset.asset_name);
      
      L2::ExchangeType exchange_type = L2::infer_exchange_type(asset.asset_code);
      lobs.push_back(std::make_unique<LimitOrderBook>(
          L2::DEFAULT_ENCODER_ORDER_SIZE, exchange_type, feature_store, asset.asset_id));
      decoders.push_back(std::make_unique<L2::BinaryDecoder_L2>(
          L2::DEFAULT_ENCODER_SNAPSHOT_SIZE, L2::DEFAULT_ENCODER_ORDER_SIZE));
    }
  }
};

// Process one date for all assets in this worker's batch
size_t process_date_for_asset_batch(AnalysisWorkerContext &ctx,
                                     const std::string &date_str,
                                     const std::string &temp_dir,
                                     GlobalFeatureStore *feature_store,
                                     const std::vector<std::set<std::string>> &asset_date_sets) {
  
  size_t total_orders = 0;
  
  for (size_t i = 0; i < ctx.asset_ids.size(); ++i) {
    // Skip if this asset doesn't have data for this date
    if (asset_date_sets[i].find(date_str) == asset_date_sets[i].end()) {
      continue;
    }
    
    const size_t asset_id = ctx.asset_ids[i];
    const std::string &asset_code = ctx.asset_codes[i];
    
    // Set current date for this asset
    feature_store->set_current_date(asset_id, date_str);
    
    // Load and process binary files
    const std::string temp_asset_dir = Utils::generate_temp_asset_dir(temp_dir, date_str, asset_code);
    const BinaryFiles files = Encoding::check_existing_binaries(temp_asset_dir, asset_code);
    
    if (files.exists()) {
      total_orders += Analysis::process_binary_files(files, *ctx.decoders[i], *ctx.lobs[i]);
    }
  }
  
  // Cross-sectional feature computation point (all assets at same date)
  // TODO: Insert cross-sectional factor calculation here
  
  return total_orders;
}

// Worker function: processes assigned assets with date-first order
void analysis_worker_date_first(const std::vector<AssetTask> &assigned_assets,
                                 const std::vector<std::string> &all_dates,
                                 const std::string &temp_dir,
                                 GlobalFeatureStore *feature_store,
                                 misc::ProgressHandle progress_handle) {
  
  // Initialize worker context
  AnalysisWorkerContext ctx(assigned_assets, feature_store);
  
  // Precompute date sets for each asset (for fast lookup)
  std::vector<std::set<std::string>> asset_date_sets;
  for (const auto &asset : assigned_assets) {
    asset_date_sets.emplace_back(asset.dates.begin(), asset.dates.end());
  }
  
  // Build progress label: "  N Assets: CODE1(NAME1)"
  char label_buf[128];
  snprintf(label_buf, sizeof(label_buf), "%3zu Assets: %s(%s)", 
           assigned_assets.size(),
           assigned_assets[0].asset_code.c_str(),
           assigned_assets[0].asset_name.c_str());
  progress_handle.set_label(label_buf);
  
  size_t cumulative_orders = 0;
  auto start_time = std::chrono::steady_clock::now();
  
  // Date-first traversal
  for (size_t date_idx = 0; date_idx < all_dates.size(); ++date_idx) {
    const std::string &date_str = all_dates[date_idx];
    
    // Process this date for all assigned assets
    size_t date_orders = process_date_for_asset_batch(ctx, date_str, temp_dir, feature_store, asset_date_sets);
    cumulative_orders += date_orders;
    
    // Update progress
    auto current_time = std::chrono::steady_clock::now();
    double elapsed_seconds = std::chrono::duration<double>(current_time - start_time).count();
    double speed_M_per_sec = (elapsed_seconds > 0) ? (cumulative_orders / 1e6) / elapsed_seconds : 0.0;
    double avg_orders_per_asset_M = assigned_assets.size() > 0 ? (static_cast<double>(cumulative_orders) / assigned_assets.size()) / 1e6 : 0.0;
    
    char msg_buf[128];
    snprintf(msg_buf, sizeof(msg_buf), "%s [%.1fM/s (%.1fM)]", date_str.c_str(), speed_M_per_sec, avg_orders_per_asset_M);
    progress_handle.update(date_idx + 1, all_dates.size(), msg_buf);
  }
}

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

int main() {
  try {
    std::cout << "=== L2 Data Processor (CSV Mode) ===" << "\n";

    // Load configuration
    const std::string config_file = Config::DEFAULT_CONFIG_FILE;
    const std::string stock_info_file = Config::DEFAULT_STOCK_INFO_FILE;
    const std::string l2_archive_base = Config::DEFAULT_L2_ARCHIVE_BASE;
    const std::string temp_dir = Config::DEFAULT_TEMP_DIR;

    const JsonConfig::AppConfig app_config = JsonConfig::ParseAppConfig(config_file);
    auto stock_info_map = JsonConfig::ParseStockInfo(stock_info_file);

    // Adjust stock dates based on config
    const std::chrono::year_month config_start{app_config.start_date.year(), app_config.start_date.month()};
    const std::chrono::year_month config_end{app_config.end_date.year(), app_config.end_date.month()};
    for (auto &[code, info] : stock_info_map) {
      if (info.start_date < config_start) {
        info.start_date = config_start;
      }
      if (!info.is_delisted) {
        info.end_date = config_end;
      }
    }

    // Print summary
    std::cout << "Configuration:" << "\n";
    std::cout << "  Archive: " << Config::ARCHIVE_TOOL << " (" << Config::ARCHIVE_EXTENSION << ")\n";
    std::cout << "  L2 base: " << l2_archive_base << "\n";
    std::cout << "  Temp dir: " << temp_dir << "\n";
    std::cout << "  Period: " << JsonConfig::FormatYearMonthDay(app_config.start_date)
              << " → " << JsonConfig::FormatYearMonthDay(app_config.end_date) << "\n";
    std::cout << "  Assets: " << stock_info_map.size() << "\n";
    std::cout << "  Skip existing: " << (Config::SKIP_EXISTING_BINARIES ? "Yes" : "No") << "\n";
    std::cout << "  Auto cleanup: " << (Config::CLEANUP_AFTER_PROCESSING ? "Yes" : "No") << "\n\n";

    std::filesystem::create_directories(temp_dir);
    Logger::init(temp_dir);

    const unsigned int num_threads = misc::Affinity::core_count();
    const unsigned int num_workers = std::min(num_threads, static_cast<unsigned int>(stock_info_map.size()));

    std::cout << "Threads: " << num_threads;
    if (misc::Affinity::supported()) {
      std::cout << " (CPU affinity enabled)";
    }
    std::cout << "\n";
    std::cout << "Workers: " << num_workers << " (processing " << stock_info_map.size() << " assets)\n\n";

    // ========================================================================
    // PHASE 1: ENCODING (can be out-of-order, uses RAR locks)
    // ========================================================================
    std::cout << "=== Phase 1: Encoding ===" << "\n";

    // Build asset queue and count total tasks
    std::vector<AssetTask> asset_queue;

    size_t asset_id = 0;
    for (const auto &[asset_code, stock_info] : stock_info_map) {
      const auto effective_start = std::max(std::chrono::year_month_day{stock_info.start_date / std::chrono::day{1}}, app_config.start_date);
      const auto effective_end = std::min(std::chrono::year_month_day{stock_info.end_date / std::chrono::last}, app_config.end_date);

      const auto dates = Utils::generate_date_range(JsonConfig::FormatYearMonthDay(effective_start), JsonConfig::FormatYearMonthDay(effective_end), l2_archive_base);

      if (!dates.empty()) {
        asset_queue.push_back({asset_id++, asset_code, stock_info.name, dates});
      }
    }

    // Save a copy for Phase 2 to reuse
    std::vector<AssetTask> asset_queue_for_analysis = asset_queue;

    std::mutex queue_mutex;
    std::atomic<size_t> encoding_completed_assets{0};

    auto encoding_progress = std::make_shared<misc::ParallelProgress>(num_workers);
    std::vector<std::future<void>> encoding_workers;

    for (unsigned int i = 0; i < num_workers; ++i) {
      encoding_workers.push_back(
          std::async(std::launch::async, encoding_worker, std::ref(asset_queue), std::ref(queue_mutex), std::ref(encoding_completed_assets), std::cref(l2_archive_base), std::cref(temp_dir), i, encoding_progress->acquire_slot("")));
    }

    for (auto &worker : encoding_workers) {
      worker.wait();
    }
    encoding_progress->stop();

    std::cout << "Encoding complete: " << encoding_completed_assets << " / " << asset_queue_for_analysis.size() << " assets\n\n";

    // ========================================================================
    // PHASE 2: ANALYSIS
    // ========================================================================
    std::cout << "=== Phase 2: Analysis ===" << "\n";

    // Calculate sorted unique dates
    std::set<std::string> unique_dates_set;
    for (const auto &asset : asset_queue_for_analysis) {
      unique_dates_set.insert(asset.dates.begin(), asset.dates.end());
    }
    std::vector<std::string> all_dates(unique_dates_set.begin(), unique_dates_set.end());
    
    std::cout << "Date range: " << all_dates.front() << " → " << all_dates.back() 
              << " (" << all_dates.size() << " trading days)\n";

    // Initialize global feature store with preallocated blocks
    GlobalFeatureStore feature_store(asset_queue_for_analysis.size(), all_dates.size());

    // Distribute assets evenly across workers
    std::vector<std::vector<AssetTask>> worker_asset_batches(num_workers);
    for (size_t i = 0; i < asset_queue_for_analysis.size(); ++i) {
      worker_asset_batches[i % num_workers].push_back(asset_queue_for_analysis[i]);
    }

    // Launch workers with date-first processing
    auto analysis_progress = std::make_shared<misc::ParallelProgress>(num_workers);
    std::vector<std::future<void>> analysis_workers;

    for (unsigned int i = 0; i < num_workers; ++i) {
      analysis_workers.push_back(std::async(std::launch::async,
                                            analysis_worker_date_first,
                                            std::cref(worker_asset_batches[i]),
                                            std::cref(all_dates),
                                            std::cref(temp_dir),
                                            &feature_store,
                                            analysis_progress->acquire_slot("")));
    }

    for (auto &worker : analysis_workers) {
      worker.wait();
    }
    analysis_progress->stop();

    // Print feature storage summary
    std::cout << "\n";
    std::cout << "Feature Storage Summary:\n";
    std::cout << "  Total assets: " << feature_store.get_num_assets() << "\n";
    std::cout << "  Total dates: " << feature_store.get_num_dates() << "\n";
    std::cout << "\n";

    Logger::close();

    if (Config::CLEANUP_AFTER_PROCESSING) {
      if (std::filesystem::exists(temp_dir)) {
        std::filesystem::remove_all(temp_dir);
      }
    }

    std::cout << "\n=== Processing Complete ===" << "\n";
    return 0;

  } catch (const std::exception &e) {
    std::cerr << "Error: " << e.what() << "\n";
    std::cerr << "Check: config files, " << Config::ARCHIVE_TOOL << " availability, archive path\n";
    std::cerr << "Format: " << Config::DEFAULT_L2_ARCHIVE_BASE << "/YYYY/YYYYMM/YYYYMMDD" << Config::ARCHIVE_EXTENSION << "\n";
    return 1;
  }
}
