#include "codec/binary_encoder_L2.hpp"
#include "codec/delta_encoding.hpp"
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <locale>

namespace L2 {

// Open file with GBK locale for direct reading
static std::ifstream open_gbk_file(const std::string& filepath) {
    std::ifstream file(filepath);
    if (file.is_open()) {
        // Set locale to handle GBK encoding properly
        try {
            file.imbue(std::locale("zh_CN.GBK"));
        } catch (const std::exception&) {
            // Fallback to default locale if GBK not available
            // File will still work, just might have encoding issues with Chinese characters
        }
    }
    return file;
}

// Constructor with capacity hints
BinaryEncoder_L2::BinaryEncoder_L2(size_t estimated_snapshots, size_t estimated_orders) {
    // Pre-reserve space for snapshot vectors
    temp_hours.reserve(estimated_snapshots);
    temp_minutes.reserve(estimated_snapshots);
    temp_seconds.reserve(estimated_snapshots);
    temp_highs.reserve(estimated_snapshots);
    temp_lows.reserve(estimated_snapshots);
    temp_closes.reserve(estimated_snapshots);
    temp_all_bid_vwaps.reserve(estimated_snapshots);
    temp_all_ask_vwaps.reserve(estimated_snapshots);
    temp_all_bid_volumes.reserve(estimated_snapshots);
    temp_all_ask_volumes.reserve(estimated_snapshots);
    
    for (size_t i = 0; i < 10; ++i) {
        temp_bid_prices[i].reserve(estimated_snapshots);
        temp_ask_prices[i].reserve(estimated_snapshots);
    }
    
    // Pre-reserve space for order vectors
    temp_order_hours.reserve(estimated_orders);
    temp_order_minutes.reserve(estimated_orders);
    temp_order_seconds.reserve(estimated_orders);
    temp_order_milliseconds.reserve(estimated_orders);
    temp_order_prices.reserve(estimated_orders);
    temp_bid_order_ids.reserve(estimated_orders);
    temp_ask_order_ids.reserve(estimated_orders);
}

// CSV parsing functions
std::vector<std::string> BinaryEncoder_L2::split_csv_line(const std::string &line) {
  std::vector<std::string> result;
  std::stringstream ss(line);
  std::string item;

  while (std::getline(ss, item, ',')) {
    result.push_back(item);
  }

  return result;
}

uint32_t BinaryEncoder_L2::parse_time_to_ms(uint32_t time_int) {
  // time format: HHMMSSMS (8 digits)
  uint32_t ms = time_int % 1000;
  time_int /= 1000;
  uint32_t second = time_int % 100;
  time_int /= 100;
  uint32_t minute = time_int % 100;
  time_int /= 100;
  uint32_t hour = time_int;

  return hour * 3600000 + minute * 60000 + second * 1000 + ms;
}

inline uint32_t BinaryEncoder_L2::parse_price_to_fen(const std::string &price_str) {
  if (price_str.empty() || price_str[0] == ' ' || price_str[0] == '\0') {
    return 0;
  }
  // Optimized: Convert directly from CSV to 0.01 RMB units
  // CSV / 10000 / 0.01 = CSV / 100, avoiding floating point operations
  return static_cast<uint32_t>(std::stoll(price_str) / 100);
}

inline uint32_t BinaryEncoder_L2::parse_vwap_price(const std::string &price_str) {
  if (price_str.empty() || price_str[0] == ' ' || price_str[0] == '\0') {
    return 0;
  }
  // Optimized: Convert directly from CSV to 0.001 RMB units
  // CSV / 10000 / 0.001 = CSV / 10, avoiding floating point operations
  return static_cast<uint32_t>(std::stoll(price_str) / 10);
}

inline uint32_t BinaryEncoder_L2::parse_volume_to_100shares(const std::string &volume_str) {
  if (volume_str.empty() || volume_str[0] == ' ' || volume_str[0] == '\0') {
    return 0;
  }
  // Convert from shares to 100-share units
  return static_cast<uint32_t>(std::stoll(volume_str) / 100);
}

inline uint64_t BinaryEncoder_L2::parse_turnover_to_fen(const std::string &turnover_str) {
  if (turnover_str.empty() || turnover_str[0] == ' ' || turnover_str[0] == '\0') {
    return 0;
  }
  return static_cast<uint64_t>(std::stod(turnover_str));
}

bool BinaryEncoder_L2::parse_snapshot_csv(const std::string &filepath, std::vector<CSVSnapshot> &snapshots) {
  // Open file with GBK locale
  auto file = open_gbk_file(filepath);
  if (!file.is_open()) [[unlikely]] {
    std::cerr << "L2 Encoder: Failed to open snapshot CSV: " << filepath << std::endl;
    return false;
  }

  std::string line;
  
  // Skip header line
  if (!std::getline(file, line)) [[unlikely]] {
    std::cerr << "L2 Encoder: Failed to read snapshot CSV header: " << filepath << std::endl;
    return false;
  }

  uint32_t prev_trade_count = 0; // Track previous trade count

  while (std::getline(file, line)) {
    auto fields = split_csv_line(line);
    if (fields.size() < 65) { // Expected minimum number of fields
      continue;
    }

    CSVSnapshot snapshot = {};
    snapshot.stock_code = fields[0];
    snapshot.exchange_code = fields[1];
    snapshot.date = std::stoul(fields[2]);
    snapshot.time = std::stoul(fields[3]);
    snapshot.price = parse_price_to_fen(fields[4]);
    snapshot.volume = parse_volume_to_100shares(fields[5]);
    snapshot.turnover = parse_turnover_to_fen(fields[6]);

    // Calculate incremental trade count
    uint32_t curr_trade_count = std::stoul(fields[7]);
    assert(curr_trade_count >= prev_trade_count && "Trade count should never decrease");
    snapshot.trade_count = curr_trade_count - prev_trade_count;
    prev_trade_count = curr_trade_count;

    snapshot.high = parse_price_to_fen(fields[13]);
    snapshot.low = parse_price_to_fen(fields[14]);
    snapshot.open = parse_price_to_fen(fields[15]);
    snapshot.prev_close = parse_price_to_fen(fields[16]);

    // Parse ask prices (申卖价1-10: fields 17-26)
    for (int i = 0; i < 10; i++) {
      snapshot.ask_prices[i] = parse_price_to_fen(fields[17 + i]);
    }

    // Parse ask volumes (申卖量1-10: fields 27-36)
    for (int i = 0; i < 10; i++) {
      snapshot.ask_volumes[i] = parse_volume_to_100shares(fields[27 + i]);
    }

    // Parse bid prices (申买价1-10: fields 37-46)
    for (int i = 0; i < 10; i++) {
      snapshot.bid_prices[i] = parse_price_to_fen(fields[37 + i]);
    }

    // Parse bid volumes (申买量1-10: fields 47-56)
    for (int i = 0; i < 10; i++) {
      snapshot.bid_volumes[i] = parse_volume_to_100shares(fields[47 + i]);
    }

    snapshot.weighted_avg_ask_price = parse_vwap_price(fields[57]);
    snapshot.weighted_avg_bid_price = parse_vwap_price(fields[58]);
    snapshot.total_ask_volume = parse_volume_to_100shares(fields[59]);
    snapshot.total_bid_volume = parse_volume_to_100shares(fields[60]);

    snapshots.push_back(snapshot);
  }

  return true;
}

bool BinaryEncoder_L2::parse_order_csv(const std::string &filepath, std::vector<CSVOrder> &orders) {
  // Open file with GBK locale
  auto file = open_gbk_file(filepath);
  if (!file.is_open()) [[unlikely]] {
    std::cerr << "L2 Encoder: Failed to open order CSV: " << filepath << std::endl;
    return false;
  }

  std::string line;
  
  // Skip header line
  if (!std::getline(file, line)) [[unlikely]] {
    std::cerr << "L2 Encoder: Failed to read order CSV header: " << filepath << std::endl;
    return false;
  }

  while (std::getline(file, line)) {
    auto fields = split_csv_line(line);
    if (fields.size() < 10) {
      continue;
    }

    CSVOrder order = {};
    order.stock_code = fields[0];
    order.exchange_code = fields[1];
    order.date = std::stoul(fields[2]);
    order.time = std::stoul(fields[3]);
    order.order_id = std::stoull(fields[4]);
    order.exchange_order_id = std::stoull(fields[5]);

    bool is_szse = is_szse_market(order.stock_code);

    if (is_szse) {
      // SZSE: field[6] = 委托类型 (always '0', not used)
      //       field[7] = 委托代码 (B/S for buy/sell)
      order.order_type = '0'; // Always '0' for SZSE
      if (!fields[7].empty()) {
        order.order_side = fields[7][0];
      } else {
        order.order_side = ' ';
      }
    } else {
      // SSE: field[6] = 委托类型 (A:add, D:delete)
      //      field[7] = 委托代码 (B/S for buy/sell)
      if (!fields[6].empty()) {
        order.order_type = fields[6][0];
      } else {
        order.order_type = 'A'; // Default to add for SSE
      }
      if (!fields[7].empty()) {
        order.order_side = fields[7][0];
      } else {
        order.order_side = ' ';
      }
    }

    order.price = parse_price_to_fen(fields[8]);
    order.volume = parse_volume_to_100shares(fields[9]);

    orders.push_back(order);
  }

  return true;
}

bool BinaryEncoder_L2::parse_trade_csv(const std::string &filepath, std::vector<CSVTrade> &trades) {
  // Open file with GBK locale
  auto file = open_gbk_file(filepath);
  if (!file.is_open()) [[unlikely]] {
    std::cerr << "L2 Encoder: Failed to open trade CSV: " << filepath << std::endl;
    return false;
  }

  std::string line;
  
  // Skip header line
  if (!std::getline(file, line)) [[unlikely]] {
    std::cerr << "L2 Encoder: Failed to read trade CSV header: " << filepath << std::endl;
    return false;
  }

  while (std::getline(file, line)) {
    auto fields = split_csv_line(line);
    if (fields.size() < 12) {
      continue;
    }

    CSVTrade trade = {};
    trade.stock_code = fields[0];
    trade.exchange_code = fields[1];
    trade.date = std::stoul(fields[2]);
    trade.time = std::stoul(fields[3]);
    trade.trade_id = std::stoull(fields[4]);

    bool is_szse = is_szse_market(trade.stock_code);

    if (is_szse) {
      // SZSE: field[5] = 成交代码 (0:成交, C:撤单)
      //       field[6] = 委托代码 (not used, always empty)
      //       field[7] = BS标志 (B:buy, S:sell, empty:撤单)
      if (!fields[5].empty()) {
        trade.trade_code = fields[5][0];
      } else {
        trade.trade_code = '0'; // Default to trade
      }
      trade.dummy_code = ' '; // Not used for SZSE

      if (!fields[7].empty()) {
        trade.bs_flag = fields[7][0];
      } else {
        trade.bs_flag = ' '; // Empty for cancel
      }
    } else {
      // SSE: field[5] = 成交代码 (not used, always empty)
      //      field[6] = 委托代码 (not used, always empty)
      //      field[7] = BS标志 (B:buy, S:sell)
      trade.trade_code = '0'; // SSE doesn't use trade_code
      trade.dummy_code = ' '; // Not used for SSE

      if (!fields[7].empty()) {
        trade.bs_flag = fields[7][0];
      } else {
        trade.bs_flag = ' ';
      }
    }

    trade.price = parse_price_to_fen(fields[8]);
    trade.volume = parse_volume_to_100shares(fields[9]);
    trade.ask_order_id = std::stoull(fields[10]);
    trade.bid_order_id = std::stoull(fields[11]);

    trades.push_back(trade);
  }

  return true;
}

// Private helper functions
inline uint8_t BinaryEncoder_L2::time_to_hour(uint32_t time_ms) {
  return static_cast<uint8_t>(time_ms / 3600000);
}

inline uint8_t BinaryEncoder_L2::time_to_minute(uint32_t time_ms) {
  return static_cast<uint8_t>((time_ms % 3600000) / 60000);
}

inline uint8_t BinaryEncoder_L2::time_to_second(uint32_t time_ms) {
  return static_cast<uint8_t>((time_ms % 60000) / 1000);
}

inline uint8_t BinaryEncoder_L2::time_to_millisecond_10ms(uint32_t time_ms) {
  return static_cast<uint8_t>((time_ms % 1000) / 10);
}

inline bool BinaryEncoder_L2::is_szse_market(const std::string &stock_code) {
  // Optimized: check last 3 characters directly for better performance
  if (stock_code.size() >= 3) {
    const std::string suffix = stock_code.substr(stock_code.size() - 3);
    if (suffix == ".SZ")
      return true;
    if (suffix == ".SH")
      return false;
  }
  assert(false && "Stock code must contain either .SZ or .SH");
  return false; // Unreachable
}

inline uint8_t BinaryEncoder_L2::determine_order_type(char csv_order_type, char csv_trade_code, bool is_trade, bool is_szse) {
  if (is_trade) {
    // For trades, check trade_code if available (SZSE uses it)
    return (is_szse && csv_trade_code == 'C') ? 1 : 3; // cancel or taker
  }

  // For orders
  if (is_szse) {
    return 0; // maker (order) - SZSE always uses 0
  } else {
    // SSE: determine from order type
    return (csv_order_type == 'A') ? 0 : (csv_order_type == 'D') ? 1
                                                                 : 0;
  }
}

inline bool BinaryEncoder_L2::determine_order_direction(char side_flag) {
  return side_flag == 'S'; // 0:bid(B), 1:ask(S)
}

// CSV to L2 conversion functions
Snapshot BinaryEncoder_L2::csv_to_snapshot(const CSVSnapshot &csv_snap) {
  Snapshot snapshot = {};

  uint32_t time_ms = parse_time_to_ms(csv_snap.time);
  snapshot.hour = BitwidthBounds::clamp_to_bound(time_to_hour(time_ms), BitwidthBounds::HOUR_BOUND);
  snapshot.minute = BitwidthBounds::clamp_to_bound(time_to_minute(time_ms), BitwidthBounds::MINUTE_BOUND);
  snapshot.second = BitwidthBounds::clamp_to_bound(time_to_second(time_ms), BitwidthBounds::SECOND_BOUND);

  snapshot.trade_count = BitwidthBounds::clamp_to_bound(csv_snap.trade_count, BitwidthBounds::TRADE_COUNT_BOUND);
  snapshot.volume = BitwidthBounds::clamp_to_bound(csv_snap.volume, BitwidthBounds::VOLUME_BOUND);
  snapshot.turnover = BitwidthBounds::clamp_to_bound(csv_snap.turnover, BitwidthBounds::TURNOVER_BOUND);

  // Convert prices using 14-bit bounds
  snapshot.high = BitwidthBounds::clamp_to_bound(csv_snap.high, BitwidthBounds::PRICE_BOUND);
  snapshot.low = BitwidthBounds::clamp_to_bound(csv_snap.low, BitwidthBounds::PRICE_BOUND);
  snapshot.close = BitwidthBounds::clamp_to_bound(csv_snap.price, BitwidthBounds::PRICE_BOUND);

  // Copy bid/ask prices and volumes using bitwidth-based bounds
  for (int i = 0; i < 10; i++) {
    snapshot.bid_price_ticks[i] = BitwidthBounds::clamp_to_bound(csv_snap.bid_prices[i], BitwidthBounds::PRICE_BOUND);
    snapshot.bid_volumes[i] = BitwidthBounds::clamp_to_bound(csv_snap.bid_volumes[i], BitwidthBounds::ORDERBOOK_VOLUME_BOUND);
    snapshot.ask_price_ticks[i] = BitwidthBounds::clamp_to_bound(csv_snap.ask_prices[i], BitwidthBounds::PRICE_BOUND);
    snapshot.ask_volumes[i] = BitwidthBounds::clamp_to_bound(csv_snap.ask_volumes[i], BitwidthBounds::ORDERBOOK_VOLUME_BOUND);
  }

  // Determine direction based on price movement (simplified)
  snapshot.direction = false; // Default to buy direction

  snapshot.all_bid_vwap = BitwidthBounds::clamp_to_bound(csv_snap.weighted_avg_bid_price, BitwidthBounds::VWAP_BOUND);
  snapshot.all_ask_vwap = BitwidthBounds::clamp_to_bound(csv_snap.weighted_avg_ask_price, BitwidthBounds::VWAP_BOUND);
  snapshot.all_bid_volume = BitwidthBounds::clamp_to_bound(csv_snap.total_bid_volume, BitwidthBounds::TOTAL_VOLUME_BOUND);
  snapshot.all_ask_volume = BitwidthBounds::clamp_to_bound(csv_snap.total_ask_volume, BitwidthBounds::TOTAL_VOLUME_BOUND);

  return snapshot;
}

Order BinaryEncoder_L2::csv_to_order(const CSVOrder &csv_order) {
  Order order = {};

  uint32_t time_ms = parse_time_to_ms(csv_order.time);
  order.hour = BitwidthBounds::clamp_to_bound(time_to_hour(time_ms), BitwidthBounds::HOUR_BOUND);
  order.minute = BitwidthBounds::clamp_to_bound(time_to_minute(time_ms), BitwidthBounds::MINUTE_BOUND);
  order.second = BitwidthBounds::clamp_to_bound(time_to_second(time_ms), BitwidthBounds::SECOND_BOUND);
  order.millisecond = BitwidthBounds::clamp_to_bound(time_to_millisecond_10ms(time_ms), BitwidthBounds::MILLISECOND_BOUND);

  bool is_szse = is_szse_market(csv_order.stock_code);
  order.order_type = BitwidthBounds::clamp_to_bound(determine_order_type(csv_order.order_type, '0', false, is_szse), BitwidthBounds::ORDER_TYPE_BOUND);
  order.order_dir = BitwidthBounds::clamp_to_bound(determine_order_direction(csv_order.order_side), BitwidthBounds::ORDER_DIR_BOUND);
  order.price = BitwidthBounds::clamp_to_bound(csv_order.price, BitwidthBounds::PRICE_BOUND);
  order.volume = BitwidthBounds::clamp_to_bound(csv_order.volume, BitwidthBounds::VOLUME_BOUND);

  // Set order IDs based on direction and type
  if (order.order_dir == 0) { // bid
    order.bid_order_id = BitwidthBounds::clamp_to_bound(csv_order.order_id, BitwidthBounds::ORDER_ID_BOUND);
    order.ask_order_id = 0;
  } else { // ask
    order.bid_order_id = 0;
    order.ask_order_id = BitwidthBounds::clamp_to_bound(csv_order.order_id, BitwidthBounds::ORDER_ID_BOUND);
  }

  return order;
}

Order BinaryEncoder_L2::csv_to_trade(const CSVTrade &csv_trade) {
  Order order = {};

  uint32_t time_ms = parse_time_to_ms(csv_trade.time);
  order.hour = BitwidthBounds::clamp_to_bound(time_to_hour(time_ms), BitwidthBounds::HOUR_BOUND);
  order.minute = BitwidthBounds::clamp_to_bound(time_to_minute(time_ms), BitwidthBounds::MINUTE_BOUND);
  order.second = BitwidthBounds::clamp_to_bound(time_to_second(time_ms), BitwidthBounds::SECOND_BOUND);
  order.millisecond = BitwidthBounds::clamp_to_bound(time_to_millisecond_10ms(time_ms), BitwidthBounds::MILLISECOND_BOUND);

  bool is_szse = is_szse_market(csv_trade.stock_code);
  order.order_type = BitwidthBounds::clamp_to_bound(determine_order_type('0', csv_trade.trade_code, true, is_szse), BitwidthBounds::ORDER_TYPE_BOUND);
  order.order_dir = BitwidthBounds::clamp_to_bound(determine_order_direction(csv_trade.bs_flag), BitwidthBounds::ORDER_DIR_BOUND);
  order.price = BitwidthBounds::clamp_to_bound(csv_trade.price, BitwidthBounds::PRICE_BOUND);
  order.volume = BitwidthBounds::clamp_to_bound(csv_trade.volume, BitwidthBounds::VOLUME_BOUND);

  // For trades, set both order IDs
  order.bid_order_id = BitwidthBounds::clamp_to_bound(csv_trade.bid_order_id, BitwidthBounds::ORDER_ID_BOUND);
  order.ask_order_id = BitwidthBounds::clamp_to_bound(csv_trade.ask_order_id, BitwidthBounds::ORDER_ID_BOUND);

  return order;
}



// binary encoding functions
bool BinaryEncoder_L2::encode_snapshots(const std::vector<Snapshot>& snapshots, 
                                        const std::string& filepath, bool use_delta) {
  if (snapshots.empty()) {
    std::cerr << "L2 Encoder: No snapshots to encode: " << filepath << std::endl;
    return false;
  }
  
  // Create local copies for delta encoding (based on schema use_delta flags)
  std::vector<Snapshot> delta_snapshots = snapshots;
  size_t count = delta_snapshots.size();
  
  if (use_delta && count > 1) {
    // std::cout << "L2 Encoder: Applying delta encoding to " << count << " snapshots..." << std::endl;
    // Apply delta encoding to columns marked with use_delta=true in schema
    // Reuse pre-allocated member vectors for better performance
    temp_hours.resize(count);
    temp_minutes.resize(count);
    temp_seconds.resize(count);
    temp_highs.resize(count);
    temp_lows.resize(count);
    temp_closes.resize(count);
    temp_all_bid_vwaps.resize(count);
    temp_all_ask_vwaps.resize(count);
    temp_all_bid_volumes.resize(count);
    temp_all_ask_volumes.resize(count);
    
    for (size_t i = 0; i < 10; ++i) {
      temp_bid_prices[i].resize(count);
      temp_ask_prices[i].resize(count);
    }
    
    // Extract data
    for (size_t i = 0; i < count; ++i) {
      temp_hours[i] = delta_snapshots[i].hour;
      temp_minutes[i] = delta_snapshots[i].minute;
      temp_seconds[i] = delta_snapshots[i].second;
      temp_highs[i] = delta_snapshots[i].high;
      temp_lows[i] = delta_snapshots[i].low;
      temp_closes[i] = delta_snapshots[i].close;
      temp_all_bid_vwaps[i] = delta_snapshots[i].all_bid_vwap;
      temp_all_ask_vwaps[i] = delta_snapshots[i].all_ask_vwap;
      temp_all_bid_volumes[i] = delta_snapshots[i].all_bid_volume;
      temp_all_ask_volumes[i] = delta_snapshots[i].all_ask_volume;
      
      for (size_t j = 0; j < 10; ++j) {
        temp_bid_prices[j][i] = delta_snapshots[i].bid_price_ticks[j];
        temp_ask_prices[j][i] = delta_snapshots[i].ask_price_ticks[j];
      }
    }
    
    // Apply delta encoding where use_delta=true
    DeltaUtils::encode_deltas(temp_hours.data(), count);
    DeltaUtils::encode_deltas(temp_minutes.data(), count);
    DeltaUtils::encode_deltas(temp_seconds.data(), count);
    DeltaUtils::encode_deltas(temp_highs.data(), count);
    DeltaUtils::encode_deltas(temp_lows.data(), count);
    DeltaUtils::encode_deltas(temp_closes.data(), count);
    DeltaUtils::encode_deltas(temp_all_bid_vwaps.data(), count);
    DeltaUtils::encode_deltas(temp_all_ask_vwaps.data(), count);
    DeltaUtils::encode_deltas(temp_all_bid_volumes.data(), count);
    DeltaUtils::encode_deltas(temp_all_ask_volumes.data(), count);
    
    for (size_t j = 0; j < 10; ++j) {
      DeltaUtils::encode_deltas(temp_bid_prices[j].data(), count);
      DeltaUtils::encode_deltas(temp_ask_prices[j].data(), count);
    }
    
    // Copy back delta-encoded data
    for (size_t i = 0; i < count; ++i) {
      delta_snapshots[i].hour = temp_hours[i];
      delta_snapshots[i].minute = temp_minutes[i];
      delta_snapshots[i].second = temp_seconds[i];
      delta_snapshots[i].high = temp_highs[i];
      delta_snapshots[i].low = temp_lows[i];
      delta_snapshots[i].close = temp_closes[i];
      delta_snapshots[i].all_bid_vwap = temp_all_bid_vwaps[i];
      delta_snapshots[i].all_ask_vwap = temp_all_ask_vwaps[i];
      delta_snapshots[i].all_bid_volume = temp_all_bid_volumes[i];
      delta_snapshots[i].all_ask_volume = temp_all_ask_volumes[i];
      
      for (size_t j = 0; j < 10; ++j) {
        delta_snapshots[i].bid_price_ticks[j] = temp_bid_prices[j][i];
        delta_snapshots[i].ask_price_ticks[j] = temp_ask_prices[j][i];
      }
    }
  }
  
  // Prepare data for compression: count + snapshots
  size_t header_size = sizeof(count);
  size_t snapshots_size = delta_snapshots.size() * sizeof(Snapshot);
  size_t total_size = header_size + snapshots_size;
  
  auto data_buffer = std::make_unique<char[]>(total_size);
  char* write_ptr = data_buffer.get();
  
  // Copy count to buffer
  std::memcpy(write_ptr, &count, header_size);
  write_ptr += header_size;
  
  // Copy snapshots to buffer
  std::memcpy(write_ptr, delta_snapshots.data(), snapshots_size);
  
  // Compress and write data
  if (!compress_and_write_data(filepath, data_buffer.get(), total_size)) {
    return false;
  }
  
  // Progress is now shown via print_progress_with_message in workers.cpp
  // std::cout << "L2 Encoder: Compressed " << compression_stats.original_size << " bytes to " 
  //           << compression_stats.compressed_size << " bytes (ratio: " << std::fixed 
  //           << std::setprecision(2) << compression_stats.ratio << "x), wrote " << count 
  //           << " snapshots to " << filepath << std::endl;
  
  return true;
}

bool BinaryEncoder_L2::encode_orders(const std::vector<Order>& orders,
                                    const std::string& filepath, bool use_delta) {
  if (orders.empty()) {
    std::cerr << "L2 Encoder: No orders to encode: " << filepath << std::endl;
    return false;
  }
  
  // Create local copies for delta encoding (based on schema use_delta flags)
  std::vector<Order> delta_orders = orders;
  size_t count = delta_orders.size();
  
  if (use_delta && count > 1) {
    // std::cout << "L2 Encoder: Applying delta encoding to " << count << " orders..." << std::endl;
    // Apply delta encoding to columns marked with use_delta=true in schema
    // Reuse pre-allocated member vectors for better performance
    temp_order_hours.resize(count);
    temp_order_minutes.resize(count);
    temp_order_seconds.resize(count);
    temp_order_milliseconds.resize(count);
    temp_order_prices.resize(count);
    temp_bid_order_ids.resize(count);
    temp_ask_order_ids.resize(count);
    
    // Extract data
    for (size_t i = 0; i < count; ++i) {
      temp_order_hours[i] = delta_orders[i].hour;
      temp_order_minutes[i] = delta_orders[i].minute;
      temp_order_seconds[i] = delta_orders[i].second;
      temp_order_milliseconds[i] = delta_orders[i].millisecond;
      temp_order_prices[i] = delta_orders[i].price;
      temp_bid_order_ids[i] = delta_orders[i].bid_order_id;
      temp_ask_order_ids[i] = delta_orders[i].ask_order_id;
    }
    
    // Apply delta encoding where use_delta=true (based on schema)
    DeltaUtils::encode_deltas(temp_order_hours.data(), count);
    DeltaUtils::encode_deltas(temp_order_minutes.data(), count);
    DeltaUtils::encode_deltas(temp_order_seconds.data(), count);
    DeltaUtils::encode_deltas(temp_order_milliseconds.data(), count);
    DeltaUtils::encode_deltas(temp_order_prices.data(), count);
    DeltaUtils::encode_deltas(temp_bid_order_ids.data(), count);
    DeltaUtils::encode_deltas(temp_ask_order_ids.data(), count);
    
    // Copy back delta-encoded data
    for (size_t i = 0; i < count; ++i) {
      delta_orders[i].hour = temp_order_hours[i];
      delta_orders[i].minute = temp_order_minutes[i];
      delta_orders[i].second = temp_order_seconds[i];
      delta_orders[i].millisecond = temp_order_milliseconds[i];
      delta_orders[i].price = temp_order_prices[i];
      delta_orders[i].bid_order_id = temp_bid_order_ids[i];
      delta_orders[i].ask_order_id = temp_ask_order_ids[i];
    }
  }
  
  // Prepare data for compression: count + orders
  size_t header_size = sizeof(count);
  size_t orders_size = delta_orders.size() * sizeof(Order);
  size_t total_size = header_size + orders_size;
  
  auto data_buffer = std::make_unique<char[]>(total_size);
  char* write_ptr = data_buffer.get();
  
  // Copy count to buffer
  std::memcpy(write_ptr, &count, header_size);
  write_ptr += header_size;
  
  // Copy orders to buffer
  std::memcpy(write_ptr, delta_orders.data(), orders_size);
  
  // Compress and write data
  if (!compress_and_write_data(filepath, data_buffer.get(), total_size)) {
    return false;
  }
  
  // Progress is now shown via print_progress_with_message in workers.cpp  
  // std::cout << "L2 Encoder: Compressed " << compression_stats.original_size << " bytes to " 
  //           << compression_stats.compressed_size << " bytes (ratio: " << std::fixed 
  //           << std::setprecision(2) << compression_stats.ratio << "x), wrote " << count 
  //           << " orders to " << filepath << std::endl;
  
  return true;
}

// Enhanced processing function with delta encoding
bool BinaryEncoder_L2::process_stock_data(const std::string& stock_dir,
                                          const std::string& output_dir,
                                          const std::string& stock_code,
                                          std::vector<Snapshot>* out_snapshots,
                                          std::vector<Order>* out_orders) {
  // Create output directory if it doesn't exist
  std::filesystem::create_directories(output_dir);
  
  std::vector<CSVSnapshot> csv_snapshots;
  std::vector<CSVOrder> csv_orders;
  std::vector<CSVTrade> csv_trades;
  
  // Parse CSV files (using existing methods)
  std::string snapshot_file = stock_dir + "/行情.csv";
  std::string order_file = stock_dir + "/逐笔委托.csv";
  std::string trade_file = stock_dir + "/逐笔成交.csv";
  
  // Parse snapshots
  if (std::filesystem::exists(snapshot_file)) {
    if (!parse_snapshot_csv(snapshot_file, csv_snapshots)) {
      return false;
    }
  }
  
  // Parse orders
  if (std::filesystem::exists(order_file)) {
    if (!parse_order_csv(order_file, csv_orders)) {
      return false;
    }
  }
  
  // Parse trades
  if (std::filesystem::exists(trade_file)) {
    if (!parse_trade_csv(trade_file, csv_trades)) {
      return false;
    }
  }
  
  // Convert and encode snapshots with compression
  if (!csv_snapshots.empty()) {
    std::vector<Snapshot> snapshots;
    snapshots.reserve(csv_snapshots.size());
    for (const auto& csv_snap : csv_snapshots) {
      snapshots.push_back(csv_to_snapshot(csv_snap));
    }
    
    // Store original data if requested
    if (out_snapshots) {
      *out_snapshots = snapshots;
    }
    
    std::string output_file = output_dir + "/" + stock_code + "_snapshots_" + std::to_string(snapshots.size()) + ".bin";
    if (!encode_snapshots(snapshots, output_file, ENABLE_DELTA_ENCODING)) {
      return false;
    }
  }
  
  // Convert and encode orders with compression
  std::vector<Order> all_orders;
  all_orders.reserve(csv_orders.size() + csv_trades.size());
  
  // Add orders
  for (const auto& csv_order : csv_orders) {
    all_orders.push_back(csv_to_order(csv_order));
  }
  
  // Add trades as taker orders
  for (const auto& csv_trade : csv_trades) {
    all_orders.push_back(csv_to_trade(csv_trade));
  }
  
  // Sort orders by time
  std::sort(all_orders.begin(), all_orders.end(), [](const Order& a, const Order& b) -> bool {
    uint32_t time_a = a.hour * 3600000 + a.minute * 60000 + a.second * 1000 + a.millisecond * 10;
    uint32_t time_b = b.hour * 3600000 + b.minute * 60000 + b.second * 1000 + b.millisecond * 10;
    return time_a < time_b;
  });
  
  if (!all_orders.empty()) {
    // Store original data if requested
    if (out_orders) {
      *out_orders = all_orders;
    }
    
    std::string output_file = output_dir + "/" + stock_code + "_orders_" + std::to_string(all_orders.size()) + ".bin";
    if (!encode_orders(all_orders, output_file, ENABLE_DELTA_ENCODING)) {
      return false;
    }
  }
  
  return true;
}

// Zstandard compression helper functions
size_t BinaryEncoder_L2::calculate_compression_bound(size_t data_size) {
  return ZSTD_compressBound(data_size);
}

bool BinaryEncoder_L2::compress_and_write_data(const std::string& filepath, const void* data, size_t data_size) {
  std::ofstream file(filepath, std::ios::binary);
  if (!file.is_open()) [[unlikely]] {
    std::cerr << "L2 Encoder: Failed to open file for compression: " << filepath << std::endl;
    return false;
  }

  // Calculate compression bound and allocate buffer
  size_t compressed_bound = calculate_compression_bound(data_size);
  auto compressed_buffer = std::make_unique<char[]>(compressed_bound);

  // Standard Zstandard compression
  size_t compressed_size = ZSTD_compress(
    compressed_buffer.get(), compressed_bound,
    data, data_size,
    ZSTD_COMPRESSION_LEVEL
  );

  if (ZSTD_isError(compressed_size)) [[unlikely]] {
    std::cerr << "L2 Encoder: Compression failed: " << ZSTD_getErrorName(compressed_size) << std::endl;
    return false;
  }

  // Write header: original size and compressed size
  file.write(reinterpret_cast<const char*>(&data_size), sizeof(data_size));
  file.write(reinterpret_cast<const char*>(&compressed_size), sizeof(compressed_size));
  
  if (file.fail()) [[unlikely]] {
    std::cerr << "L2 Encoder: Failed to write compression header: " << filepath << std::endl;
    return false;
  }

  // Write compressed data
  file.write(compressed_buffer.get(), compressed_size);
  
  if (file.fail()) [[unlikely]] {
    std::cerr << "L2 Encoder: Failed to write compressed data: " << filepath << std::endl;
    return false;
  }

  // Store compression statistics for later reporting
  compression_stats.original_size = data_size;
  compression_stats.compressed_size = compressed_size;
  compression_stats.ratio = static_cast<double>(data_size) / static_cast<double>(compressed_size);

  return true;
}

} // namespace L2
