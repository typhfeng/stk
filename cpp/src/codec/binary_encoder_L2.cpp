#include "codec/binary_encoder_L2.hpp"
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>

namespace L2 {

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
  std::ifstream file(filepath);
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

  file.close();
  return true;
}

bool BinaryEncoder_L2::parse_order_csv(const std::string &filepath, std::vector<CSVOrder> &orders) {
  std::ifstream file(filepath);
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

  file.close();
  return true;
}

bool BinaryEncoder_L2::parse_trade_csv(const std::string &filepath, std::vector<CSVTrade> &trades) {
  std::ifstream file(filepath);
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

  file.close();
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

// Binary encoding functions
bool BinaryEncoder_L2::encode_snapshots_to_binary(const std::vector<Snapshot> &snapshots,
                                                  const std::string &filepath) {
  std::ofstream file(filepath, std::ios::binary);
  if (!file.is_open()) [[unlikely]] {
    std::cerr << "L2 Encoder: Failed to open snapshot output file: " << filepath << std::endl;
    return false;
  }

  // Write header: number of snapshots
  size_t count = snapshots.size();
  file.write(reinterpret_cast<const char *>(&count), sizeof(count));
  if (file.fail()) [[unlikely]] {
    std::cerr << "L2 Encoder: Failed to write snapshot header: " << filepath << std::endl;
    return false;
  }

  // Batch write all snapshots at once for maximum efficiency
  if (count > 0) {
    file.write(reinterpret_cast<const char *>(snapshots.data()), count * sizeof(Snapshot));
    if (file.fail()) [[unlikely]] {
      std::cerr << "L2 Encoder: Failed to write snapshot data: " << filepath << std::endl;
      return false;
    }
  }

  return true;
}

bool BinaryEncoder_L2::encode_orders_to_binary(const std::vector<Order> &orders,
                                               const std::string &filepath) {
  std::ofstream file(filepath, std::ios::binary);
  if (!file.is_open()) [[unlikely]] {
    std::cerr << "L2 Encoder: Failed to open order output file: " << filepath << std::endl;
    return false;
  }

  // Write header: number of orders
  size_t count = orders.size();
  file.write(reinterpret_cast<const char *>(&count), sizeof(count));
  if (file.fail()) [[unlikely]] {
    std::cerr << "L2 Encoder: Failed to write order header: " << filepath << std::endl;
    return false;
  }

  // Batch write all orders at once for maximum efficiency
  if (count > 0) {
    file.write(reinterpret_cast<const char *>(orders.data()), count * sizeof(Order));
    if (file.fail()) [[unlikely]] {
      std::cerr << "L2 Encoder: Failed to write order data: " << filepath << std::endl;
      return false;
    }
  }

  return true;
}

// High-level processing function
bool BinaryEncoder_L2::process_stock_data(const std::string &stock_dir,
                                          const std::string &output_dir,
                                          const std::string &stock_code) {
  // Create output directory if it doesn't exist
  std::filesystem::create_directories(output_dir);

  std::vector<CSVSnapshot> csv_snapshots;
  std::vector<CSVOrder> csv_orders;
  std::vector<CSVTrade> csv_trades;

  // Parse CSV files
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

  // Convert and encode snapshots
  if (!csv_snapshots.empty()) {
    std::vector<Snapshot> snapshots;
    snapshots.reserve(csv_snapshots.size()); // Pre-allocate for efficiency
    for (const auto &csv_snap : csv_snapshots) {
      snapshots.push_back(csv_to_snapshot(csv_snap));
    }

    // Include count in filename for decoder optimization
    std::string output_file = output_dir + "/" + stock_code + "_snapshots_" + std::to_string(snapshots.size()) + ".bin";
    if (!encode_snapshots_to_binary(snapshots, output_file)) {
      return false;
    }
  }

  // Convert and encode orders and trades together
  std::vector<Order> all_orders;
  all_orders.reserve(csv_orders.size() + csv_trades.size()); // Pre-allocate for efficiency

  // Add orders
  for (const auto &csv_order : csv_orders) {
    all_orders.push_back(csv_to_order(csv_order));
  }

  // Add trades as taker orders
  for (const auto &csv_trade : csv_trades) {
    all_orders.push_back(csv_to_trade(csv_trade));
  }

  // Sort orders by time using optimized lambda
  std::sort(all_orders.begin(), all_orders.end(), [](const Order &a, const Order &b) -> bool {
    uint32_t time_a = a.hour * 3600000 + a.minute * 60000 + a.second * 1000 + a.millisecond * 10;
    uint32_t time_b = b.hour * 3600000 + b.minute * 60000 + b.second * 1000 + b.millisecond * 10;
    return time_a < time_b;
  });

  if (!all_orders.empty()) {
    // Include count in filename for decoder optimization
    std::string output_file = output_dir + "/" + stock_code + "_orders_" + std::to_string(all_orders.size()) + ".bin";
    if (!encode_orders_to_binary(all_orders, output_file)) {
      return false;
    }
  }

  return true;
}

} // namespace L2
