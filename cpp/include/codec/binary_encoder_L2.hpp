#pragma once

#include "L2_DataType.hpp"
#include <string>
#include <vector>
#include <cassert>
#include <cstdint>

namespace L2 {

// Intermediate CSV data structures for parsing
struct CSVSnapshot {
    std::string stock_code;
    std::string exchange_code;
    uint32_t date;
    uint32_t time;
    uint32_t price;           // in 0.01 RMB units  
    uint32_t volume;          // in 100-share units
    uint64_t turnover;        // in fen
    uint32_t trade_count;
    
    uint32_t high;
    uint32_t low;
    uint32_t open;
    uint32_t prev_close;
    
    // bid/ask prices and volumes (10 levels each)
    uint32_t bid_prices[10];   // in 0.01 RMB units
    uint32_t bid_volumes[10];  // in 100-share units
    uint32_t ask_prices[10];   // in 0.01 RMB units
    uint32_t ask_volumes[10];  // in 100-share units
    
    uint32_t weighted_avg_ask_price;  // in 0.001 RMB units (VWAP)
    uint32_t weighted_avg_bid_price;  // in 0.001 RMB units (VWAP)
    uint32_t total_ask_volume;        // in 100-share units
    uint32_t total_bid_volume;        // in 100-share units
};

struct CSVOrder {
    std::string stock_code;
    std::string exchange_code;
    uint32_t date;
    uint32_t time;
    uint64_t order_id;
    uint64_t exchange_order_id;
    char order_type;     // A:add, D:delete for SSE; 0 for SZSE
    char order_side;     // B:bid, S:ask
    uint32_t price;      // in 0.01 RMB units
    uint32_t volume;     // in 100-share units
};

struct CSVTrade {
    std::string stock_code;
    std::string exchange_code;
    uint32_t date;
    uint32_t time;
    uint64_t trade_id;
    char trade_code;     // 0:trade, C:cancel for SZSE; empty for SSE
    char dummy_code;     // not used
    char bs_flag;        // B:buy, S:sell, empty:cancel
    uint32_t price;      // in 0.01 RMB units
    uint32_t volume;     // in 100-share units
    uint64_t ask_order_id;
    uint64_t bid_order_id;
};

class BinaryEncoder_L2 {
public:
    // CSV parsing functions (hot path functions inlined)
    static std::vector<std::string> split_csv_line(const std::string& line);
    static uint32_t parse_time_to_ms(uint32_t time_int);
    static inline uint32_t parse_price_to_fen(const std::string& price_str);
    static inline uint32_t parse_vwap_price(const std::string& price_str);  // For VWAP prices with 0.001 RMB precision
    static inline uint32_t parse_volume_to_100shares(const std::string& volume_str);  // Convert shares to 100-share units
    static inline uint64_t parse_turnover_to_fen(const std::string& turnover_str);
    
    static bool parse_snapshot_csv(const std::string& filepath, std::vector<CSVSnapshot>& snapshots);
    static bool parse_order_csv(const std::string& filepath, std::vector<CSVOrder>& orders);
    static bool parse_trade_csv(const std::string& filepath, std::vector<CSVTrade>& trades);

    // CSV to L2 conversion functions
    static Snapshot csv_to_snapshot(const CSVSnapshot& csv_snap);
    static Order csv_to_order(const CSVOrder& csv_order);
    static Order csv_to_trade(const CSVTrade& csv_trade);
    
    // Binary encoding functions
    static bool encode_snapshots_to_binary(const std::vector<Snapshot>& snapshots, 
                                          const std::string& filepath);
    static bool encode_orders_to_binary(const std::vector<Order>& orders,
                                       const std::string& filepath);
    
    // High-level processing function
    static bool process_stock_data(const std::string& stock_dir,
                                  const std::string& output_dir,
                                  const std::string& stock_code);

private:
    // Time conversion functions (inlined for performance)
    static inline uint8_t time_to_hour(uint32_t time_ms);
    static inline uint8_t time_to_minute(uint32_t time_ms);
    static inline uint8_t time_to_second(uint32_t time_ms);
    static inline uint8_t time_to_millisecond_10ms(uint32_t time_ms);
    
    // Market detection (inlined for performance)
    static inline bool is_szse_market(const std::string& stock_code);
    
    // Order processing (inlined for performance)
    static inline uint8_t determine_order_type(char csv_order_type, char csv_trade_code, bool is_trade, bool is_szse);
    static inline bool determine_order_direction(char side_flag);
};

} // namespace L2
