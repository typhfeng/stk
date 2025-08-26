#pragma once

#include <cstdint>

namespace L2 {

// ============================================================================
// 深圳交易所集中竞价快照行情数据结构定义
// SZSE Centralized Auction Snapshot Market Data Structures
// ============================================================================

// 行情条目类别枚举 - MDEntryType
enum class MDEntryType : char {
    // 买卖盘
    BID = '0',           // 买入
    ASK = '1',           // 卖出
    
    // 价格类
    LAST_PRICE = '2',    // 最近价
    OPEN_PRICE = '4',    // 开盘价
    HIGH_PRICE = '7',    // 最高价
    LOW_PRICE = '8',     // 最低价
    
    // 升跌
    PRICE_CHANGE_1 = 'A',    // x1=升跌一 (使用'A'表示x1)
    PRICE_CHANGE_2 = 'B',    // x2=升跌二 (使用'B'表示x2)
    
    // 汇总统计
    BID_SUMMARY = 'C',       // x3=买入汇总
    ASK_SUMMARY = 'D',       // x4=卖出汇总
    
    // 比率指标
    PE_RATIO_1 = 'E',        // x5=股票市盈率一
    PE_RATIO_2 = 'F',        // x6=股票市盈率二
    
    // 基金相关
    FUND_NAV = 'G',          // x7=基金T-1日净值
    FUND_IOPV = 'H',         // x8=基金实时参考净值(包括ETF的IOPV)
    
    // 权证
    WARRANT_PREMIUM = 'I',   // x9=权证溢价率
    
    // 价格限制
    LIMIT_UP = 'J',          // xe=涨停价
    LIMIT_DOWN = 'K',        // xf=跌停价
    
    // 期权相关
    OPEN_INTEREST = 'L',     // xg=合约持仓量
    REF_PRICE = 'M'          // xi=参考价
};

// 委托明细结构
#pragma pack(push, 1)
struct OrderEntry {
    uint64_t order_qty;      // 委托数量
};
#pragma pack(pop)

// 行情条目结构
#pragma pack(push, 1)
struct MDEntry {
    MDEntryType md_entry_type;       // 行情条目类别
    double md_entry_px;              // 价格
    uint64_t md_entry_size;          // 数量
    uint8_t md_price_level;          // 买卖盘档位 (1-10)
    uint32_t number_of_orders;       // 价位总委托笔数 (0表示不揭示)
    uint16_t no_orders;              // 价位揭示委托笔数 (0表示不揭示)
    
    // 委托明细数组，最多50笔
    OrderEntry orders[50];
    
    // 实际使用的委托明细数量
    uint16_t actual_orders_count;
    
    // 构造函数
    MDEntry() : md_entry_type(MDEntryType::LAST_PRICE), 
                md_entry_px(0.0), 
                md_entry_size(0), 
                md_price_level(0), 
                number_of_orders(0), 
                no_orders(0),
                actual_orders_count(0) {
        // 初始化委托明细数组
        for (int i = 0; i < 50; ++i) {
            orders[i].order_qty = 0;
        }
    }
};
#pragma pack(pop)

// 深圳交易所集中竞价快照行情主结构
#pragma pack(push, 1)
struct sz_stk_snapshot {
    uint16_t no_md_entries;          // 行情条目个数
    
    // 行情条目数组，根据实际情况动态分配
    // 最大可能的条目数：买卖盘各10档 + 各种价格和指标条目
    MDEntry md_entries[50];          // 预分配足够空间
    
    // 实际使用的行情条目数量
    uint16_t actual_entries_count;
    
    // 构造函数
    sz_stk_snapshot() : no_md_entries(0), actual_entries_count(0) {
        // 初始化所有条目
        for (int i = 0; i < 50; ++i) {
            md_entries[i] = MDEntry();
        }
    }
    
    // 添加行情条目
    bool add_md_entry(const MDEntry& entry) {
        if (actual_entries_count >= 50) {
            return false;  // 超出最大容量
        }
        md_entries[actual_entries_count] = entry;
        actual_entries_count++;
        no_md_entries = actual_entries_count;
        return true;
    }
    
    // 根据类型查找行情条目
    const MDEntry* find_entry_by_type(MDEntryType type) const {
        for (uint16_t i = 0; i < actual_entries_count; ++i) {
            if (md_entries[i].md_entry_type == type) {
                return &md_entries[i];
            }
        }
        return nullptr;
    }
    
    // 获取买盘档位数据 (type=0, level=1-10)
    const MDEntry* get_bid_level(uint8_t level) const {
        for (uint16_t i = 0; i < actual_entries_count; ++i) {
            if (md_entries[i].md_entry_type == MDEntryType::BID && 
                md_entries[i].md_price_level == level) {
                return &md_entries[i];
            }
        }
        return nullptr;
    }
    
    // 获取卖盘档位数据 (type=1, level=1-10)
    const MDEntry* get_ask_level(uint8_t level) const {
        for (uint16_t i = 0; i < actual_entries_count; ++i) {
            if (md_entries[i].md_entry_type == MDEntryType::ASK && 
                md_entries[i].md_price_level == level) {
                return &md_entries[i];
            }
        }
        return nullptr;
    }
    
    // 获取最近价
    double get_last_price() const {
        const MDEntry* entry = find_entry_by_type(MDEntryType::LAST_PRICE);
        return entry ? entry->md_entry_px : 0.0;
    }
    
    // 获取开盘价
    double get_open_price() const {
        const MDEntry* entry = find_entry_by_type(MDEntryType::OPEN_PRICE);
        return entry ? entry->md_entry_px : 0.0;
    }
    
    // 获取最高价
    double get_high_price() const {
        const MDEntry* entry = find_entry_by_type(MDEntryType::HIGH_PRICE);
        return entry ? entry->md_entry_px : 0.0;
    }
    
    // 获取最低价
    double get_low_price() const {
        const MDEntry* entry = find_entry_by_type(MDEntryType::LOW_PRICE);
        return entry ? entry->md_entry_px : 0.0;
    }
    
    // 获取涨停价
    double get_limit_up_price() const {
        const MDEntry* entry = find_entry_by_type(MDEntryType::LIMIT_UP);
        return entry ? entry->md_entry_px : 0.0;
    }
    
    // 获取跌停价
    double get_limit_down_price() const {
        const MDEntry* entry = find_entry_by_type(MDEntryType::LIMIT_DOWN);
        return entry ? entry->md_entry_px : 0.0;
    }
    
    // 获取升跌一 (最近价-昨收价)
    double get_price_change_1() const {
        const MDEntry* entry = find_entry_by_type(MDEntryType::PRICE_CHANGE_1);
        return entry ? entry->md_entry_px : 0.0;
    }
    
    // 获取升跌二 (最近价-上一最近价)
    double get_price_change_2() const {
        const MDEntry* entry = find_entry_by_type(MDEntryType::PRICE_CHANGE_2);
        return entry ? entry->md_entry_px : 0.0;
    }
    
    // 判断是否无涨停限制
    bool is_no_limit_up() const {
        double limit_up = get_limit_up_price();
        return limit_up >= 999999999.9999;
    }
    
    // 判断是否无跌停限制
    bool is_no_limit_down() const {
        double limit_down = get_limit_down_price();
        return limit_down <= -999999999.9999 || limit_down == 0.01;
    }
};
#pragma pack(pop)

// 实用函数

// 将字符转换为MDEntryType
inline MDEntryType char_to_md_entry_type(char c) {
    return static_cast<MDEntryType>(c);
}

// 将MDEntryType转换为字符
inline char md_entry_type_to_char(MDEntryType type) {
    return static_cast<char>(type);
}

// 获取MDEntryType的描述
inline const char* get_md_entry_type_description(MDEntryType type) {
    switch (type) {
        case MDEntryType::BID: return "买入";
        case MDEntryType::ASK: return "卖出";
        case MDEntryType::LAST_PRICE: return "最近价";
        case MDEntryType::OPEN_PRICE: return "开盘价";
        case MDEntryType::HIGH_PRICE: return "最高价";
        case MDEntryType::LOW_PRICE: return "最低价";
        case MDEntryType::PRICE_CHANGE_1: return "升跌一";
        case MDEntryType::PRICE_CHANGE_2: return "升跌二";
        case MDEntryType::BID_SUMMARY: return "买入汇总";
        case MDEntryType::ASK_SUMMARY: return "卖出汇总";
        case MDEntryType::PE_RATIO_1: return "股票市盈率一";
        case MDEntryType::PE_RATIO_2: return "股票市盈率二";
        case MDEntryType::FUND_NAV: return "基金T-1日净值";
        case MDEntryType::FUND_IOPV: return "基金实时参考净值";
        case MDEntryType::WARRANT_PREMIUM: return "权证溢价率";
        case MDEntryType::LIMIT_UP: return "涨停价";
        case MDEntryType::LIMIT_DOWN: return "跌停价";
        case MDEntryType::OPEN_INTEREST: return "合约持仓量";
        case MDEntryType::REF_PRICE: return "参考价";
        default: return "未知类型";
    }
}

} // namespace L2
