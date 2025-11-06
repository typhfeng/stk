#pragma once

#include "backend/FeatureStore.hpp"
#include "backend/FeatureStoreConfig.hpp"
#include "lob/LimitOrderBookDefine.hpp"
#include "math/normalize/RollingZScore.hpp"
#include <cmath>

// 核心理念: 在低信噪比、强竞争的二级市场，端到端的深度模型(数据先验)会先被淘汰, 特征工程因子挖掘(结构性先验)是生存条件，不是选择。

// ============================================================================
// LEVEL 0 FEATURES - Tick-level Computation
// ============================================================================

#define TICK_SIZE 0.01f
static constexpr int ZSCORE_WINDOW = 1800;

class FeaturesTick {
private:
  const LOB_Feature *lob_feature_;
  GlobalFeatureStore *global_store_;
  size_t asset_id_;

  // Time index tracking
  size_t last_time_idx_ = 0;

  RollingZScore<float, ZSCORE_WINDOW> zs_spread_;
  RollingZScore<float, ZSCORE_WINDOW> zs_tobi_;
  RollingZScore<float, ZSCORE_WINDOW> zs_mpg_;

public:
  FeaturesTick(const LOB_Feature *lob_feature)
      : lob_feature_(lob_feature),
        global_store_(nullptr),
        asset_id_(0) {}

  // Set store context (called once at LOB construction)
  void set_store_context(GlobalFeatureStore *store, size_t asset_id) {
    global_store_ = store;
    asset_id_ = asset_id;
  }

  // Compute and store - called every tick, handles time index internally
  void compute_and_store() {
    if (!lob_feature_->depth_updated || !global_store_ || lob_feature_->depth_buffer.size() < 2 * LOB_FEATURE_DEPTH_LEVELS) [[unlikely]] {
      return;
    }

    const size_t curr_time_idx = time_to_index(L0_INDEX, lob_feature_->hour, lob_feature_->minute,
                                               lob_feature_->second, lob_feature_->millisecond);

    // Only compute on time index change
    if (curr_time_idx == last_time_idx_) {
      return;
    }
    last_time_idx_ = curr_time_idx;

    const auto &depth_buffer = lob_feature_->depth_buffer;
    if (depth_buffer.size() < 2 * LOB_FEATURE_DEPTH_LEVELS) [[unlikely]] {
      return;
    }

    // Extract LOB data
    const Level *best_ask_level = depth_buffer[LOB_FEATURE_DEPTH_LEVELS - 1];
    const Level *best_bid_level = depth_buffer[LOB_FEATURE_DEPTH_LEVELS];

    const float best_bid_price = static_cast<float>(best_bid_level->price) * 0.01f;
    const float best_ask_price = static_cast<float>(best_ask_level->price) * 0.01f;
    const float best_bid_volume = static_cast<float>(std::abs(best_bid_level->net_quantity));
    const float best_ask_volume = static_cast<float>(std::abs(best_ask_level->net_quantity));

    // Compute all features
    Level0Data data = {};
    data.timestamp = 0; // TODO

    data.mid_price = (best_bid_price + best_ask_price) * 0.5f;
    data.spread = best_ask_price - best_bid_price;
    data.spread_z = zs_spread_.update(data.spread);

    const float tobi_denom = best_bid_volume + best_ask_volume;
    data.tobi = (tobi_denom > 0.0f) ? (best_bid_volume - best_ask_volume) / tobi_denom : 0.0f;
    data.tobi_z = zs_tobi_.update(data.tobi);

    const float denom = best_bid_volume + best_ask_volume;
    data.micro_price = (denom > 0.0f)
                           ? ((best_ask_price * best_bid_volume + best_bid_price * best_ask_volume) / denom)
                           : data.mid_price;
    data.mpg = data.micro_price - data.mid_price;
    data.mpg_z = zs_mpg_.update(data.mpg);

    // Push with explicit time_idx
    global_store_->push(L0_INDEX, asset_id_, curr_time_idx, &data);
  }
};




// Feature class level 1 name mapping
struct FeatureClass1Name {
  std::string_view name_cn;
  std::string_view name_en;
};

// Feature class level 2 name mapping
struct FeatureClass2Name {
  std::string_view name_cn;
  std::string_view name_en;
};

struct FeaturesTickMeta {
  std::string_view code;         // 特征简写
  std::string_view name_cn;      // 特征名称(中文)
  std::string_view name_en;      // 特征名称(英文)
  std::string_view class_level1; // 一级特征类型简写
  std::string_view class_level2; // 二级特征类型简写
  std::string_view description;  // 描述
  std::string_view formula;      // 公式
};

// clang-format off
constexpr std::pair<std::string_view, FeatureClass1Name> FeatureClass1Map[] = {
  {"SD", {"深度结构特征",           "Structural Depth Features"}},
  {"DF", {"订单流动态特征",         "Dynamic Order Flow Features"}},
  {"BH", {"行为与策略特征",         "Behavioral & Strategic Features"}},
  {"CD", {"事件聚集与依赖特征",     "Clustering & Dependency Features"}},
  {"RS", {"韧性与恢复特征",         "Resiliency & Replenishment Features"}},
  {"IC", {"价格冲击与流动性成本",   "Impact & Liquidity Cost Features"}},
  {"AN", {"异常与结构失衡特征",     "Anomaly & Structural Outlier Features"}},
};

constexpr std::pair<std::string_view, FeatureClass2Name> FeatureClass2Map[] = {
  // SD - Structural Depth Features (深度结构特征)
  {"SD_IMB",  {"深度失衡",         "Depth Imbalance"}},
  {"SD_RAT",  {"深度比率",         "Depth Ratio"}},
  {"SD_CVX",  {"深度凸性",         "Depth Convexity"}},
  {"SD_SLP",  {"深度斜率",         "Depth Slope"}},
  {"SD_ENT",  {"深度熵",           "Depth Entropy"}},
  
  // DF - Dynamic Order Flow Features (订单流动态特征)
  {"DF_ARR",  {"订单到达率",       "Order Arrival Rate"}},
  {"DF_CAN",  {"撤单率",           "Cancellation Rate"}},
  {"DF_TRD",  {"成交流率",         "Trade Flow Rate"}},
  {"DF_NET",  {"净订单流",         "Net Order Flow"}},
  {"DF_FIM",  {"订单流失衡",       "Flow Imbalance"}},
  
  // BH - Behavioral & Strategic Features (行为与策略特征)
  {"BH_AGG",  {"订单侵略性",       "Order Aggressiveness"}},
  {"BH_PAT",  {"撤单模式",         "Cancellation Pattern"}},
  {"BH_SPO",  {"欺骗性行为",       "Spoofing / Fleeting"}},
  {"BH_TRN",  {"行为趋势",         "Behavior Trend"}},
  {"BH_VOL",  {"单笔特征",         "Order-level Traits"}},
  
  // CD - Clustering & Dependency Features (事件聚集与依赖特征)
  {"CD_TMP",  {"时间聚集性",       "Temporal Clustering"}},
  {"CD_CCL",  {"撤单聚集",         "Cancel Clustering"}},
  {"CD_OCL",  {"大单聚集",         "Large Order Clustering"}},
  {"CD_JNT",  {"多事件依赖",       "Multi-event Dependency"}},
  {"CD_AUT",  {"自相关性",         "Autocorrelation Index"}},
  
  // RS - Resiliency & Replenishment Features (韧性与恢复特征)
  {"RS_SPD",  {"恢复速度",         "Recovery Speed"}},
  {"RS_VOL",  {"恢复量",           "Replenish Volume"}},
  {"RS_DIR",  {"恢复方向",         "Recovery Direction Bias"}},
  {"RS_RAT",  {"韧性比率",         "Resiliency Ratio"}},
  {"RS_MUL",  {"多次冲击韧性",     "Multi-impact Resiliency"}},
  
  // IC - Impact & Liquidity Cost Features (价格冲击与流动性成本特征)
  {"IC_SIM",  {"模拟冲击成本",     "Simulated Impact Cost"}},
  {"IC_EFC",  {"有效成本",         "Effective Cost"}},
  {"IC_VTP",  {"量价关系",         "Volume-to-Price Relation"}},
  {"IC_LDR",  {"流动性消耗率",     "Liquidity Depletion Rate"}},
  {"IC_TLD",  {"吃单深度",         "Take Liquidity Depth"}},
  
  // AN - Anomaly & Structural Outlier Features (异常与结构失衡特征)
  {"AN_EXT",  {"极端档异常",       "Extreme Level Anomaly"}},
  {"AN_EVN",  {"异常事件",         "Event Outlier"}},
  {"AN_SHK",  {"冲击异常",         "Shock Anomaly"}},
  {"AN_PAT",  {"异常模式",         "Abnormal Pattern"}},
  {"AN_LOC",  {"局部异常",         "Local Depth Outlier"}},
};

constexpr FeaturesTickMeta FeaturesTick_Schema[] = {
//  // ========== SD: Structural Depth Features (深度结构特征) ==========
//  {"sd_tobi",     "顶层失衡",            "Top-of-book Imbalance",                   "SD", "SD_IMB", "买一卖一量失衡",                     "(V_bid1 - V_ask1) / (V_bid1 + V_ask1)"},
//  {"sd_cwi_1",    "凸加权失衡γ=1",       "Convexity-weighted Imb γ=1",              "SD", "SD_IMB", "凸加权多层失衡，γ=1",                "Σw_i*(V_bid-V_ask)/Σw_i*(V_bid+V_ask), w=1/i^γ"},
//  {"sd_cwi_2",    "凸加权失衡γ=2",       "Convexity-weighted Imb γ=2",              "SD", "SD_IMB", "凸加权多层失衡，γ=2",                "Σw_i*(V_bid-V_ask)/Σw_i*(V_bid+V_ask), w=1/i^γ"},
//  {"sd_ddi_1",    "距离折扣失衡λ=0.01",  "Distance-discounted Imb λ=0.01",          "SD", "SD_IMB", "距离折扣多层失衡，λ=0.01",           "Σe^(-λΔp)*(V_bid-V_ask)/Σe^(-λΔp)*(V_bid+V_ask)"},
//  {"sd_ddi_2",    "距离折扣失衡λ=0.05",  "Distance-discounted Imb λ=0.05",          "SD", "SD_IMB", "距离折扣多层失衡，λ=0.05",           "Σe^(-λΔp)*(V_bid-V_ask)/Σe^(-λΔp)*(V_bid+V_ask)"},
//  {"sd_cum_rat",  "累计深度比",          "Cumulative Depth Ratio",                  "SD", "SD_RAT", "前N档累计深度占比",                  "Σ(V_1..N) / Σ(V_all)"},
//  {"sd_adj_rat",  "相邻档深度比",        "Adjacent Level Depth Ratio",              "SD", "SD_RAT", "相邻档位深度比值",                   "V_i / V_{i+1}"},
//  {"sd_cvx_bid",  "买侧深度凸性",        "Bid Depth Convexity",                     "SD", "SD_CVX", "买侧深度二阶导数",                   "c2 from V~c0+c1*i+c2*i^2"},
//  {"sd_cvx_ask",  "卖侧深度凸性",        "Ask Depth Convexity",                     "SD", "SD_CVX", "卖侧深度二阶导数",                   "c2 from V~c0+c1*i+c2*i^2"},
//  {"sd_slp_bid",  "买侧深度斜率",        "Bid Depth Slope",                         "SD", "SD_SLP", "买侧深度梯度",                       "ΔV_bid / Δlevel"},
//  {"sd_slp_ask",  "卖侧深度斜率",        "Ask Depth Slope",                         "SD", "SD_SLP", "卖侧深度梯度",                       "ΔV_ask / Δlevel"},
//  {"sd_ent_bid",  "买侧深度熵",          "Bid Depth Entropy",                       "SD", "SD_ENT", "买侧深度分布熵",                     "H=-Σπ_i*log(π_i), π=V_i/ΣV"},
//  {"sd_ent_ask",  "卖侧深度熵",          "Ask Depth Entropy",                       "SD", "SD_ENT", "卖侧深度分布熵",                     "H=-Σπ_i*log(π_i), π=V_i/ΣV"},
//  
//  // ========== DF: Dynamic Order Flow Features (订单流动态特征) ==========
//  {"df_arr_bid",  "买单到达率",          "Bid Order Arrival Rate",                  "DF", "DF_ARR", "单位时间买单新增数",                 "count(new_bid_orders) / Δt"},
//  {"df_arr_ask",  "卖单到达率",          "Ask Order Arrival Rate",                  "DF", "DF_ARR", "单位时间卖单新增数",                 "count(new_ask_orders) / Δt"},
//  {"df_can_bid",  "买单撤单率",          "Bid Cancellation Rate",                   "DF", "DF_CAN", "单位时间买单撤销数",                 "count(cancel_bid) / Δt"},
//  {"df_can_ask",  "卖单撤单率",          "Ask Cancellation Rate",                   "DF", "DF_CAN", "单位时间卖单撤销数",                 "count(cancel_ask) / Δt"},
//  {"df_trd_buy",  "主动买成交率",        "Active Buy Trade Rate",                   "DF", "DF_TRD", "单位时间主动买成交数",               "count(active_buy_trade) / Δt"},
//  {"df_trd_sell", "主动卖成交率",        "Active Sell Trade Rate",                  "DF", "DF_TRD", "单位时间主动卖成交数",               "count(active_sell_trade) / Δt"},
//  {"df_net_ord",  "净订单流",            "Net Order Flow",                          "DF", "DF_NET", "买卖订单流净差",                     "(new_bid - cancel_bid) - (new_ask - cancel_ask)"},
//  {"df_foi",      "订单流失衡",          "Flow Imbalance",                          "DF", "DF_FIM", "买卖订单流不平衡度",                 "(flow_bid - flow_ask) / (flow_bid + flow_ask)"},
//  
//  // ========== BH: Behavioral & Strategic Features (行为与策略特征) ==========
//  {"bh_agg_buy",  "买单平均侵略性",      "Mean Buy Aggressiveness",                 "BH", "BH_AGG", "买单距最优价距离",                   "mean(log(best_bid/order_price))"},
//  {"bh_agg_sell", "卖单平均侵略性",      "Mean Sell Aggressiveness",                "BH", "BH_AGG", "卖单距最优价距离",                   "mean(log(order_price/best_ask))"},
//  {"bh_agg_dif",  "多空侵略性差",        "Buy-Sell Aggressiveness Delta",           "BH", "BH_AGG", "买卖侵略性差",                       "agg_buy - agg_sell"},
//  {"bh_cpr",      "撤挂比",              "Cancel-to-Post Ratio",                    "BH", "BH_PAT", "撤单量/挂单量",                      "cancel_vol / post_vol"},
//  {"bh_ptc_rt",   "成交前撤单比",        "Pre-trade Cancel Ratio",                  "BH", "BH_PAT", "成交前撤单比率",                     "pre_trade_cancel / trade"},
//  {"bh_fleet_rt", "闪单占比",            "Fleeting Order Ratio",                    "BH", "BH_SPO", "极短时间(<50ms)挂撤占比",            "fleeting_vol / total_vol"},
//  {"bh_spoof",    "欺骗标识",            "Spoofing Flag",                           "BH", "BH_SPO", "识别欺骗性挂单",                     "1{vol↑ + cancel↑}"},
//  {"bh_agg_trd",  "侵略性趋势",          "Aggressiveness Trend",                    "BH", "BH_TRN", "侵略性时间序列斜率",                 "slope(aggressiveness_t)"},
//  {"bh_ord_size", "平均单笔规模",        "Mean Order Size",                         "BH", "BH_VOL", "单笔订单平均规模",                   "mean(order_size)"},
//  
//  // ========== CD: Clustering & Dependency Features (事件聚集与依赖特征) ==========
//  {"cd_can_clst", "撤单聚集",            "Cancel Clustering",                       "CD", "CD_CCL", "单位时间撤单事件密度",               "cancel_event_count(1s)"},
//  {"cd_ord_clst", "大单聚集",            "Large Order Clustering",                  "CD", "CD_OCL", "单位时间大挂单事件密度",             "large_order_event_rate"},
//  {"cd_evt_idx",  "高频事件指数",        "High-frequency Event Index",              "CD", "CD_TMP", "异常行为单位时间指数",               "event_rate"},
//  {"cd_multi",    "多事件联合聚集",      "Multi-event Clustering",                  "CD", "CD_JNT", "多事件同时发生评分",                 "multi_event_score"},
//  {"cd_autocor",  "事件自相关",          "Event Autocorrelation",                   "CD", "CD_AUT", "事件时间序列自相关",                 "corr(event_t, event_{t-lag})"},
//  
//  // ========== RS: Resiliency & Replenishment Features (韧性与恢复特征) ==========
//  {"rs_rpl_vol",  "恢复量",              "Replenish Volume",                        "RS", "RS_VOL", "冲击后回补深度",                     "replenish_vol - removed_vol"},
//  {"rs_ratio",    "韧性比率",            "Resiliency Ratio",                        "RS", "RS_RAT", "恢复量/冲击量",                      "replenish_vol / removed_vol"},
//  {"rs_speed",    "恢复速度",            "Recovery Speed",                          "RS", "RS_SPD", "达到80%恢复所需时间",                "t_80%_recovery"},
//  {"rs_dir_bid",  "买侧恢复倾向",        "Bid Recovery Bias",                       "RS", "RS_DIR", "买侧恢复速度",                       "bid_recovery_rate"},
//  {"rs_dir_ask",  "卖侧恢复倾向",        "Ask Recovery Bias",                       "RS", "RS_DIR", "卖侧恢复速度",                       "ask_recovery_rate"},
//  {"rs_multi",    "多次冲击韧性",        "Multi-impact Resiliency",                 "RS", "RS_MUL", "连续冲击韧性演化",                   "resiliency_decay_curve"},
//  
//  // ========== IC: Impact & Liquidity Cost Features (价格冲击与流动性成本) ==========
//  {"ic_sim_buy",  "买方模拟冲击成本",    "Simulated Buy Impact Cost",               "IC", "IC_SIM", "模拟吃N档卖单成本",                  "Σ(P_i*V_i) - mid*ΣV_i"},
//  {"ic_sim_sell", "卖方模拟冲击成本",    "Simulated Sell Impact Cost",              "IC", "IC_SIM", "模拟吃N档买单成本",                  "mid*ΣV_i - Σ(P_i*V_i)"},
//  {"ic_vtp",      "量价弹性",            "Volume-to-Price Elasticity",              "IC", "IC_VTP", "执行X量需要的价格移动",              "ΔP / ΔV"},
//  {"ic_ldr_bid",  "买侧流动性消耗率",    "Bid Liquidity Depletion Rate",            "IC", "IC_LDR", "买侧深度消耗速度",                   "ΔV_bid / Δt"},
//  {"ic_ldr_ask",  "卖侧流动性消耗率",    "Ask Liquidity Depletion Rate",            "IC", "IC_LDR", "卖侧深度消耗速度",                   "ΔV_ask / Δt"},
//  {"ic_tld",      "吃单深度",            "Take Liquidity Depth",                    "IC", "IC_TLD", "吃掉X%深度所需层数",                 "levels_to_consume_pct"},
//  
//  // ========== AN: Anomaly & Structural Outlier Features (异常与结构失衡特征) ==========
//  {"an_ext_bid",  "买侧极端档异常",      "Bid Extreme Level Anomaly",               "AN", "AN_EXT", "远离买一的异常挂单",                 "vol_bid_at_dist_N / mean(vol)"},
//  {"an_ext_ask",  "卖侧极端档异常",      "Ask Extreme Level Anomaly",               "AN", "AN_EXT", "远离卖一的异常挂单",                 "vol_ask_at_dist_N / mean(vol)"},
//  {"an_hump_bid", "买侧峰位置异常",      "Bid Hump Location Anomaly",               "AN", "AN_LOC", "买侧挂单峰位置偏离",                 "argmax(V_bid) deviation"},
//  {"an_hump_ask", "卖侧峰位置异常",      "Ask Hump Location Anomaly",               "AN", "AN_LOC", "卖侧挂单峰位置偏离",                 "argmax(V_ask) deviation"},
//  {"an_stale",    "无成交大挂单",        "Stale Large Order",                       "AN", "AN_EVN", "长时间未成交挂单",                   "stale_order_vol"},
//  {"an_block",    "异常密集挂单",        "Blockage Volume",                         "AN", "AN_PAT", "偏离市价的集中挂单",                 "blockage_at_price(p)"},
//  {"an_spike",    "深度突变",            "Depth Spike",                             "AN", "AN_SHK", "深度异常突变",                       "ΔV / mean(V) > threshold"},
};
// clang-format on
