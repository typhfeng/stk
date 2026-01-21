#pragma once

// =============================================================================
// BuyMood - 买入情绪因子 (基于长江证券研报)
// =============================================================================
// 区分积极买入与保守买入，构建买入情绪因子
//
// 积极买入 (Aggressive Buy):
//   - 投资者下单主动与盘口卖盘挂单成交
//   - 判断: 当前成交价 >= 前一条tick的卖一价
//
// 保守买入 (Passive Buy):
//   - 投资者所下限价订单挂单等待后续卖单与之成交
//   - 判断: 当前成交价 <= 前一条tick的买一价
//
// BM因子 = 保守买入量 / 积极买入量
//
// 参考: 长江证券-金工高频识途系列（一）：基于买入行为构建情绪因子
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"
#include "features/DataDefine.hpp"

class BuyMood {
public:
  BuyMood(const TickData &tick_data,
          const CBuffer<float, L2::BLEN> &bid_price_0,
          const CBuffer<float, L2::BLEN> &ask_price_0,
          CBuffer<float, L2::BLEN> &aggressive_buy_vol,
          CBuffer<float, L2::BLEN> &passive_buy_vol,
          CBuffer<float, L2::BLEN> &buy_mood_ratio)
      : tick_data_(tick_data),
        bid_price_0_(bid_price_0),
        ask_price_0_(ask_price_0),
        aggressive_buy_vol_(aggressive_buy_vol),
        passive_buy_vol_(passive_buy_vol),
        buy_mood_ratio_(buy_mood_ratio) {
    // 初始化前一个tick的价格
    prev_bid_price_ = 0.0f;
    prev_ask_price_ = 0.0f;
    
    // 初始化累计量（用于计算BM比率）
    cumulative_aggressive_buy_ = 0.0f;
    cumulative_passive_buy_ = 0.0f;
  }

  void compute() {
    float aggressive_vol = 0.0f;
    float passive_vol = 0.0f;
    float bm_ratio = 0.0f;

    // 只在TAKER订单（成交）时计算买入情绪
    if (tick_data_.lob.order_type == L2::OrderType::TAKER) {
      float current_trade_price = tick_data_.lob.price;
      float current_volume = static_cast<float>(tick_data_.lob.volume);

      // 需要前一个tick的价格来判断，如果还没有前一个tick，跳过
      if (prev_bid_price_ > 1e-6f && prev_ask_price_ > 1e-6f) {
        // 积极买入: 当前成交价 >= 前一个tick的卖一价
        // 说明主动与卖盘挂单成交
        if (current_trade_price >= prev_ask_price_) {
          aggressive_vol = current_volume;
          cumulative_aggressive_buy_ += aggressive_vol;
        }
        
        // 保守买入: 当前成交价 <= 前一个tick的买一价
        // 说明限价挂单等待成交
        if (current_trade_price <= prev_bid_price_) {
          passive_vol = current_volume;
          cumulative_passive_buy_ += passive_vol;
        }
      }

      // 计算BM比率（保守买入量/积极买入量）
      if (cumulative_aggressive_buy_ > 1e-6f) {
        bm_ratio = cumulative_passive_buy_ / cumulative_aggressive_buy_;
      } else if (cumulative_passive_buy_ > 1e-6f) {
        // 如果只有保守买入，没有积极买入，设为较大值
        bm_ratio = 100.0f; // 或使用其他标记值
      }
    }

    aggressive_buy_vol_.push_back(aggressive_vol);
    passive_buy_vol_.push_back(passive_vol);
    buy_mood_ratio_.push_back(bm_ratio);

    // 更新前一个tick的价格（用于下一个tick的判断）
    // 注意：这里使用当前tick的买一卖一价，作为下一个tick的"前一个tick"价格
    if (bid_price_0_.size() > 0 && bid_price_0_.back() > 1e-6f) {
      prev_bid_price_ = bid_price_0_.back();
    }
    if (ask_price_0_.size() > 0 && ask_price_0_.back() > 1e-6f) {
      prev_ask_price_ = ask_price_0_.back();
    }
  }

  // 重置累计量（跨天时调用）
  void reset() {
    cumulative_aggressive_buy_ = 0.0f;
    cumulative_passive_buy_ = 0.0f;
    prev_bid_price_ = 0.0f;
    prev_ask_price_ = 0.0f;
  }

private:
  const TickData &tick_data_;
  const CBuffer<float, L2::BLEN> &bid_price_0_;
  const CBuffer<float, L2::BLEN> &ask_price_0_;
  CBuffer<float, L2::BLEN> &aggressive_buy_vol_;
  CBuffer<float, L2::BLEN> &passive_buy_vol_;
  CBuffer<float, L2::BLEN> &buy_mood_ratio_;

  // 前一个tick的价格（用于判断买入类型）
  float prev_bid_price_;
  float prev_ask_price_;

  // 累计量（用于计算BM比率）
  float cumulative_aggressive_buy_;
  float cumulative_passive_buy_;
};
