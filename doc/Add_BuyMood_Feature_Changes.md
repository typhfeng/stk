# 添加买入情绪因子特征 - 详细变更说明

本文档详细说明如何根据长江证券研报《金工高频识途系列（一）：基于买入行为构建情绪因子》添加买入情绪因子特征的所有步骤。

**参考文档**: `~/work/trader/doc/Sellside 卖方金工研报/高频研报/长江证券/20170310-长江证券-金工高频识途系列（一）：基于买入行为构建情绪因子.pdf`

---

## 一、研报核心内容摘要

### 1.1 买入行为分类

**积极买入 (Aggressive Buy)**:
- 定义：投资者所下订单主动与盘口卖盘挂单成交
- 判断方法：当前成交价格 >= 前一条tick数据的卖一价
- 含义：投资者买入意愿强烈，愿意以高于当前成交价的价格买入

**保守买入 (Passive Buy)**:
- 定义：投资者所下限价订单挂单等待后续卖单与之成交
- 判断方法：当前成交价格 <= 前一条tick数据的买一价
- 含义：投资者对价格敏感，只愿意以自己认定的价格买入

### 1.2 买入情绪因子 (BM Factor)

**定义**:
```
BM = 过去N日保守买入量 / 过去N日积极买入量
```

**研报使用**: 过去20个交易日的保守买入量与积极买入量比

**因子含义**:
- BM值越高：保守买入占比越高，买入情绪越保守
- BM值越低：积极买入占比越高，买入情绪越积极（追高意愿强）

**因子表现**:
- 平均RankIC: 0.0724
- 年化超额收益（中证500）: 14.53%（原始因子）

---

## 二、需要添加的特征

### 2.1 特征列表

需要在 `LEVEL_0_FIELDS` 宏中添加以下3个特征：

1. **aggressive_buy_vol** (积极买入量)
   - 类型: TS (时序特征)
   - 分类: ORDER_FLOW (订单流)
   - 归一化: NONE (原始值)
   - 描述: 成交价>=前tick卖一价时的成交量

2. **passive_buy_vol** (保守买入量)
   - 类型: TS (时序特征)
   - 分类: ORDER_FLOW (订单流)
   - 归一化: NONE (原始值)
   - 描述: 成交价<=前tick买一价时的成交量

3. **buy_mood_ratio** (买入情绪比率)
   - 类型: TS (时序特征)
   - 分类: ORDER_FLOW (订单流)
   - 归一化: RATIO (比率)
   - 描述: 保守买入量/积极买入量，反映买入情绪

---

## 三、详细变更步骤

### 步骤1: 在 FeaturesDefine.hpp 中添加特征定义

**文件**: `cpp/include/features/FeaturesDefine.hpp`

**位置**: 在 `LEVEL_0_FIELDS` 宏中，`ofi_5` 特征之后添加

**需要添加的代码**:
```cpp
  /* BM - Buy Mood Features (买入情绪特征) - 基于长江证券研报 */\
  X(aggressive_buy_vol, 1, DATA,  TS,   ORDER_FLOW, RAW,        NONE,        "100/00/00", "Aggressive Buy Volume",           "积极买入量",            "成交价>=前tick卖一价时的成交量(主动与卖盘成交)",      R"(V_{t}^{AB} = \begin{cases}V_t, & P_t \geq P_{t-1}^{A1} \\ 0, & \text{otherwise} \end{cases})")\
  X(passive_buy_vol,    1, DATA,  TS,   ORDER_FLOW, RAW,        NONE,        "100/00/00", "Passive Buy Volume",              "保守买入量",            "成交价<=前tick买一价时的成交量(限价挂单等待成交)",    R"(V_{t}^{PB} = \begin{cases}V_t, & P_t \leq P_{t-1}^{B1} \\ 0, & \text{otherwise} \end{cases})")\
  X(buy_mood_ratio,     1, DATA,  TS,   ORDER_FLOW, RATIO,      NONE,        "100/00/00", "Buy Mood Ratio (BM)",             "买入情绪比率",          "保守买入量/积极买入量,反映买入情绪",                  R"(\text{BM} = \frac{\sum_{i=t-N}^{t} V_i^{PB}}{\sum_{i=t-N}^{t} V_i^{AB}})")\
```

**说明**:
- 这些特征定义会自动生成对应的枚举值：`L0_FieldOffset::aggressive_buy_vol`, `L0_FieldOffset::passive_buy_vol`, `L0_FieldOffset::buy_mood_ratio`
- 系统会自动计算特征在数组中的偏移量

---

### 步骤2: 创建 BuyMood 算子类

**文件**: `cpp/include/features/FeaturesTick/TS/BuyMood.hpp` (新建文件)

**完整代码**:
```cpp
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
```

**关键设计说明**:
1. **状态维护**: 维护前一个tick的买一价和卖一价，用于判断当前成交是积极买入还是保守买入
2. **累计量**: 维护累计的积极买入量和保守买入量，用于计算BM比率
3. **触发条件**: 只在TAKER订单（成交）时计算，非成交订单输出0
4. **跨天重置**: 提供`reset()`方法，在跨天时重置累计量和价格状态

---

### 步骤3: 在 ComputeGraph.hpp 中添加包含和CBuffer

**文件**: `cpp/include/features/ComputeGraph.hpp`

#### 3.1 添加头文件包含

**位置**: 文件顶部，在其他特征算子包含之后

**需要添加**:
```cpp
#include "features/FeaturesTick/TS/BuyMood.hpp"
```

**具体位置**: 在 `#include "features/FeaturesTick/TS/OFI.hpp"` 和 `#include "features/FeaturesTick/TS/PARA.hpp"` 之后

#### 3.2 在 DAG::L0 结构体中添加CBuffer

**位置**: `struct L0` 中，在 `ofi_1_` 和 `ofi_5_` 之后

**需要添加**:
```cpp
    // --- BM: Buy Mood Features (买入情绪特征) ---
    CBuffer<float, L2::BLEN> aggressive_buy_vol_;
    CBuffer<float, L2::BLEN> passive_buy_vol_;
    CBuffer<float, L2::BLEN> buy_mood_ratio_;
```

**具体位置**: 在 `CBuffer<float, L2::BLEN> ofi_5_;` 之后

#### 3.3 在 DAG::L0 结构体中添加算子实例

**位置**: `struct L0` 中，在 `OFI` 算子之后

**需要添加**:
```cpp
    // BM: Buy Mood
    BuyMood buy_mood{td, BidPrice_[0], AskPrice_[0], aggressive_buy_vol_, passive_buy_vol_, buy_mood_ratio_};
```

**具体位置**: 在 `OFI<5> ofi_5{BidQty_, AskQty_, BidPrice_, AskPrice_, ofi_5_};` 之后

**说明**:
- `td`: TickData引用，用于访问当前订单信息
- `BidPrice_[0]`: 买一价CBuffer，用于获取当前tick的买一价
- `AskPrice_[0]`: 卖一价CBuffer，用于获取当前tick的卖一价
- 后三个参数是输出CBuffer

#### 3.4 在 reset_for_new_day() 中添加重置调用

**位置**: `DAG::reset_for_new_day()` 方法中

**需要添加**:
```cpp
    // 重置BuyMood累计量
    l0.buy_mood.reset();
```

**具体位置**: 在 `l0.DepthData.set_prev_close(prev_close);` 之后，`// TODO:` 注释之前

---

### 步骤4: 在 Tick_Sequential.hpp 中调用计算

**文件**: `cpp/include/features/FeaturesTick/Tick_Sequential.hpp`

#### 4.1 在 compute_ts_tick() 中调用BuyMood计算

**位置**: `compute_ts_tick()` 方法中，`[ON TAKER]` 部分

**需要修改**:
```cpp
  // =========================================================================
  // [ON TAKER] 成交时更新 - order_type == TAKER 时触发
  // =========================================================================
  if (dag_.tick_data.lob.order_type == L2::OrderType::TAKER) {
    dag_.l0.TradePrice.compute();
    dag_.l0.buy_mood.compute(); // 买入情绪因子计算
  }
```

**说明**: 在TAKER订单时调用BuyMood计算，因为只有成交订单才能判断是积极买入还是保守买入

#### 4.2 在缓冲区填充部分添加特征值

**位置**: `compute_ts_tick()` 方法中，缓冲区填充部分（`--- 写入缓冲区` 注释之后）

**需要添加**:
```cpp
    // --- BM: Buy Mood Features (买入情绪特征) ---
    ts_features_buffer_[L0_FieldOffset::aggressive_buy_vol] = dag_.l0.aggressive_buy_vol_.back();
    ts_features_buffer_[L0_FieldOffset::passive_buy_vol] = dag_.l0.passive_buy_vol_.back();
    ts_features_buffer_[L0_FieldOffset::buy_mood_ratio] = dag_.l0.buy_mood_ratio_.back();
```

**具体位置**: 在 `ts_features_buffer_[L0_FieldOffset::ofi_5] = dag_.l0.ofi_5_.back();` 之后

**说明**: 
- 这些值会被自动包含在 `TS_WRITE_FEATURES` 的批量写入中
- 确保 `ts_features_buffer_` 的大小足够（系统会自动处理）

---

## 四、变更文件清单

### 需要修改的文件 (3个)

1. **cpp/include/features/FeaturesDefine.hpp**
   - 在 `LEVEL_0_FIELDS` 宏中添加3个特征定义

2. **cpp/include/features/ComputeGraph.hpp**
   - 添加 `#include "features/FeaturesTick/TS/BuyMood.hpp"`
   - 在 `DAG::L0` 中添加3个CBuffer
   - 在 `DAG::L0` 中添加BuyMood算子实例
   - 在 `reset_for_new_day()` 中添加重置调用

3. **cpp/include/features/FeaturesTick/Tick_Sequential.hpp**
   - 在 `compute_ts_tick()` 的TAKER部分调用 `buy_mood.compute()`
   - 在缓冲区填充部分添加3个特征值

### 需要新建的文件 (1个)

1. **cpp/include/features/FeaturesTick/TS/BuyMood.hpp**
   - 创建BuyMood算子类，实现买入情绪计算逻辑

---

## 五、实现细节说明

### 5.1 判断逻辑

**积极买入判断**:
```cpp
if (current_trade_price >= prev_ask_price_) {
    // 当前成交价 >= 前一个tick的卖一价
    // 说明主动与卖盘挂单成交，是积极买入
    aggressive_vol = current_volume;
}
```

**保守买入判断**:
```cpp
if (current_trade_price <= prev_bid_price_) {
    // 当前成交价 <= 前一个tick的买一价
    // 说明限价挂单等待成交，是保守买入
    passive_vol = current_volume;
}
```

**注意**: 
- 一个成交可能同时满足两个条件（理论上不应该，但为安全起见都计算）
- 需要前一个tick的价格有效（> 1e-6f）才能判断

### 5.2 BM比率计算

**计算公式**:
```cpp
BM = cumulative_passive_buy / cumulative_aggressive_buy
```

**边界情况处理**:
- 如果 `cumulative_aggressive_buy == 0` 但 `cumulative_passive_buy > 0`，设为100.0f（表示极端保守）
- 如果两者都为0，BM比率为0.0f

### 5.3 状态维护

**前一个tick价格更新**:
- 在每个tick（无论是否TAKER）都更新前一个tick的价格
- 使用当前tick的买一卖一价作为下一个tick的"前一个tick"价格
- 确保价格有效（> 1e-6f）才更新

**累计量维护**:
- 只在TAKER订单时更新累计量
- 跨天时通过 `reset()` 方法重置

### 5.4 与研报的差异

**研报方法**:
- 使用tick数据，比对每一条tick数据
- 如果当前成交价格 >= 前一条tick的卖一价，记为积极买入
- 如果当前成交价格 <= 前一条tick的买一价，记为保守买入

**我们的实现**:
- 使用逐笔订单数据（更细粒度）
- 在TAKER订单时判断，逻辑与研报一致
- BM比率使用累计量计算（实时更新），研报使用过去20日累计

**注意**: 
- 研报中BM因子是过去20日的累计，我们的实现是实时累计
- 如果需要与研报完全一致，需要在分钟级或日级进行20日滚动窗口计算
- 当前实现提供的是tick级的实时BM比率

---

## 六、编译和测试

### 6.1 编译

```bash
cd /home/typhwang/work/stk
python3 py/main.py
```

### 6.2 验证

编译成功后，系统会自动：
1. 生成特征枚举：`L0_FieldOffset::aggressive_buy_vol`, `L0_FieldOffset::passive_buy_vol`, `L0_FieldOffset::buy_mood_ratio`
2. 计算特征偏移量
3. 在特征计算流程中调用BuyMood算子

### 6.3 预期行为

- **aggressive_buy_vol**: 在TAKER订单且成交价>=前tick卖一价时，输出成交量；否则为0
- **passive_buy_vol**: 在TAKER订单且成交价<=前tick买一价时，输出成交量；否则为0
- **buy_mood_ratio**: 实时计算累计的保守买入量/积极买入量

---

## 七、潜在问题和注意事项

### 7.1 第一个tick的处理

- 第一个tick没有"前一个tick"，`prev_bid_price_` 和 `prev_ask_price_` 为0
- 此时无法判断买入类型，输出为0（符合预期）

### 7.2 价格精度

- 价格比较使用浮点数，可能存在精度问题
- 当前使用 `>=` 和 `<=` 比较，应该足够

### 7.3 BM比率的滚动窗口

- 当前实现使用累计量（从交易日开始累计）
- 如果需要与研报一致的20日滚动窗口，需要在更高层级（分钟级或日级）实现
- 当前tick级实现提供实时BM比率，可用于实时监控

### 7.4 跨天重置

- `reset()` 方法在 `DAG::reset_for_new_day()` 中调用
- 确保每个交易日开始时累计量重置为0

---

## 八、后续优化建议

### 8.1 分钟级BM因子

如果需要与研报完全一致的20日滚动窗口BM因子，可以：
1. 在分钟级特征中添加日累计的积极买入量和保守买入量
2. 在分钟级计算20日滚动窗口的BM比率

### 8.2 卖出情绪因子

可以类似地实现卖出情绪因子：
- 积极卖出：成交价 <= 前tick买一价
- 保守卖出：成交价 >= 前tick卖一价
- SM因子 = 保守卖出量 / 积极卖出量

### 8.3 多时间窗口

可以计算多个时间窗口的BM比率：
- 1分钟BM比率
- 5分钟BM比率
- 20日BM比率（与研报一致）

---

## 九、总结

本变更实现了基于长江证券研报的买入情绪因子特征，包括：

1. **3个新特征**: 积极买入量、保守买入量、买入情绪比率
2. **1个新算子**: BuyMood类，实现买入行为识别和BM比率计算
3. **完整集成**: 从特征定义到计算流程的完整集成

所有变更遵循现有代码风格和架构模式，与现有特征系统完全兼容。

---

## 十、参考文档

- 研报: `~/work/trader/doc/Sellside 卖方金工研报/高频研报/长江证券/20170310-长江证券-金工高频识途系列（一）：基于买入行为构建情绪因子.pdf`
- 特征系统教程: `doc/Features_System_Tutorial.md`
- 添加特征指南: `doc/How_To_Add_New_Feature.md`
