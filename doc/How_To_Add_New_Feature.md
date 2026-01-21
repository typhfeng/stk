# 如何添加新特征 - 完整步骤指南

本文档详细说明如何向系统中添加一个新特征，包括所有必要的步骤和代码示例。

---

## 目录
1. [概述](#1-概述)
2. [步骤1: 在FeaturesDefine.hpp中定义特征](#2-步骤1-在featuresdefinehpp中定义特征)
3. [步骤2: 创建特征算子类（如需要）](#3-步骤2-创建特征算子类如需要)
4. [步骤3: 在ComputeGraph.hpp中添加计算逻辑](#4-步骤3-在computegraphhpp中添加计算逻辑)
5. [步骤4: 在Sequential文件中调用计算](#5-步骤4-在sequential文件中调用计算)
6. [步骤5: 编译和测试](#6-步骤5-编译和测试)
7. [完整示例：添加一个新特征](#7-完整示例添加一个新特征)
8. [常见问题](#8-常见问题)

---

## 1. 概述

添加新特征需要修改以下文件（按顺序）：

```
1. FeaturesDefine.hpp          - 定义特征元数据
2. FeaturesTick/TS/XXX.hpp     - 创建算子类（如需要）
3. ComputeGraph.hpp            - 添加CBuffer和算子实例
4. Tick_Sequential.hpp         - 调用计算并写入（L0级）
   或 Minute_Sequential.hpp     - 调用计算并写入（L1级）
```

**重要提示：**
- 系统使用宏自动生成枚举和偏移量，只需在 `LEVEL_0_FIELDS` 或 `LEVEL_1_FIELDS` 中添加定义
- 特征偏移量（`L0_FieldOffset::xxx`）会自动生成，无需手动维护
- 编译时系统会自动计算所有偏移量和宽度

---

## 2. 步骤1: 在FeaturesDefine.hpp中定义特征

### 2.1 确定特征级别

- **L0 (Tick级)**: 秒级特征，在 `LEVEL_0_FIELDS` 中定义
- **L1 (Minute级)**: 分钟级特征，在 `LEVEL_1_FIELDS` 中定义

### 2.2 添加特征定义

在 `cpp/include/features/FeaturesDefine.hpp` 中找到对应的宏，添加新特征：

**格式：**
```cpp
X(code, width, valid_type, data_type, cat_l1, cat_l2, norm_method, PSD, name_en, name_cn, description, formula)
```

**参数说明：**
- `code`: 特征代码名（C++标识符，如 `my_new_feature`）
- `width`: 特征宽度（维度数，通常为1）
- `valid_type`: `DATA`, `DEPTH`, 或 `ALL`
- `data_type`: `TS`(时序), `CS`(截面), `LB`(标签), `SH`(共享), `META`(元数据)
- `cat_l1`: 一级分类（如 `IMBALANCE`, `SHAPE`, `LIQUIDITY`等）
- `cat_l2`: 二级分类（如 `RAW`, `RATIO`, `NORMALIZED`等）
- `norm_method`: 归一化方法（如 `NONE`, `ZSCORE`, `RANK_ZSCORE`等）
- `PSD`: 频谱能量分布，格式 `"秒/分钟/小时"`（如 `"100/00/00"`）
- `name_en`: 英文名称
- `name_cn`: 中文名称
- `description`: 描述
- `formula`: LaTeX数学公式（用 `R"(...)"` 格式）

**示例：添加一个简单的价格比率特征（L0级）**

```cpp
// 在 LEVEL_0_FIELDS 宏中添加
#define LEVEL_0_FIELDS(X)\
  // ... 现有特征 ...
  X(price_ratio, 1, DATA, TS, LIQUIDITY, RATIO, NONE, "100/00/00", 
    "Price Ratio", "价格比率", 
    "买卖价格比率", 
    R"(\frac{P^A_1}{P^B_1})")
```

**示例：添加一个分钟级特征（L1级）**

```cpp
// 在 LEVEL_1_FIELDS 宏中添加
#define LEVEL_1_FIELDS(X)\
  // ... 现有特征 ...
  X(volatility_10m, 1, DATA, TS, VOLATILITY, NORMALIZED, ZSCORE, "00/100/00",
    "10-Minute Volatility", "10分钟波动率",
    "10分钟滚动波动率标准化",
    R"(\sigma_{10m})")
```

### 2.3 验证定义

编译时系统会自动：
- 生成 `L0_FieldOffset::price_ratio` 枚举值
- 计算特征在数组中的偏移量
- 生成所有元数据

**无需手动维护枚举或偏移量！**

---

## 3. 步骤2: 创建特征算子类（如需要）

### 3.1 判断是否需要新算子

- **简单特征**（如价格比率）：可以直接在 `compute_ts_tick()` 中计算，无需新算子
- **复杂特征**（如CI、PARA）：需要创建独立的算子类

### 3.2 创建算子类文件

如果特征计算逻辑复杂，创建新文件：

**文件位置：**
- L0特征：`cpp/include/features/FeaturesTick/TS/MyFeature.hpp`
- L1特征：`cpp/include/features/FeaturesMinute/TS/MyFeature.hpp`

**示例：创建一个简单的价格比率算子**

```cpp
// cpp/include/features/FeaturesTick/TS/PriceRatio.hpp
#pragma once

// =============================================================================
// PriceRatio - 价格比率
// =============================================================================
// 计算买卖价格比率: AskPrice[0] / BidPrice[0]
// =============================================================================

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

class PriceRatio {
public:
  PriceRatio(const CBuffer<float, L2::BLEN> &bid_price_0,
             const CBuffer<float, L2::BLEN> &ask_price_0,
             CBuffer<float, L2::BLEN> &buffer)
      : bid_price_0_(bid_price_0),
        ask_price_0_(ask_price_0),
        buffer_(buffer) {}

  void compute() {
    float bid = bid_price_0_.back();
    float ask = ask_price_0_.back();
    
    // 避免除零
    if (bid > 1e-6f) {
      float ratio = ask / bid;
      buffer_.push_back(ratio);
    } else {
      buffer_.push_back(0.0f);
    }
  }

  float back() const { return buffer_.back(); }

private:
  const CBuffer<float, L2::BLEN> &bid_price_0_;
  const CBuffer<float, L2::BLEN> &ask_price_0_;
  CBuffer<float, L2::BLEN> &buffer_;
};
```

**参考现有算子：**
- 简单算子：`cpp/include/features/FeaturesTick/TS/base/MidPrice.hpp`
- 复杂算子：`cpp/include/features/FeaturesTick/TS/CI.hpp`

---

## 4. 步骤3: 在ComputeGraph.hpp中添加计算逻辑

### 4.1 添加CBuffer存储

在 `cpp/include/features/ComputeGraph.hpp` 的 `DAG::L0` 或 `DAG::L1` 结构体中添加：

**对于L0特征：**
```cpp
struct L0 {
  // ... 现有CBuffer ...
  
  // 新特征的CBuffer
  CBuffer<float, L2::BLEN> price_ratio_;
  
  // ... 更多CBuffer ...
};
```

### 4.2 添加算子实例

在 `DAG::L0` 的构造函数区域添加算子实例：

```cpp
struct L0 {
  // ... 现有算子 ...
  
  // 新特征算子
  PriceRatio price_ratio{BidPrice_[0], AskPrice_[0], price_ratio_};
  
  // ... 更多算子 ...
  
  explicit L0(TickData &t) : td(t) {}
};
```

**完整示例（在DAG::L0中添加）：**

```cpp
struct L0 {
  TickData &td;
  
  // ... 现有CBuffer ...
  CBuffer<float, L2::BLEN> BidPrice_[L2::LOB_DEPTH];
  CBuffer<float, L2::BLEN> AskPrice_[L2::LOB_DEPTH];
  
  // 新特征CBuffer
  CBuffer<float, L2::BLEN> price_ratio_;
  
  // ... 现有算子 ...
  MidPrice MidPrice{BidPrice_[0], AskPrice_[0], MidPrice_};
  
  // 新特征算子
  PriceRatio price_ratio{BidPrice_[0], AskPrice_[0], price_ratio_};
  
  explicit L0(TickData &t) : td(t) {}
};
```

### 4.3 包含算子头文件

在 `ComputeGraph.hpp` 顶部添加包含：

```cpp
#pragma once

// ... 现有包含 ...
#include "features/FeaturesTick/TS/base/MidPrice.hpp"
#include "features/FeaturesTick/TS/PriceRatio.hpp"  // 新添加
```

---

## 5. 步骤4: 在Sequential文件中调用计算

### 5.1 L0级特征（Tick级）

在 `cpp/include/features/FeaturesTick/Tick_Sequential.hpp` 中：

#### 5.1.1 在 `compute_ts_tick()` 中调用计算

```cpp
inline void Tick_Sequential::compute_ts_tick(size_t t) {
  // ... 现有计算 ...
  
  if (dag_.tick_data.lob.depth_updated) {
    // ... 现有特征计算 ...
    dag_.l0.MidPrice.compute();
    dag_.l0.Spread.compute();
    
    // 新特征计算
    dag_.l0.price_ratio.compute();
    
    // ... 更多特征 ...
  }
  
  // ... 其他计算 ...
}
```

#### 5.1.2 填充缓冲区

在 `compute_ts_tick()` 的缓冲区填充部分添加：

```cpp
inline void Tick_Sequential::compute_ts_tick(size_t t) {
  // ... 计算部分 ...
  
  if (dag_.tick_data.lob.depth_updated) {
    // ... 计算特征 ...
    
    // --- 写入缓冲区 (用 L0_FieldOffset 索引) ---
    ts_features_buffer_[L0_FieldOffset::ci_1] = dag_.l0.ci_1_.back();
    ts_features_buffer_[L0_FieldOffset::ci_5] = dag_.l0.ci_5_.back();
    // ... 现有特征 ...
    
    // 新特征
    ts_features_buffer_[L0_FieldOffset::price_ratio] = dag_.l0.price_ratio_.back();
    
    // ... 更多特征 ...
  }
}
```

#### 5.1.3 写入存储（已自动包含）

`TS_WRITE_FEATURES` 宏会自动写入所有缓冲区中的特征，无需额外代码：

```cpp
// 在 compute_and_store() 中，这行代码已经会写入所有特征
TS_WRITE_FEATURES(store_, date_str_, 0, t, asset_id_, 
                  0, L0_FieldOffset::imba_30_entropy, 
                  ts_features_buffer_.data(), worker_id_);
```

**注意：** 确保 `ts_features_buffer_` 的大小足够（在 `Tick_Sequential` 类中定义为 `std::array<float, L0_TS_WIDTH>`，会自动调整）。

### 5.2 L1级特征（Minute级）

在 `cpp/include/features/FeaturesMinute/Minute_Sequential.hpp` 中：

#### 5.2.1 在 `compute_ts_minute()` 中计算

```cpp
inline void Minute_Sequential::compute_ts_minute(bool is_valid, size_t t) {
  if (!is_valid) {
    std::memset(ts_features_buffer_.data(), 0, sizeof(ts_features_buffer_));
  } else {
    ts_features_buffer_ = {
        dag_.l1.Min_.back(),           // min
        compute_min_ret_z(),           // min_ret_z
        compute_rv_5m_norm(),          // rv_5m_norm
        compute_vwap_gap_pct(),        // vwap_gap_pct
        compute_momentum_15m(),         // momentum_15m
        compute_range_squeeze(),        // range_squeeze
        compute_volatility_10m()       // 新特征（需要实现函数）
    };
  }
  
  // 写入特征
  TS_WRITE_FEATURES(store_, date_str_, 1, t, asset_id_, 
                    0, L1_FieldOffset::range_squeeze, 
                    ts_features_buffer_.data(), worker_id_);
}
```

#### 5.2.2 实现计算函数

在 `Minute_Sequential` 类中添加私有方法：

```cpp
class Minute_Sequential {
  // ... 现有方法 ...
  
private:
  // ... 现有计算函数 ...
  
  // 新特征计算函数
  float compute_volatility_10m() {
    auto &window = dag_.l1.minute_return_window;
    
    if (window.size() < 10)
      return 0.0f;
    
    // 计算最近10分钟的波动率
    float sum = 0, sq_sum = 0;
    size_t n = std::min(window.size(), size_t(10));
    
    for (size_t i = window.size() - n + 1; i < window.size(); ++i) {
      if (i > 0 && window[i-1] > 0) {
        float r = std::log(window[i] / window[i-1]);
        sum += r;
        sq_sum += r * r;
      }
    }
    
    float mean = sum / (n - 1);
    float variance = (sq_sum / (n - 1)) - (mean * mean);
    float stddev = std::sqrt(std::max(0.0f, variance));
    
    return stddev;
  }
};
```

---

## 6. 步骤5: 编译和测试

### 6.1 编译项目

```bash
cd /home/typhwang/work/stk
python3 py/main.py
```

### 6.2 检查编译错误

常见错误：
- **未定义的枚举**: 检查特征名拼写是否与 `FeaturesDefine.hpp` 中一致
- **缺少包含**: 确保在 `ComputeGraph.hpp` 中包含了算子头文件
- **缓冲区大小**: 确保 `ts_features_buffer_` 大小足够（通常自动处理）

### 6.3 验证特征

运行计算后，检查：
1. 特征是否正确写入存储
2. 特征值是否合理
3. 特征是否出现在GUI中（如果GUI支持）

---

## 7. 完整示例：添加一个新特征

让我们添加一个完整的示例：**买卖价差比率** (Bid-Ask Spread Ratio)

### 步骤1: 在FeaturesDefine.hpp中定义

```cpp
// 在 LEVEL_0_FIELDS 宏中添加
#define LEVEL_0_FIELDS(X)\
  // ... 现有特征 ...
  X(spread_ratio, 1, DATA, TS, LIQUIDITY, RATIO, NONE, "100/00/00",
    "Spread Ratio", "价差比率",
    "买卖价差与中间价的比率",
    R"(\frac{P^A_1 - P^B_1}{(P^A_1 + P^B_1)/2})")
```

### 步骤2: 创建算子类（可选，这里直接计算）

由于计算简单，我们可以直接在 `compute_ts_tick()` 中计算，无需新算子。

或者创建算子（更规范）：

```cpp
// cpp/include/features/FeaturesTick/TS/base/SpreadRatio.hpp
#pragma once

#include "codec/L2_DataType.hpp"
#include "define/CBuffer.hpp"

class SpreadRatio {
public:
  SpreadRatio(const CBuffer<float, L2::BLEN> &bid_price_0,
              const CBuffer<float, L2::BLEN> &ask_price_0,
              CBuffer<float, L2::BLEN> &buffer)
      : bid_price_0_(bid_price_0),
        ask_price_0_(ask_price_0),
        buffer_(buffer) {}

  void compute() {
    float bid = bid_price_0_.back();
    float ask = ask_price_0_.back();
    float mid = (bid + ask) * 0.5f;
    
    if (mid > 1e-6f) {
      float spread = ask - bid;
      float ratio = spread / mid;
      buffer_.push_back(ratio);
    } else {
      buffer_.push_back(0.0f);
    }
  }

  float back() const { return buffer_.back(); }

private:
  const CBuffer<float, L2::BLEN> &bid_price_0_;
  const CBuffer<float, L2::BLEN> &ask_price_0_;
  CBuffer<float, L2::BLEN> &buffer_;
};
```

### 步骤3: 在ComputeGraph.hpp中添加

```cpp
// 在 DAG::L0 结构体中
struct L0 {
  // ... 现有CBuffer ...
  CBuffer<float, L2::BLEN> spread_ratio_;
  
  // ... 现有算子 ...
  Spread Spread{BidPrice_[0], AskPrice_[0], Spread_};
  
  // 新算子
  SpreadRatio spread_ratio{BidPrice_[0], AskPrice_[0], spread_ratio_};
  
  explicit L0(TickData &t) : td(t) {}
};

// 在文件顶部添加包含
#include "features/FeaturesTick/TS/base/SpreadRatio.hpp"
```

### 步骤4: 在Tick_Sequential.hpp中调用

```cpp
inline void Tick_Sequential::compute_ts_tick(size_t t) {
  // ... 现有计算 ...
  
  if (dag_.tick_data.lob.depth_updated) {
    // ... 现有特征 ...
    dag_.l0.Spread.compute();
    
    // 新特征
    dag_.l0.spread_ratio.compute();
    
    // ... 更多特征 ...
    
    // 填充缓冲区
    ts_features_buffer_[L0_FieldOffset::spread] = dag_.l0.Spread_.back();
    ts_features_buffer_[L0_FieldOffset::spread_ratio] = dag_.l0.spread_ratio_.back();  // 新特征
    
    // ... 更多缓冲区填充 ...
  }
  
  // TS_WRITE_FEATURES 会自动写入所有特征
}
```

### 步骤5: 编译测试

```bash
python3 py/main.py
```

如果编译成功，特征就已经添加到系统中了！

---

## 8. 常见问题

### Q1: 特征枚举未定义错误

**错误：** `'L0_FieldOffset' has no member named 'my_feature'`

**解决：**
1. 检查 `FeaturesDefine.hpp` 中特征名拼写
2. 确保特征定义在正确的宏中（`LEVEL_0_FIELDS` 或 `LEVEL_1_FIELDS`）
3. 重新编译（枚举是编译时生成的）

### Q2: 缓冲区越界错误

**错误：** `array index out of bounds`

**解决：**
- `ts_features_buffer_` 大小由 `L0_TS_WIDTH` 自动计算
- 如果添加的特征数量很多，可能需要检查缓冲区大小
- 通常系统会自动处理

### Q3: 特征值始终为0

**可能原因：**
1. 计算函数未被调用（检查触发条件）
2. 数据无效（检查 `depth_updated` 或 `is_valid` 标志）
3. 计算逻辑错误（检查算子实现）

### Q4: 如何添加截面特征（CS特征）？

截面特征需要：
1. 在 `FeaturesDefine.hpp` 中定义 `data_type` 为 `CS`
2. 在 `Tick_Crosssection.hpp` 或 `Minute_Crosssection.hpp` 中添加计算逻辑
3. 使用 `CS_READ_ALL()` 读取TS特征
4. 使用 `CS_WRITE_ALL()` 写入CS特征

### Q5: 特征在GUI中不显示？

**可能原因：**
1. GUI需要重新加载特征列表
2. 特征元数据未正确生成
3. 检查GUI的特征选择逻辑

### Q6: 如何调试特征计算？

**方法：**
1. 在 `compute()` 函数中添加日志
2. 检查 `CBuffer` 的 `back()` 值
3. 使用调试器查看 `ts_features_buffer_` 内容
4. 检查存储中的实际值

---

## 9. 总结

添加新特征的完整流程：

```
1. FeaturesDefine.hpp     → 定义特征元数据
2. 创建算子类（如需要）   → 实现计算逻辑
3. ComputeGraph.hpp      → 添加CBuffer和算子
4. Sequential文件         → 调用计算并写入
5. 编译测试              → 验证功能
```

**关键点：**
- ✅ 系统自动生成枚举和偏移量，无需手动维护
- ✅ 遵循现有代码风格和模式
- ✅ 注意特征的触发条件（`depth_updated`, `is_valid`等）
- ✅ 确保计算逻辑正确，处理边界情况（除零、空数据等）

---

## 10. 参考文件

- **特征定义**: `cpp/include/features/FeaturesDefine.hpp`
- **计算图**: `cpp/include/features/ComputeGraph.hpp`
- **Tick级计算**: `cpp/include/features/FeaturesTick/Tick_Sequential.hpp`
- **Minute级计算**: `cpp/include/features/FeaturesMinute/Minute_Sequential.hpp`
- **简单算子示例**: `cpp/include/features/FeaturesTick/TS/base/MidPrice.hpp`
- **复杂算子示例**: `cpp/include/features/FeaturesTick/TS/CI.hpp`

---

**祝您添加特征顺利！如有问题，请参考现有特征的实现模式。**
