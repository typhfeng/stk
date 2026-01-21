# 特征系统完整教程 (Features System Tutorial)

## 目录
1. [特征定义系统](#1-特征定义系统)
2. [计算架构](#2-计算架构)
3. [计算流程](#3-计算流程)
4. [数据存储](#4-数据存储)
5. [数据读取](#5-数据读取)
6. [完整算法流程图](#6-完整算法流程图)

---

## 1. 特征定义系统

### 1.1 特征定义宏 (LEVEL_0_FIELDS / LEVEL_1_FIELDS)

特征通过宏定义在 `FeaturesDefine.hpp` 中，格式如下：

```cpp
#define LEVEL_0_FIELDS(X)\
  X(code, width, valid_type, data_type, cat_l1, cat_l2, norm_method, PSD, name_en, name_cn, description, formula)
```

**参数说明：**
- `code`: 特征代码名（如 `ci_1`, `spread`）
- `width`: 特征宽度（维度数，通常为1）
- `valid_type`: 有效性类型（`DATA`, `DEPTH`, `ALL`）
- `data_type`: 数据类型（`TS`时序, `CS`截面, `LB`标签, `SH`共享, `META`元数据）
- `cat_l1`: 一级分类（`IMBALANCE`, `SHAPE`, `ORDER_FLOW`, `LIQUIDITY`, `VOLATILITY`, `BASIC`, `LABEL`, `META`）
- `cat_l2`: 二级分类（`RAW`, `NORMALIZED`, `OSCILLATOR`, `DEVIATION`, `RATIO`, `RANK`, `FUTURE_RET`, `SCORE`）
- `norm_method`: 归一化方法（`NONE`, `ZSCORE`, `RANK_ZSCORE`, `WINSOR`, `SINCOS`等）
- `PSD`: 频谱能量分布（格式：`"秒/分钟/小时"`）
- `name_en/cn`: 英文/中文名称
- `description`: 描述
- `formula`: LaTeX数学公式

### 1.2 特征示例

**示例1：累计失衡特征 (CI)**
```cpp
X(ci_5, 1, DATA, TS, IMBALANCE, RATIO, NONE, "100/00/00", 
  "Cumu Imba 5-Level", "累计5档失衡", "累计5档订单失衡率",
  R"(\frac{\sum_{i=1}^{5}(V_{i,t}^{B} - V_{i,t}^{A})}{\sum_{i=1}^{5}(V_{i,t}^{B} + V_{i,t}^{A})})")
```

**示例2：截面排名特征 (CS)**
```cpp
X(cs_spread_rank, 1, DATA, CS, LIQUIDITY, RANK, RANK_ZSCORE, "100/00/00",
  "CS Spread Rank", "价差截面排名", "spread截面rank→inverse normal",
  R"(\Phi^{-1}(\mathrm{pctl}(\mathrm{spread})))")
```

### 1.3 特征分类体系

```
FeatureDataType (数据类型)
├── TS (Time-Series) 时序特征
│   └── 单资产时间序列计算
├── CS (Cross-Sectional) 截面特征
│   └── 跨资产横截面计算
├── LB (Label) 标签
│   └── 预测目标（未来收益等）
├── SH (Shared) 共享值
│   └── TS/CS共用中间值
└── META (Metadata) 元数据
    └── 系统内部数据

FeatureCategoryL1 (一级分类)
├── IMBALANCE (失衡)
├── SHAPE (形状)
├── ORDER_FLOW (订单流)
├── LIQUIDITY (流动性)
├── VOLATILITY (波动率)
├── BASIC (基础)
├── LABEL (标签)
└── META (元数据)
```

---

## 2. 计算架构

### 2.1 分层架构

```
┌─────────────────────────────────────────────────────────────┐
│                    LOB (限价订单簿)                          │
│              LimitOrderBook::process_batch()                │
└──────────────────────┬────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              CoreSequential (时序特征核心)                    │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Level 0: Tick_Sequential (秒级特征)                 │  │
│  │  ├── compute_ts_tick(t)                              │  │
│  │  │   ├── [EVERY TICK] DeltaT, TickIndex              │  │
│  │  │   ├── [ON TAKER] TradePrice                       │  │
│  │  │   └── [ON DEPTH] 所有深度特征                     │  │
│  │  └── TS_WRITE_FEATURES() → FeatureStore              │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  ResamplerTick2Min (重采样器)                        │  │
│  │  └── tick → minute 聚合                              │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Level 1: Minute_Sequential (分钟级特征)             │  │
│  │  ├── compute_ts_minute(t)                            │  │
│  │  └── TS_WRITE_FEATURES() → FeatureStore              │  │
│  └──────────────────────────────────────────────────────┘  │
└──────────────────────┬────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│           CoreCrosssection (截面特征核心)                    │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Level 0: Tick_Crosssection                          │  │
│  │  ├── CS_READ_ALL() 读取所有资产的TS特征               │  │
│  │  ├── compute_rank_inverse_normal_sparse()            │  │
│  │  └── CS_WRITE_ALL() 写入截面特征                      │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Level 1: Minute_Crosssection                        │  │
│  │  └── 同上，但处理分钟级数据                           │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 DAG (计算图)

DAG (Directed Acyclic Graph) 是特征计算的核心数据结构：

```cpp
class DAG {
  TickData &tick_data;    // L0 输入
  MinuteData minute_data; // L1 输入
  
  struct L0 {
    // 基础数据 CBuffer
    CBuffer<float> BidPrice_[DEPTH];
    CBuffer<float> AskPrice_[DEPTH];
    CBuffer<float> BidQty_[DEPTH];
    CBuffer<float> AskQty_[DEPTH];
    
    // 特征算子
    CI<5> ci_5;              // 累计失衡
    CWI<10> cwi_1;           // 凸加权失衡
    PARA<true, 0> b_para_c0; // 抛物线拟合
    // ... 更多算子
  } l0;
  
  struct L1 {
    CBuffer<float> Min_;
    // 分钟级特征计算
  } l1;
};
```

**计算图依赖关系：**
```
LOB数据
  │
  ├─→ DepthData (基础数据层)
  │     │
  │     ├─→ MidPrice, Spread (基础价格)
  │     │
  │     └─→ CI, CWI, DDI (失衡特征)
  │           │
  │           └─→ PARA (形状特征)
  │                 │
  │                 └─→ GRAD, ENTROPY (衍生特征)
  │
  └─→ TradePrice (成交价)
        │
        └─→ Minute级特征 (重采样后)
```

---

## 3. 计算流程

### 3.1 时序特征计算流程

```
┌─────────────────────────────────────────────────────────────┐
│  sequential_worker (多线程，每个worker处理N个资产)          │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
        ┌──────────────────────────┐
        │  遍历日期 (date-first)    │
        └──────────┬─────────────────┘
                   │
                   ▼
        ┌──────────────────────────┐
        │  遍历资产 (asset)          │
        └──────────┬─────────────────┘
                   │
                   ▼
        ┌──────────────────────────┐
        │  decode_orders_stream()   │
        │  解码L2订单流             │
        └──────────┬─────────────────┘
                   │
                   ▼
        ┌──────────────────────────┐
        │  LimitOrderBook::         │
        │  process_batch(orders)    │
        └──────────┬─────────────────┘
                   │
                   ▼
        ┌──────────────────────────┐
        │  对每个订单:               │
        │  - 更新LOB状态            │
        │  - 触发特征计算           │
        └──────────┬─────────────────┘
                   │
                   ▼
        ┌──────────────────────────┐
        │  CoreSequential::          │
        │  compute_and_store()       │
        └──────────┬─────────────────┘
                   │
        ┌──────────┴──────────┐
        │                     │
        ▼                     ▼
┌──────────────┐    ┌─────────────────┐
│ Tick级计算   │    │ Minute级计算     │
│              │    │ (当tick2min      │
│ compute_ts_  │    │  .update()时)    │
│ tick(t)      │    │                  │
│              │    │ compute_ts_      │
│ 1. DeltaT    │    │ minute(t)        │
│ 2. TickIndex │    │                  │
│ 3. 如果depth_│    │ 1. min_ret_z     │
│    updated:  │    │ 2. rv_5m_norm    │
│    - CI      │    │ 3. momentum_15m   │
│    - CWI     │    │ 4. range_squeeze  │
│    - PARA    │    │ ...               │
│    - ...     │    │                  │
│              │    │                  │
│ TS_WRITE_    │    │ TS_WRITE_        │
│ FEATURES()   │    │ FEATURES()       │
└──────────────┘    └─────────────────┘
```

### 3.2 截面特征计算流程

```
┌─────────────────────────────────────────────────────────────┐
│  crosssectional_worker (单线程，等待TS完成)                 │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
        ┌──────────────────────────┐
        │  遍历日期 (date-first)    │
        └──────────┬─────────────────┘
                   │
                   ▼
        ┌──────────────────────────┐
        │  遍历时间槽 t (0..T-1)     │
        └──────────┬─────────────────┘
                   │
                   ▼
        ┌──────────────────────────┐
        │  store.cs_wait(date, t)   │
        │  等待所有TS workers完成    │
        │  该时间槽的计算            │
        └──────────┬─────────────────┘
                   │
                   ▼
        ┌──────────────────────────┐
        │  CoreCrosssection::       │
        │  compute_and_store(t)     │
        └──────────┬─────────────────┘
                   │
        ┌──────────┴──────────┐
        │                     │
        ▼                     ▼
┌──────────────┐    ┌─────────────────┐
│ Tick级CS     │    │ Minute级CS      │
│              │    │ (当t%60==0时)   │
│ 1. CS_READ_  │    │                  │
│    ALL()     │    │ 1. CS_READ_ALL() │
│    读取所有   │    │    读取所有资产   │
│    资产的TS   │    │    的TS特征      │
│    特征       │    │                  │
│              │    │ 2. compute_rank_ │
│ 2. compute_  │    │    inverse_      │
│    rank_     │    │    normal_       │
│    inverse_  │    │    sparse()      │
│    normal_   │    │    计算排名       │
│    sparse()  │    │                  │
│              │    │ 3. CS_WRITE_ALL()│
│ 3. CS_WRITE_ │    │    写入截面特征   │
│    ALL()     │    │                  │
│    写入截面   │    │                  │
│    特征       │    │                  │
└──────────────┘    └─────────────────┘
```

### 3.3 特征计算触发条件

**Tick级特征触发：**
- `[EVERY TICK]`: 每个订单都触发
  - `DeltaT`: 时间间隔
  - `TickIndex`: 时间索引
- `[ON TAKER]`: 成交单触发
  - `TradePrice`: 成交价
- `[ON DEPTH]`: 盘口更新触发（`depth_updated == true`）
  - 所有深度相关特征（CI, CWI, PARA, GRAD, ENTROPY等）

**Minute级特征触发：**
- 当 `ResamplerTick2Min::update()` 返回 `true` 时
- 即：tick数据累积到1分钟边界时

**截面特征触发：**
- 在每个时间槽 `t`，等待所有TS workers完成
- 然后计算该时刻所有资产的截面排名/标准化

---

## 4. 数据存储

### 4.1 存储布局

特征数据以 **3D张量** 形式存储：`[T, F, A]`

- **T (Time)**: 时间维度
  - L0: 15300 (秒级，255分钟×60)
  - L1: 255 (分钟级)
- **F (Feature)**: 特征维度
  - L0: ~40个特征
  - L1: ~15个特征
- **A (Asset)**: 资产维度
  - 通常100-1000个资产

**内存布局：`[T][F][A]` (行主序)**
```
地址公式: offset = (t * F + f) * A + a

示例: 访问 (t=100, f=5, a=10)
offset = (100 * 40 + 5) * 1000 + 10 = 4,005,010
```

### 4.2 存储API

**写入时序特征：**
```cpp
// 单个特征写入
TS_WRITE_SINGLE(store, date, level, t, field_enum, asset_id, value, worker_id);

// 批量特征写入（推荐，更高效）
TS_WRITE_FEATURES(store, date, level, t, asset_id, f_start, f_end, src, worker_id);
```

**写入截面特征：**
```cpp
// 写入所有资产的某个特征
CS_WRITE_ALL(store, date, level, t, field_enum, src, count);
```

**示例代码：**
```cpp
// 在 Tick_Sequential::compute_ts_tick() 中
std::array<float, L0_TS_WIDTH> ts_features_buffer_;

// 填充缓冲区
ts_features_buffer_[L0_FieldOffset::ci_1] = dag_.l0.ci_1_.back();
ts_features_buffer_[L0_FieldOffset::ci_5] = dag_.l0.ci_5_.back();
// ... 更多特征

// 批量写入（一次性写入多个特征）
TS_WRITE_FEATURES(store_, date_str_, 0, t, asset_id_, 
                  0, L0_FieldOffset::imba_30_entropy, 
                  ts_features_buffer_.data(), worker_id_);
```

### 4.3 磁盘存储格式

**L0 (秒级) - 列式存储：**
```
output/features/YYYY/MM/DD/
├── features_L0_f0.zst   (第0个特征，所有时间×所有资产)
├── features_L0_f1.zst   (第1个特征)
├── ...
└── features_L0_f39.zst  (第39个特征)
```

**L1 (分钟级) - 合并存储：**
```
output/features/YYYY/MM/DD/
└── features_L1.zst      (所有特征合并，[T][F][A])
```

**优势：**
- L0列式：适合按特征查询（GUI常用）
- L1合并：适合按时间查询（回测常用）
- Zstd压缩：减少磁盘占用

---

## 5. 数据读取

### 5.1 读取API

**读取时序特征（单个资产）：**
```cpp
// 通过 FeatureReader 类
FeatureReader reader(base_dir);
DayTensor tensor;
tensor.preallocate(A, selected_features);

// 读取单日单级别
reader.load_day_level(date, level, tensor);

// 访问数据
float value = tensor.data[level][(t * F + f) * A + a];
```

**读取截面特征（所有资产）：**
```cpp
// 在截面计算中使用
const _Float16 *all_assets = CS_READ_ALL(store, date, level, t, field_enum);

// all_assets 是指向 A 个资产值的连续数组
for (size_t a = 0; a < A; ++a) {
  float value = static_cast<float>(all_assets[a]);
  // 处理...
}
```

### 5.2 读取流程

```
┌─────────────────────────────────────────────────────────────┐
│  FeatureReader::load_day_level(date, level, tensor)        │
└──────────────────────┬──────────────────────────────────────┘
                       │
        ┌──────────────┴──────────────┐
        │                             │
        ▼                             ▼
┌──────────────┐            ┌─────────────────┐
│ L0 (列式)    │            │ L1 (合并)       │
│              │            │                 │
│ for f in     │            │ 直接读取单个文件  │
│ features:    │            │ features_L1.zst  │
│              │            │                 │
│ 1. 读取      │            │ 解压 → 内存      │
│    features_ │            │ [T][F][A]       │
│    L0_f{f}.  │            │                 │
│    zst        │            │                 │
│              │            │                 │
│ 2. 解压      │            │                 │
│    → [T][A]   │            │                 │
│              │            │                 │
│ 3. 交错写入   │            │                 │
│    tensor     │            │                 │
│    [T][F][A]  │            │                 │
└──────────────┘            └─────────────────┘
```

---

## 6. 完整算法流程图

### 6.1 整体架构流程图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          主程序入口                                        │
│                        py/main.py → build.sh                            │
└──────────────────────────────┬──────────────────────────────────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │   初始化 SharedData   │
                    │   - 加载资产列表      │
                    │   - 加载日期列表      │
                    │   - 分配worker        │
                    └───────────┬───────────┘
                                │
                ┌───────────────┴───────────────┐
                │                               │
                ▼                               ▼
    ┌───────────────────────┐      ┌───────────────────────┐
    │  TS Workers (N-2个)   │      │  CS Worker (1个)       │
    │  并行处理时序特征       │      │  单线程处理截面特征     │
    └───────────┬───────────┘      └───────────┬───────────┘
                │                               │
                │                               │
                ▼                               │
    ┌───────────────────────┐                   │
    │  sequential_worker()  │                   │
    │                       │                   │
    │  for date in dates:   │                   │
    │    for asset in my_   │                   │
    │         assets:       │                   │
    │      decode orders    │                   │
    │      process LOB      │                   │
    │      compute TS       │                   │
    │      TS_WRITE()       │                   │
    │      → FeatureStore   │                   │
    └───────────┬───────────┘                   │
                │                               │
                │  同步点                        │
                │  (每个时间槽t)                 │
                ├───────────────────────────────┤
                │                               │
                ▼                               ▼
    ┌───────────────────────┐      ┌───────────────────────┐
    │  store.ts_done()      │      │  store.cs_wait()      │
    │  标记TS完成           │      │  等待TS完成           │
    └───────────┬───────────┘      └───────────┬───────────┘
                │                               │
                │                               ▼
                │                   ┌───────────────────────┐
                │                   │  crosssectional_      │
                │                   │  worker()             │
                │                   │                       │
                │                   │  for date in dates:   │
                │                   │    for t in [0..T):  │
                │                   │      CS_READ_ALL()   │
                │                   │      compute CS      │
                │                   │      CS_WRITE_ALL()  │
                │                   │      → FeatureStore   │
                │                   └───────────┬───────────┘
                │                               │
                │                               │
                └───────────────┬───────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │  IO Worker (1个)       │
                    │  异步写入磁盘          │
                    │                       │
                    │  for date in dates:   │
                    │    wait DONE state    │
                    │    compress & write    │
                    │    → disk             │
                    └───────────────────────┘
```

### 6.2 时序特征计算详细流程

```
┌─────────────────────────────────────────────────────────────────┐
│  LimitOrderBook::process_batch(orders, order_num)              │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │  for each order:     │
                    │    update LOB state  │
                    │    set flags:        │
                    │    - depth_updated    │
                    │    - order_type      │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │  CoreSequential::     │
                    │  compute_and_store() │
                    └──────────┬───────────┘
                               │
                ┌──────────────┴──────────────┐
                │                             │
                ▼                             ▼
    ┌───────────────────────┐    ┌───────────────────────┐
    │  Tick级计算            │    │  Minute级计算         │
    │                       │    │                       │
    │  l0_index = Clock_     │    │  if tick2min.update():│
    │           to_L0()      │    │                       │
    │                       │    │    l1_index = ...     │
    │  tick_sequential_.    │    │    minute_sequential_.│
    │  compute_and_store()   │    │    compute_and_store()│
    │                       │    │                       │
    │  ┌─────────────────┐  │    │  ┌─────────────────┐  │
    │  │ compute_ts_tick │  │    │  │ compute_ts_     │  │
    │  │ (t)            │  │    │  │ minute(t)       │  │
    │  │                │  │    │  │                 │  │
    │  │ 1. DeltaT.     │  │    │  │ 1. min_ret_z    │  │
    │  │    compute()   │  │    │  │ 2. rv_5m_norm   │  │
    │  │ 2. TickIndex.  │  │    │  │ 3. momentum_15m │  │
    │  │    compute()   │  │    │  │ 4. range_squeeze│  │
    │  │                │  │    │  │                 │  │
    │  │ if TAKER:      │  │    │  │ 填充buffer      │  │
    │  │   TradePrice.  │  │    │  │                 │  │
    │  │   compute()    │  │    │  │ TS_WRITE_       │  │
    │  │                │  │    │  │ FEATURES()     │  │
    │  │ if depth_      │  │    │  └─────────────────┘  │
    │  │ updated:       │  │    │                       │
    │  │   DepthData.   │  │    │                       │
    │  │   compute()    │  │    │                       │
    │  │   MidPrice.    │  │    │                       │
    │  │   compute()    │  │    │                       │
    │  │   CI.compute() │  │    │                       │
    │  │   CWI.compute()│  │    │                       │
    │  │   PARA.compute()│ │    │                       │
    │  │   ...          │  │    │                       │
    │  │                │  │    │                       │
    │  │ 填充buffer     │  │    │                       │
    │  │                │  │    │                       │
    │  │ TS_WRITE_      │  │    │                       │
    │  │ FEATURES()     │  │    │                       │
    │  └─────────────────┘  │    │                       │
    └───────────────────────┘    └───────────────────────┘
```

### 6.3 截面特征计算详细流程

```
┌─────────────────────────────────────────────────────────────────┐
│  crosssectional_worker()                                        │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │  for date in dates:  │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │  core.set_date(date) │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │  for t in [0..T):    │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │  store.cs_wait(      │
                    │    date, t)           │
                    │                       │
                    │  等待所有TS workers   │
                    │  完成时间槽t          │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │  core.compute_and_   │
                    │  store(t)            │
                    └──────────┬───────────┘
                               │
                ┌──────────────┴──────────────┐
                │                             │
                ▼                             ▼
    ┌───────────────────────┐    ┌───────────────────────┐
    │  Tick级CS             │    │  Minute级CS            │
    │                       │    │  (if t%60==0)          │
    │  tick_cs_.compute_    │    │                       │
    │  and_store(t)         │    │  minute_cs_.compute_   │
    │                       │    │  and_store(t_minute)  │
    │  ┌─────────────────┐  │    │                       │
    │  │ 1. CS_READ_ALL  │  │    │  ┌─────────────────┐  │
    │  │    (date, 0, t, │  │    │  │ 1. CS_READ_ALL  │  │
    │  │     field)      │  │    │  │    (date, 1, t, │  │
    │  │    → 获取所有    │  │    │  │     field)      │  │
    │  │    资产的TS值    │  │    │  │                 │  │
    │  │                 │  │    │  │ 2. 构建valid_   │  │
    │  │ 2. 构建valid_   │  │    │  │    indices      │  │
    │  │    indices      │  │    │  │    (过滤无效)    │  │
    │  │    (过滤无效)    │  │    │  │                 │  │
    │  │                 │  │    │  │ 3. compute_rank_│  │
    │  │ 3. compute_rank_│  │    │  │    inverse_     │  │
    │  │    inverse_     │  │    │  │    normal_       │  │
    │  │    normal_      │  │    │  │    sparse()      │  │
    │  │    sparse()     │  │    │  │    (排名→逆正态)  │  │
    │  │    (排名→逆正态) │  │    │  │                 │  │
    │  │                 │  │    │  │ 4. CS_WRITE_ALL  │  │
    │  │ 4. CS_WRITE_ALL │  │    │  │    (写入结果)     │  │
    │  │    (写入结果)    │  │    │  └─────────────────┘  │
    │  └─────────────────┘  │    │                       │
    └───────────────────────┘    └───────────────────────┘
```

### 6.4 数据流图

```
┌─────────────────────────────────────────────────────────────────┐
│                        输入数据流                                 │
│                    L2订单流 (Binary)                             │
└──────────────────────────────┬──────────────────────────────────┘
                                │
                                ▼
                    ┌──────────────────────┐
                    │  BinaryDecoder_L2   │
                    │  decode_orders_     │
                    │  stream()           │
                    └──────────┬──────────┘
                                │
                                ▼
                    ┌──────────────────────┐
                    │  LimitOrderBook      │
                    │  process_batch()     │
                    │                      │
                    │  维护LOB状态:        │
                    │  - BidPrice[0..N]    │
                    │  - AskPrice[0..N]   │
                    │  - BidQty[0..N]     │
                    │  - AskQty[0..N]     │
                    └──────────┬───────────┘
                                │
                                ▼
                    ┌──────────────────────┐
                    │  DAG (计算图)        │
                    │                      │
                    │  L0层:               │
                    │  - CBuffer存储中间值  │
                    │  - 算子计算特征      │
                    │                      │
                    │  L1层:               │
                    │  - 重采样聚合        │
                    │  - 分钟级特征        │
                    └──────────┬───────────┘
                                │
                ┌───────────────┴───────────────┐
                │                               │
                ▼                               ▼
    ┌───────────────────────┐      ┌───────────────────────┐
    │  TS特征写入            │      │  CS特征计算            │
    │                       │      │                       │
    │  TS_WRITE_FEATURES()  │      │  CS_READ_ALL()        │
    │  → FeatureStore       │      │  → 读取TS特征          │
    │     [T][F][A]         │      │                       │
    │                       │      │  compute_rank_...()  │
    │  内存布局:             │      │  → 计算截面排名        │
    │  offset =              │      │                       │
    │    (t*F+f)*A+a        │      │  CS_WRITE_ALL()       │
    └───────────┬───────────┘      │  → 写入CS特征          │
                │                   │     [T][F][A]         │
                │                   └───────────┬───────────┘
                │                               │
                └───────────────┬───────────────┘
                                │
                                ▼
                    ┌──────────────────────┐
                    │  FeatureStore        │
                    │  (内存张量池)         │
                    │                      │
                    │  Slot状态机:         │
                    │  FREE → INIT →      │
                    │  BUSY → DONE →      │
                    │  FLUSH              │
                    └──────────┬───────────┘
                                │
                                ▼
                    ┌──────────────────────┐
                    │  IO Worker           │
                    │  flush_to_disk()    │
                    │                      │
                    │  1. 压缩 (Zstd)      │
                    │  2. 写入磁盘         │
                    │     output/features/ │
                    │     YYYY/MM/DD/      │
                    └──────────┬───────────┘
                                │
                                ▼
                    ┌──────────────────────┐
                    │  磁盘文件             │
                    │                      │
                    │  L0:                 │
                    │  features_L0_f0.zst │
                    │  features_L0_f1.zst │
                    │  ...                 │
                    │                      │
                    │  L1:                 │
                    │  features_L1.zst    │
                    └──────────────────────┘
```

### 6.5 同步机制流程图

```
┌─────────────────────────────────────────────────────────────────┐
│                    TS Workers (并行)                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐     │
│  │Worker 0  │  │Worker 1  │  │Worker 2  │  │Worker N-1 │     │
│  │          │  │          │  │          │  │          │     │
│  │处理资产   │  │处理资产   │  │处理资产   │  │处理资产   │     │
│  │0-9       │  │10-19     │  │20-29     │  │...       │     │
│  │          │  │          │  │          │  │          │     │
│  │for t:    │  │for t:    │  │for t:    │  │for t:    │     │
│  │  compute │  │  compute │  │  compute │  │  compute │     │
│  │  TS_WRITE│  │  TS_WRITE│  │  TS_WRITE│  │  TS_WRITE│     │
│  │  update  │  │  update  │  │  update  │  │  update  │     │
│  │  min_pos │  │  min_pos │  │  min_pos │  │  min_pos │     │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘     │
│       │             │             │             │            │
│       └─────────────┴─────────────┴─────────────┘            │
│                 │                                               │
│                 ▼                                               │
│       ┌─────────────────────┐                                  │
│       │  Slot.ts_worker_    │                                  │
│       │  state[worker_id]   │                                  │
│       │                     │                                  │
│       │  打包状态:           │                                  │
│       │  [48b:min_pos]      │                                  │
│       │  [1b:done_flag]     │                                  │
│       └──────────┬──────────┘                                  │
└──────────────────┼──────────────────────────────────────────────┘
                   │
                   │  同步点: 每个时间槽t
                   │
                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                    CS Worker (单线程)                            │
│                                                                 │
│  for t in [0..T):                                              │
│    ┌─────────────────────────────────────┐                     │
│    │  store.cs_wait(date, t)            │                     │
│    │                                     │                     │
│    │  1. 读取所有worker的min_pos         │                     │
│    │  2. 计算全局min = min(all min_pos)  │                     │
│    │  3. 等待 min >= t                   │                     │
│    │  4. 检查所有worker的done标志        │                     │
│    │  5. 确保时间槽t的所有TS数据就绪      │                     │
│    └──────────────┬────────────────────┘                     │
│                   │                                             │
│                   ▼                                             │
│    ┌─────────────────────────────────────┐                     │
│    │  compute_and_store(t)              │                     │
│    │                                     │                     │
│    │  CS_READ_ALL()  ← 读取TS特征         │                     │
│    │  compute_rank() ← 计算截面排名      │                     │
│    │  CS_WRITE_ALL() ← 写入CS特征        │                     │
│    └─────────────────────────────────────┘                     │
│                                                                 │
│    store.cs_done(date)  ← 标记完成                              │
└─────────────────────────────────────────────────────────────────┘
```

---

## 7. 实际使用示例

### 7.1 添加新特征

**步骤1：在 FeaturesDefine.hpp 中定义**
```cpp
#define LEVEL_0_FIELDS(X)\
  // ... 现有特征 ...
  X(my_new_feature, 1, DATA, TS, IMBALANCE, RATIO, NONE, 
    "100/00/00", "My New Feature", "我的新特征", 
    "新特征描述", R"(数学公式)")
```

**步骤2：在 ComputeGraph.hpp 中添加计算逻辑**
```cpp
struct L0 {
  // ... 现有CBuffer ...
  CBuffer<float, L2::BLEN> my_new_feature_;
  
  // 添加算子
  MyNewFeatureOperator my_new_feature{BidQty_, AskQty_, my_new_feature_};
};
```

**步骤3：在 Tick_Sequential.hpp 中调用**
```cpp
inline void Tick_Sequential::compute_ts_tick(size_t t) {
  // ... 现有计算 ...
  
  if (dag_.tick_data.lob.depth_updated) {
    dag_.l0.my_new_feature.compute();
    ts_features_buffer_[L0_FieldOffset::my_new_feature] = 
      dag_.l0.my_new_feature_.back();
  }
}
```

### 7.2 读取特征数据

```cpp
#include "features/backend/FeatureReader.hpp"

// 初始化读取器
FeatureReader reader("output/features");

// 预分配张量
DayTensor tensor;
tensor.preallocate(1000, {0, 1, 2, 3}); // A=1000, 选择特征0,1,2,3

// 读取数据
reader.load_day_level("20240101", 0, tensor);

// 访问数据
size_t t = 100;  // 时间索引
size_t f = 0;    // 特征索引 (ci_1)
size_t a = 10;   // 资产索引

float value = static_cast<float>(
  tensor.data[0][(t * tensor.F[0] + f) * tensor.A + a]
);
```

### 7.3 计算截面特征

```cpp
// 在 Minute_Crosssection::compute_and_store() 中
void compute_and_store(size_t t_minute) {
  const size_t A = input_fp32_.size();
  
  // 1. 读取所有资产的TS特征
  const _Float16 *input = CS_READ_ALL(
    store_, date_str_, 1, t_minute, L1_FieldOffset::min_ret_z
  );
  
  // 2. 转换为float32
  for (size_t a = 0; a < A; ++a)
    input_fp32_[a] = input[a];
  
  // 3. 构建有效索引
  valid_indices_.clear();
  for (size_t a = 0; a < A; ++a) {
    if (input_fp32_[a] != 0.0f) // 或其他有效性检查
      valid_indices_.push_back(a);
  }
  
  // 4. 计算排名并转换为逆正态分布
  std::fill(output_fp32_.begin(), output_fp32_.end(), 0.0f);
  compute_rank_inverse_normal_sparse(
    input_fp32_.data(), valid_indices_, output_fp32_.data()
  );
  
  // 5. 转换回Float16并写入
  for (size_t a = 0; a < A; ++a)
    output_fp16_[a] = output_fp32_[a];
  
  CS_WRITE_ALL(
    store_, date_str_, 1, t_minute, 
    L1_FieldOffset::cs_min_return_rank, 
    output_fp16_.data(), A
  );
}
```

---

## 8. 关键设计理念

### 8.1 事件驱动计算
- 特征计算由LOB事件触发（订单到达、盘口更新）
- 避免无效计算，提高效率

### 8.2 分层计算
- L0 (秒级) → L1 (分钟级) → L2 (小时级，未来)
- 每层独立计算，通过重采样器连接

### 8.3 时序/截面分离
- TS: 单资产时间序列，可并行
- CS: 跨资产横截面，需等待TS完成

### 8.4 内存布局优化
- `[T][F][A]` 布局：适合TS写入（连续写F个特征）
- 列式存储（L0）：适合按特征查询

### 8.5 稀疏性处理
- 事件驱动稀疏性：只在有数据时计算
- `_data_valid` 标志：标记有效数据点

---

## 9. 性能优化要点

1. **批量写入**: 使用 `TS_WRITE_FEATURES` 而非多次 `TS_WRITE_SINGLE`
2. **零拷贝**: DAG使用引用和指针，避免数据复制
3. **缓存友好**: CBuffer循环缓冲区，局部性好
4. **并行计算**: TS workers并行，CS worker单线程等待
5. **压缩存储**: Zstd压缩，减少磁盘I/O

---

## 10. 总结

特征系统是一个**分层、事件驱动、并行计算**的架构：

- **定义**: 通过宏定义，编译期生成所有元数据
- **计算**: DAG计算图，算子式设计，易于扩展
- **存储**: 3D张量 `[T][F][A]`，内存+磁盘两级
- **读取**: 列式（L0）和合并（L1）两种格式
- **同步**: TS并行，CS等待，细粒度同步

这个设计既保证了计算效率，又提供了良好的扩展性和可维护性。
