# STK 量化交易系统 - 完整工作流程详解

## 📋 目录

1. [系统启动流程](#1-系统启动流程)
2. [GUI 主循环](#2-gui-主循环)
3. [数据流处理流程](#3-数据流处理流程)
4. [特征计算流程](#4-特征计算流程)
5. [多线程工作流](#5-多线程工作流)
6. [存储与读取流程](#6-存储与读取流程)
7. [GUI 任务系统](#7-gui-任务系统)

---

## 1. 系统启动流程

### 1.1 入口点链

```
run.py 
  └─> py/main.py (构建脚本)
      └─> cpp/projects/main/build.sh (CMake 构建)
          └─> CMake 编译
              └─> app_main (C++ 可执行文件)
                  └─> main() → GUI::RunGUI()
```

### 1.2 启动步骤详解

#### 步骤 1: run.py 主入口
```python
# run.py
main():
  1. 清理旧进程 (pkill/taskkill)
  2. 调用 py/main.py 构建
  3. 运行编译后的二进制文件
```

**运行模式**：
- `TSAN`: 线程竞争检测（5-15x 慢）
- `DEBUG`: 交互式调试（很慢）
- `PROFILE`: CPU 性能分析（Tracy）
- `ASSERT`: 优化构建 + 断言
- `PRODUCTION`: 最大性能（-O3）

#### 步骤 2: py/main.py 构建
```python
# py/main.py
build_main_project():
  1. 设置 CMake 参数（编译器、构建类型）
  2. 配置模式标志（TSAN/PROFILE/DEBUG/ASSERT）
  3. 运行 CMake configure
  4. 运行 CMake build (并行编译)
  5. 复制 compile_commands.json (用于 clangd)
```

#### 步骤 3: C++ 程序入口
```cpp
// cpp/src/main.cpp
main():
  1. Windows: 设置 UTF-8 控制台
  2. 调用 GUI::RunGUI()
```

#### 步骤 4: GUI 初始化
```cpp
// cpp/src/gui/Run_OpenGL.cpp (或 Run_Vulkan.cpp)
RunGUI():
  1. 初始化 SharedData (全局数据容器)
  2. 设置全局终端日志句柄
  3. 设置配置重初始化回调
  4. 创建所有 GUI 任务
  5. 初始化图标栏（网络监控）
  6. 初始化 GLFW 窗口系统
  7. 设置 OpenGL/Vulkan 上下文
  8. 进入主循环
```

### 1.3 SharedData 初始化

```cpp
struct SharedData {
  Config config;              // 配置数据
  TaskState taskstate;        // 任务状态
  Asset asset;                // 资产列表
  AssetInfo assetinfo;        // 资产信息
  Feature feature;            // 特征数据
  OrderFlow orderflow;        // 订单流数据
  Dist dist;                  // 分布分析数据
  TimeSeries timeseries;      // 时间序列数据
  Transform transform;        // 变换数据
  CoroManager coromgr;        // 协程管理器
  TaskTerminal terminal;      // 终端日志
};
```

---

## 2. GUI 主循环

### 2.1 主循环结构

```cpp
// cpp/src/gui/Run_OpenGL.cpp
while (!glfwWindowShouldClose(window)) {
  1. glfwPollEvents()           // 处理窗口事件
  2. ImGui::NewFrame()          // 开始新帧
  3. DrawGUILayout()            // 绘制 GUI 布局
  4. ImGui::Render()            // 渲染 ImGui
  5. 交换缓冲区                 // 显示到屏幕
}
```

### 2.2 GUI 布局

```
┌─────────────────────────────────────────────────────────────┐
│ Tasks (左侧)              │ Panel (右侧)                   │
│  > Settings               │                                │
│  > SystemInfo             │                                │
│  > DataBase               │                                │
│  > Features               │                                │
│  > Factors                │                                │
│  ...                      │                                │
│                           │ Terminal (底部)                │
│ Icon: |Network|FPS|       │                                │
└─────────────────────────────────────────────────────────────┘
```

### 2.3 任务系统

每个任务是一个独立的模块：
- `TaskSettings`: 配置管理
- `TaskSystemInfo`: 系统监控
- `TaskDatabase`: 数据库管理
- `TaskFeatures`: 特征计算管理
- `TaskTerminal`: 终端日志

---

## 3. 数据流处理流程

### 3.1 完整数据流

```
原始 L2 数据文件 (.csv/.bin)
    ↓
[编码 Worker] 二进制编码 (.bin)
    ↓
[IO Worker] 文件扫描和索引
    ↓
[解码] 二进制 → L2 对象
    ↓
[LOB 重建] 限价订单簿维护
    ↓
[特征计算] Tick/分钟/小时级特征
    ↓
[存储] Zstd 压缩 + 列式存储
    ↓
特征文件 (.zst)
```

### 3.2 L2 数据类型

```cpp
// cpp/include/codec/L2_DataType.hpp
L2::Snapshot    // 快照数据
L2::Trade       // 逐笔成交
L2::Order       // 逐笔委托
```

### 3.3 数据编码流程

```cpp
// cpp/include/codec/binary_encoder_L2.hpp
CSV/原始数据 → L2 对象 → 二进制编码 → .bin 文件
```

**编码格式**：
- 二进制格式，高效存储
- 支持快照、逐笔成交、逐笔委托

### 3.4 LOB 重建

```cpp
// cpp/include/lob/LimitOrderBook.hpp
1. 初始化空订单簿
2. 处理逐笔委托（挂单/撤单/改单）
3. 处理逐笔成交（撮合）
4. 维护买卖盘状态
5. 生成快照数据
```

**关键操作**：
- `add_order()`: 添加订单
- `cancel_order()`: 撤单
- `modify_order()`: 改单
- `match_trade()`: 撮合成交
- `get_snapshot()`: 获取快照

---

## 4. 特征计算流程

### 4.1 特征计算层次

```
LEVEL 0 (Tick级): 逐笔粒度特征
    ↓
LEVEL 1 (分钟级): 分钟 K 线聚合特征
    ↓
LEVEL 2 (小时级): 小时级统计特征 (已移除)
```

### 4.2 Tick 级特征计算

#### 4.2.1 计算触发时机

```cpp
// cpp/include/features/FeaturesTick/Tick_Sequential.hpp
on_depth_update():
  // 盘口更新时触发
  - DepthIndex, DepthData
  - MidPrice, MicroPrice, Spread
  - CI (累计失衡)
  - CWI (凸加权失衡)
  - DDI (距离折扣失衡)
  - TLR (前N档占比)
  - PARA (抛物线拟合)
  - GRAD (深度梯度)
  - ENTROPY (香农熵)
  - OFI (订单流失衡)

on_trade():
  // 成交时触发
  - TradePrice
  - 其他成交相关特征
```

#### 4.2.2 计算图 (DAG)

```cpp
// cpp/include/features/ComputeGraph.hpp
struct L0 {
  // 输入数据
  TickData &tick_data;
  CBuffer<float, L2::BLEN> BidQty_[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> AskQty_[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> BidPrice_[DEPTH_SIZE];
  CBuffer<float, L2::BLEN> AskPrice_[DEPTH_SIZE];
  
  // 特征计算器
  CI<1> ci_1{...};
  CI<5> ci_5{...};
  OFI<1> ofi_1{...};
  OFI<5> ofi_5{...};
  // ... 更多特征
  
  // 输出缓冲区
  CBuffer<float, L2::BLEN> ci_1_;
  CBuffer<float, L2::BLEN> ofi_1_;
  // ...
};
```

#### 4.2.3 特征计算示例 (OFI)

```cpp
// cpp/include/features/FeaturesTick/TS/OFI.hpp
OFI::compute():
  1. 遍历 N_LEVELS 档位
  2. 获取当前价格和数量
  3. 计算 ΔV_bid (根据价格变化方向)
  4. 计算 ΔV_ask (根据价格变化方向)
  5. 加权求和: OFI = Σ w_i * (ΔV_bid - ΔV_ask)
  6. 更新 prev 缓存
  7. 输出到缓冲区
```

### 4.3 分钟级特征计算

```cpp
// cpp/include/features/FeaturesMinute/Minute_Sequential.hpp
1. 从 Tick 级特征聚合
2. 计算分钟 K 线 (OHLC)
3. 计算分钟级统计特征
   - min_ret_z (分钟收益)
   - rv_5m_norm (5分钟波动率)
   - vwap_gap_pct (VWAP偏离)
   - momentum_15m (15分钟动量)
```

### 4.4 截面特征计算

```cpp
// cpp/include/features/CoreCrosssection.hpp
1. 收集所有资产的同一特征值
2. 计算截面统计量
   - Rank (排名)
   - Z-score (标准化)
   - Percentile (百分位)
3. 输出截面特征
```

---

## 5. 多线程工作流

### 5.1 Worker 架构

```
主线程 (GUI)
    │
    ├─> IO Worker (文件读写)
    ├─> Encoding Worker (数据编码)
    ├─> Sequential Worker (序列特征计算)
    └─> Crosssectional Worker (截面特征计算)
```

### 5.2 Worker 类型

#### 5.2.1 IO Worker
```cpp
// cpp/include/worker/io_worker.hpp
io_worker():
  1. 扫描 tensor pool
  2. 查找 CS_DONE 状态的 tensor
  3. 刷新到磁盘 (Zstd 压缩)
  4. 重置为 UNUSED
  5. 更新进度显示
```

**职责**：
- 异步文件 I/O
- 特征数据持久化
- 进度跟踪

#### 5.2.2 Sequential Worker
```cpp
// cpp/include/worker/sequential_worker.hpp
sequential_worker():
  1. 从共享状态获取日期列表
  2. 遍历每个日期
  3. 加载 L2 数据
  4. 重建 LOB
  5. 计算序列特征 (Tick/分钟级)
  6. 写入 tensor pool
  7. 标记为 CS_DONE
```

**处理流程**：
```
日期 1 → L2 数据 → LOB → 特征计算 → Tensor Pool
日期 2 → L2 数据 → LOB → 特征计算 → Tensor Pool
...
```

#### 5.2.3 Crosssectional Worker
```cpp
// cpp/include/worker/crosssectional_worker.hpp
crosssectional_worker():
  1. 等待序列特征完成
  2. 收集所有资产的同一特征
  3. 计算截面统计量
  4. 更新截面特征
```

### 5.3 共享状态

```cpp
// cpp/include/worker/shared_state.hpp
struct SharedState {
  std::vector<std::string> dates;      // 待处理日期列表
  std::atomic<size_t> next_date_idx;   // 下一个日期索引
  TensorPool tensor_pool;              // Tensor 池
  // ...
};
```

**线程安全**：
- 使用原子操作
- Lock-free 数据结构
- 无锁队列

---

## 6. 存储与读取流程

### 6.1 存储格式

#### 6.1.1 L0 (Tick级) - 列式存储
```
features_L0_f0.zst  [Header: T,1,A][Zstd compressed column 0]
features_L0_f1.zst  [Header: T,1,A][Zstd compressed column 1]
...
features_L0_fN.zst  [Header: T,1,A][Zstd compressed column N]
```

**优势**：
- 列式存储，支持选择性加载
- 适合分布分析（只需部分特征）

#### 6.1.2 L1 (分钟级) - 合并存储
```
features_L1.zst  [Header: T,F_L1,A][Zstd compressed merged data]
```

**优势**：
- 文件数量少
- 写入速度快

#### 6.1.3 Depth (深度数据) - 独立存储
```
depth.zst  [Header: T,F_depth,A][Zstd compressed data]
```

**用途**：
- OrderFlow GUI 可视化
- 订单簿形状分析

### 6.2 存储流程

```cpp
// cpp/include/features/backend/FeatureStore.hpp
FeatureStore::flush_tensor():
  1. 获取 tensor (CS_DONE 状态)
  2. 压缩数据 (Zstd)
  3. 写入文件头 (T, F, A)
  4. 写入压缩数据
  5. 刷新到磁盘
  6. 重置 tensor 状态
```

### 6.3 读取流程

#### 6.3.1 单日加载 (GUI 可视化)
```cpp
// cpp/include/features/backend/FeatureReader.hpp
FeatureReader::load_day_level():
  1. 构建文件路径
  2. 读取文件头 (获取实际 T, F, A)
  3. 读取压缩数据
  4. Zstd 解压
  5. 列式数据交织 (L0) 或直接加载 (L1)
  6. 返回 DayTensor
```

#### 6.3.2 批量月度加载 (分布分析)
```cpp
FeatureReader::load_month_columns():
  1. 列出月份所有日期
  2. 遍历每个日期
  3. 选择性加载指定特征列
  4. 拼接成月度 tensor
  5. 返回 MonthTensor
```

### 6.4 数据压缩

```cpp
// cpp/include/features/backend/ZstdHelper.hpp
ZstdHelper::compress():
  - 使用 Zstd 压缩算法
  - 高压缩比
  - 快速解压

ZstdHelper::decompress():
  - 解压到目标缓冲区
  - 支持流式解压
```

---

## 7. GUI 任务系统

### 7.1 任务架构

每个任务都是独立的模块，包含：
- **主界面**: 任务入口和布局
- **服务层**: 业务逻辑
- **UI 组件**: 界面元素
- **基础设施**: 数据访问

### 7.2 TaskDatabase (数据库管理)

#### 7.2.1 服务初始化
```cpp
// cpp/src/gui/task_database/TaskDatabase.cpp
InitializeServices():
  1. BaostockService (数据源服务)
  2. ScanService (文件扫描服务)
  3. EncodingService (编码服务)
  4. L2DatabaseService (L2 数据库服务)
  5. StateManager (状态管理)
```

#### 7.2.2 初始化流程
```cpp
StateManager::initialize():
  1. 从 assets.json 加载资产列表
  2. 构建股票代码列表
  3. 设置到 DataManager
  4. 触发数据库扫描
  5. 加载所有 JSON 文件
```

### 7.3 TaskFeatures (特征计算管理)

#### 7.3.1 服务层
```cpp
// cpp/include/gui/task_features/services/
ComputeService      // 特征计算服务
DataLoader          // 数据加载器
DistService         // 分布分析服务
TimeSeriesService   // 时序分析服务
TransformService    // 变换服务
```

#### 7.3.2 计算流程
```cpp
ComputeService::compute_features():
  1. 选择日期范围
  2. 选择资产列表
  3. 启动多线程计算
  4. 显示进度
  5. 完成后更新状态
```

#### 7.3.3 Transform 服务
```cpp
TransformService::load_block():
  1. 检查缓存
  2. 预分配缓冲区
  3. 遍历 block 中的日期
  4. 加载每日数据
  5. 提取指定特征
  6. 拼接成连续序列
  7. 更新缓存
```

### 7.4 协程系统

```cpp
// cpp/include/gui/coro/CoroManager.hpp
CoroManager:
  - 管理所有协程生命周期
  - 调度协程执行
  - 处理异步操作

// 示例: 网络请求协程
boost::asio::co_spawn(
  io_context,
  async_network_request(),
  boost::asio::detached
);
```

**用途**：
- 异步网络请求
- 文件 I/O
- 长时间运行的任务

---

## 8. 关键数据结构

### 8.1 CBuffer (循环缓冲区)

```cpp
// cpp/include/define/CBuffer.hpp
template <typename T, size_t N>
class CBuffer {
  T data_[N];
  size_t head_ = 0;
  
  void push_back(T val);
  T back() const;
  // ...
};
```

**用途**：
- 特征值缓存
- 滑动窗口计算
- 历史数据维护

### 8.2 FeatureStore (特征存储)

```cpp
// cpp/include/features/backend/FeatureStore.hpp
class FeatureStore {
  TensorPool tensor_pool_;     // Tensor 池
  std::string base_dir_;        // 存储目录
  
  void flush_tensor(...);      // 刷新到磁盘
  // ...
};
```

### 8.3 SharedData (全局数据)

```cpp
// cpp/include/shared/SharedData.hpp
struct SharedData {
  Config config;                // 配置
  Feature feature;              // 特征数据
  OrderFlow orderflow;          // 订单流
  Dist dist;                    // 分布分析
  TimeSeries timeseries;        // 时间序列
  Transform transform;          // 变换
  CoroManager coromgr;         // 协程管理器
  TaskTerminal terminal;        // 终端
};
```

---

## 9. 性能优化要点

### 9.1 内存管理
- **内存池**: 减少分配开销
- **循环缓冲区**: 避免频繁分配
- **预分配**: 提前分配缓冲区

### 9.2 并发处理
- **多线程**: 并行计算特征
- **Lock-free**: 无锁数据结构
- **异步 I/O**: 非阻塞文件操作

### 9.3 存储优化
- **Zstd 压缩**: 高压缩比
- **列式存储**: 选择性加载
- **Float16**: 减少存储空间

### 9.4 计算优化
- **模板特化**: 编译期优化
- **SIMD**: 向量化计算
- **缓存友好**: 数据局部性

---

## 10. 典型使用场景

### 10.1 特征计算流程

```
1. 打开 GUI
2. 进入 TaskDatabase
3. 扫描数据库，确认数据完整性
4. 进入 TaskFeatures
5. 选择日期范围和资产
6. 点击"计算特征"
7. 等待计算完成
8. 查看特征数据
```

### 10.2 特征分析流程

```
1. 进入 TaskFeatures → TabFeature
2. 选择特征和资产
3. 查看特征时序图
4. 进入 TabDist
5. 选择特征和日期范围
6. 查看分布分析
7. 进入 TabTransform
8. 进行平稳化和归一化
9. 查看变换后的特征
```

### 10.3 订单流分析流程

```
1. 进入 TaskFeatures → TabOrderFlow
2. 选择日期和资产
3. 加载深度数据
4. 查看订单簿可视化
5. 分析订单流模式
```

---

## 11. 总结

### 11.1 核心流程

1. **启动**: run.py → 构建 → GUI 初始化
2. **数据流**: L2 数据 → 编码 → LOB 重建 → 特征计算 → 存储
3. **计算**: 多线程 Worker → 序列特征 → 截面特征 → 持久化
4. **交互**: GUI 任务 → 服务层 → 数据访问 → 可视化

### 11.2 关键特性

- ✅ **高性能**: 多线程、内存池、SIMD
- ✅ **可扩展**: 模块化设计、插件式特征
- ✅ **易用性**: GUI 界面、实时反馈
- ✅ **可靠性**: 错误处理、状态管理

### 11.3 技术栈

- **语言**: C++23, Python
- **GUI**: ImGui, ImPlot, GLFW
- **并发**: Boost.Asio, 协程
- **存储**: Zstd 压缩, 列式存储
- **构建**: CMake, Ninja

---

**文档版本**: 1.0  
**最后更新**: 2026-01-17  
**维护者**: Finance Quant Agent
