# 更新内容详细分析

## 更新概览

从 `8b7a017` 到 `dc06013`，共合并了 **16 个新提交**，主要更新包括：

- **62 个文件变更**
- **+4129 行新增**
- **-2278 行删除**
- **净增 +1851 行代码**

---

## 一、特征工程重大重构

### 1.1 特征定义系统重构 (`FeaturesDefine.hpp`)

#### 主要变化：
1. **格式调整**：特征定义格式从 `(code, width, valid_type, data_type, cat_l1, cat_l2, norm_method, PSD, formula, name_en, name_cn, description)` 
   改为 `(code, width, valid_type, data_type, cat_l1, cat_l2, norm_method, PSD, name_en, name_cn, description, formula)`
   - **公式位置后移**：将公式从第9位移到第12位，提高可读性

2. **特征分类重组**：
   - 新增分类注释：`SD - Structural Depth Features (深度结构特征)`
   - 新增分类注释：`DF - Dynamic Order Flow Features (订单流动态特征)`
   - 新增分类注释：`BH - Behavioral & Strategic Features (行为与策略特征)`
   - 新增分类注释：`CD - Clustering & Dependency Features (事件聚集与依赖特征)`
   - 新增分类注释：`RS - Resiliency & Replenishment Features (韧性与恢复特征)`
   - 新增分类注释：`IC - Impact & Liquidity Cost Features (价格冲击与流动性成本特征)`
   - 新增分类注释：`AN - Anomaly & Structural Outlier Features (异常与结构失衡特征)`

### 1.2 新增特征类型

#### 深度结构特征 (SD - Structural Depth Features)

1. **CI (Cumulative Imbalance) - 累计失衡**
   - `ci_1`, `ci_5`, `ci_10`, `ci_30`, `ci_all`
   - 计算前N档的累计买卖失衡率
   - 公式：`CI_N = (Σ V_bid[1:N] - Σ V_ask[1:N]) / (Σ V_bid[1:N] + Σ V_ask[1:N])`
   - **用途**：捕捉不同深度档位的订单失衡

2. **CWI (Convexity-weighted Imbalance) - 凸加权失衡**
   - `cwi_1` (γ=1), `cwi_2` (γ=2)
   - 考虑全量，但近端更高权重（按档位）
   - 权重：`w_i = 1/(i+ε)^γ`
   - **用途**：更关注近端订单，捕捉短期失衡

3. **DDI (Distance-discounted Imbalance) - 距离折扣失衡**
   - `ddi_1` (λ=0.01), `ddi_2` (λ=0.02)
   - 考虑全量，但近端更高权重（按距离）
   - 权重：`w_i = e^(-λ * Δp_i)`
   - **用途**：基于价格距离的加权失衡

4. **TBR/TAR (Top Bid/Ask Ratio) - 前N档占比**
   - `tbr_5`, `tar_5`
   - 前5档买单/卖单占比
   - **用途**：判断单侧是否容易被击穿

5. **PARA (Parabola Fit) - 抛物线拟合**
   - `b_para_c0`, `b_para_c1`, `b_para_c2` (买侧)
   - `a_para_c0`, `a_para_c1`, `a_para_c2` (卖侧)
   - `imba_para_c0`, `imba_para_c1`, `imba_para_c2` (失衡)
   - 对深度做二次拟合：`V_i ~ c0 + c1*i + c2*i^2`
     - `c0`: 截距（近端流动性）
     - `c1`: 斜率（风偏方向）
     - `c2`: 曲率（<0: 近端订单块；>0: 做市类挂单）
   - **用途**：捕捉订单簿形状特征

6. **GRAD (Gradient) - 深度梯度**
   - `b_5_c1`, `a_5_c1`, `imba_5_c1`
   - 前N档的平均梯度（一阶差分均值）
   - 公式：`GRAD_N = (1/(N-1)) * Σ (V_{i+1} - V_i)`
   - **用途**：捕捉订单簿斜率特征

7. **ENTROPY (Shannon Entropy) - 香农熵**
   - `b_5_entropy`, `a_5_entropy`, `imba_5_entropy`
   - `b_30_entropy`, `a_30_entropy`, `imba_30_entropy`
   - 深度分布香农熵：`H = -Σ π_i * log(π_i)`
   - **用途**：
     - 0: 极端集中（单档占全部）
     - ln(N): 极端均匀（各档相等）
   - 捕捉订单分布的集中度

#### 订单流动态特征 (DF - Dynamic Order Flow Features)

1. **OFI (Order Flow Imbalance) - 订单流失衡**
   - `ofi_1`, `ofi_5`
   - 委托量增量变化的差异，捕捉订单流动态
   - 公式：
     - `ΔV_bid`: price↓→0, price=→V-Vprev, price↑→V
     - `ΔV_ask`: price↓→V, price=→V-Vprev, price↑→0
     - `OFI = ΔV_bid - ΔV_ask`
   - **用途**：对近端大单挂单变动非常敏感

### 1.3 删除的旧特征

以下特征被移除（可能被新特征替代或重构）：

- `voi1`, `voi30` (Vol Order Imba) - 可能被 OFI 替代
- `oir5`, `oir10` (Order Imba Ratio) - 可能被 CI 替代
- `soir5`, `soir5s`, `soir10s`, `soir30s` (SOIR) - 可能被新的失衡特征替代
- `mpb` (Mid-Price Basis) - 市价偏离度
- `mpc1`, `mpc5`, `mpc5_max`, `mpc5_skew` (MPC) - 中间价变化相关特征

### 1.4 文件结构重组

#### 基础特征移到 `base/` 目录：
- `DeltaT.hpp` → `base/DeltaT.hpp`
- `DepthData.hpp` → `base/DepthData.hpp`
- `DepthIndex.hpp` → `base/DepthIndex.hpp`
- `MicroPrice.hpp` → `base/MicroPrice.hpp`
- `MidPrice.hpp` → `base/MidPrice.hpp`
- `Spread.hpp` → `base/Spread.hpp`
- `TickIndex.hpp` → `base/TickIndex.hpp`
- `TradePrice.hpp` → `base/TradePrice.hpp`

**目的**：更好的代码组织，区分基础工具类和业务特征类

### 1.5 小时级特征移除

- 删除了 `FeaturesHour/Hour_Crosssection.hpp`
- 删除了 `FeaturesHour/Hour_Sequential.hpp`
- 删除了 `FeaturesHour/TS/HourIndex.hpp`
- 删除了 `LEVEL_2_FIELDS` 定义

**原因**：可能暂时不需要小时级特征，或准备重构

---

## 二、数学库增强

### 2.1 新增算子框架 (`math/Operator.hpp`)

- **算子定义系统**：统一的算子元数据和参数管理
- **编译期优化**：支持常量内联和运行时动态参数
- **用途**：为 Transform 系统提供统一的算子接口

### 2.2 新增 FIR 带通滤波器 (`math/spectral/FIRBandpass.hpp`)

- **窗函数法设计**：支持 Hann、Hamming、Blackman 窗
- **归一化**：在中心频率处归一化增益为 1
- **用途**：频域滤波，提取特定频率范围的信号

### 2.3 新增 IIR 带通滤波器 (`math/spectral/IIRBandpass.hpp`)

- **IIR 滤波器实现**：无限脉冲响应滤波器
- **用途**：相比 FIR 更高效，适合实时处理

### 2.4 多分辨率功率谱密度增强 (`math/spectral/MultiResPSD.hpp`)

- **大幅增强**：+312 行代码
- **用途**：多时间尺度的频谱分析

### 2.5 平稳化方法增强

- **FracDiff** (分数差分)：+14 行
- **IntDiff** (整数差分)：+11 行
- **MADetrend** (移动平均去趋势)：+11 行

### 2.6 归一化方法重构 (`math/normalize/Normalize.hpp`)

- **大幅重构**：+202 行变更
- **用途**：改进归一化方法的实现和性能

---

## 三、Transform 系统重大更新

### 3.1 Transform 定义扩展 (`shared/Transform.hpp`)

- **+241 行变更**
- **新增算子支持**：集成新的算子框架
- **增强功能**：支持更多变换类型

### 3.2 TransformService 重构 (`gui/task_features/services/TransformService.hpp`)

- **+53 行变更**
- **服务层改进**：更好的业务逻辑封装

### 3.3 TransformService 实现大幅增强 (`cpp/src/gui/task_features/services/TransformService.cpp`)

- **+701 行变更**
- **功能增强**：
  - 支持新的滤波器
  - 改进平稳化处理
  - 增强归一化方法
  - 优化性能

### 3.4 TabTransform UI 大幅增强 (`cpp/src/gui/task_features/ui/TabTransform.cpp`)

- **+1312 行变更**（最大单文件变更）
- **UI 功能增强**：
  - 新增滤波器配置界面
  - 改进平稳化参数调整
  - 增强可视化功能
  - 优化交互体验

### 3.5 TabTransform 头文件更新 (`cpp/include/gui/task_features/ui/TabTransform.hpp`)

- **+40 行变更**
- **新增状态管理**：
  - 带通光标控制
  - 特征图轴限制同步
  - 渲染缓存优化

---

## 四、特征计算核心更新

### 4.1 ComputeGraph 增强 (`features/ComputeGraph.hpp`)

- **+190 行变更**
- **功能**：
  - 支持新的特征类型
  - 优化计算图构建
  - 改进依赖管理

### 4.2 CoreCrosssection 更新 (`features/CoreCrosssection.hpp`)

- **+27 行变更**
- **功能**：截面特征计算增强

### 4.3 CoreSequential 更新 (`features/CoreSequential.hpp`)

- **+30 行变更**
- **功能**：序列特征计算增强

### 4.4 Tick_Sequential 大幅更新 (`features/FeaturesTick/Tick_Sequential.hpp`)

- **+130 行变更**
- **功能**：
  - 集成新的特征类
  - 优化计算流程
  - 改进性能

### 4.5 DataDefine 更新 (`features/DataDefine.hpp`)

- **+23 行变更**
- **功能**：数据类型定义扩展

---

## 五、特征存储后端更新

### 5.1 FeatureReader 更新 (`features/backend/FeatureReader.hpp`)

- **+9 行变更**
- **注释更新**：L1/L2 改为 L1（可能是笔误修复）

### 5.2 FeatureStore 更新 (`features/backend/FeatureStore.hpp`)

- **+8 行变更**
- **功能**：存储逻辑优化

### 5.3 FeatureStoreConfig 更新 (`features/backend/FeatureStoreConfig.hpp`)

- **+22 行变更**
- **功能**：配置选项扩展

### 5.4 FeatureStore README 更新 (`features/backend/README`)

- **+10 行变更**
- **文档**：更新存储格式说明

---

## 六、GUI 和工具更新

### 6.1 TabFeature 更新 (`cpp/src/gui/task_features/ui/TabFeature.cpp`)

- **+24 行变更**
- **功能**：特征标签页功能增强

### 6.2 TabCompute 更新 (`cpp/src/gui/task_features/ui/TabCompute.cpp`)

- **+2 行变更**
- **功能**：计算标签页小调整

### 6.3 TaskFeatures 主界面更新 (`cpp/src/gui/task_features/TaskFeatures.cpp`)

- **+28 行变更**
- **功能**：主界面功能增强

### 6.4 Feature 数据结构更新 (`shared/Feature.hpp`)

- **+11 行变更**
- **功能**：特征数据结构扩展

### 6.5 TimeSeries 更新 (`cpp/src/shared/TimeSeries.cpp`)

- **+9 行变更**
- **功能**：时间序列处理增强

### 6.6 Dist 更新 (`cpp/src/shared/Dist.cpp`)

- **+21 行变更**
- **功能**：分布分析功能增强

---

## 七、构建和工具更新

### 7.1 CMakeLists 更新 (`cpp/projects/main/CMakeLists.txt`)

- **+3 行变更**
- **功能**：可能添加了新的依赖或编译选项

### 7.2 调试模式增强 (`py/mode_debug.py`)

- **+143 行变更**
- **功能**：
  - 增强调试功能
  - 改进错误处理
  - 新增调试工具

### 7.3 主入口更新 (`run.py`)

- **+4 行变更**
- **功能**：运行脚本小调整

### 7.4 新增工具 (`py/app/procdump/procdump64.exe`)

- **新增二进制文件**：720KB
- **用途**：Windows 进程转储工具（用于调试）

---

## 八、README 更新

- **-1 行变更**
- **内容**：可能移除了某些说明

---

## 总结

### 主要改进方向

1. **特征工程系统重构**
   - 更科学的特征分类
   - 新增大量深度结构特征
   - 新增订单流动态特征
   - 移除过时特征

2. **数学库大幅增强**
   - 新增 FIR/IIR 滤波器
   - 算子框架统一化
   - 平稳化和归一化方法增强

3. **Transform 系统重大升级**
   - UI 功能大幅增强
   - 服务层重构
   - 支持更多变换类型

4. **代码组织优化**
   - 基础特征移到 base/ 目录
   - 更好的模块划分

### 影响评估

- **正面影响**：
  - 特征体系更完善
  - 数学工具更强大
  - UI 体验更好
  - 代码组织更清晰

- **需要注意**：
  - 小时级特征被移除（如需使用需重新实现）
  - 部分旧特征被移除（需检查是否有依赖）
  - Transform 系统大幅变更（可能需要重新学习）

### 建议

1. **测试新特征**：重点测试新增的深度结构特征和订单流特征
2. **验证 Transform**：测试新的滤波器和平稳化方法
3. **检查依赖**：确认被移除的特征是否有其他地方在使用
4. **性能测试**：验证新代码的性能影响

---

**分析日期**: 2026-01-17  
**分析范围**: `8b7a017` → `dc06013`  
**总变更**: 62 文件, +4129/-2278 行
