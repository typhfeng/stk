# Cursor IDE Agent 使用说明

本目录包含专为 Cursor IDE 设计的金融工程与量化交易 Agent。

## 文件说明

- `AGENT.md`: Agent 定义文件，包含 Agent 的能力、工作流程和使用说明

## 如何加载 Agent

### 方法 1: 使用 Cursor 的 Agent 功能（推荐）

1. **在 Cursor 中打开项目**
   - 确保项目根目录为 `/Users/sunkewei/finance/stk`

2. **加载 Agent**
   - 在 Cursor 的聊天界面中，Agent 会自动识别项目中的 `agent/AGENT.md` 文件
   - 或者直接引用: `@agent/AGENT.md` 来激活此 Agent

3. **使用 Agent**
   - 在 Cursor 的 AI 聊天中，Agent 会自动应用其专业能力
   - 可以询问量化交易相关问题，Agent 会基于项目上下文提供专业建议

### 方法 2: 通过 Cursor 设置

1. 打开 Cursor 设置 (Cmd/Ctrl + ,)
2. 搜索 "Agent" 或 "Rules"
3. 添加项目级别的 Agent 规则
4. 指向 `agent/AGENT.md` 文件

### 方法 3: 使用 .cursorrules 文件

在项目根目录创建或更新 `.cursorrules` 文件，引用 Agent:

```
请参考 agent/AGENT.md 中的金融工程与量化交易 Agent 定义。
```

## Agent 特性

本 Agent 专为量化交易系统设计，具备以下能力：

- ✅ 深度理解 L2 行情数据、LOB、订单流等数据结构
- ✅ 支持多时间尺度特征工程（Tick/分钟/小时级）
- ✅ 因子挖掘和验证能力
- ✅ 策略开发和回测支持
- ✅ 性能优化建议
- ✅ 代码生成和审查

## 使用示例

### 示例 1: 开发新特征
```
用户: 帮我实现一个计算订单簿斜率的新特征
Agent: 会理解项目结构，生成符合规范的代码
```

### 示例 2: 因子验证
```
用户: 检查这个因子的分布稳定性
Agent: 会提供验证代码和标准
```

### 示例 3: 性能优化
```
用户: 这个特征计算太慢了，如何优化？
Agent: 会分析代码并提供优化建议
```

## 注意事项

- Agent 会自动理解项目上下文
- 所有代码生成会遵循项目规范
- 支持中文交互
- 充分利用 Cursor 的智能功能

## 更新 Agent

如需更新 Agent 定义，直接编辑 `agent/AGENT.md` 文件即可。Cursor 会自动识别更新。
