# 钩沉 (Gouchen) 前端设计文档

**日期**: 2026-04-09  
**目标**: 为 Gouchen NL2SQL 项目构建用户对话式查询界面（第一阶段），并为后续管理界面预留扩展空间。

---

## 1. 设计目标

- **当前阶段**: 提供一个对话式查询界面，支持自然语言输入、歧义澄清、SQL 审核、结果展示（表格 + 自动图表）。
- **后续阶段**: 无缝扩展管理后台（审计日志、权限管理、查询统计等），不污染现有聊天页代码。

## 2. 技术选型

| 层级 | 技术 | 说明 |
|------|------|------|
| 构建工具 | Vite + React + TypeScript | 配置极简，热更新快，适合前端新手入门 |
| 样式 | Tailwind CSS | 原子化 CSS，写样式直观 |
| 组件库 | shadcn/ui | 高质量开箱即用组件（Button, Collapsible, Card, Table 等） |
| 图表 | Recharts | 基于 React 声明式的图表库，易于自动推断图表类型 |
| 路由 | react-router-dom | 当前仅 ChatPage，后续通过子路由扩展 AdminPage |
| 测试 | Vitest + React Testing Library + MSW | 组件测试 + API 集成测试 |
| 部署 | 前后端分离 | React SPA 独立部署（Nginx / Vercel /任意 CDN），通过 CORS 访问后端 FastAPI |

## 3. 项目目录结构

独立的前端项目目录，建议位于后端项目同级：

```
gouchen-frontend/
├── src/
│   ├── api/
│   │   └── query.ts              # FastAPI 调用封装
│   ├── components/
│   │   ├── ChatMessage.tsx       # 单条消息气泡
│   │   ├── SqlPanel.tsx          # 可折叠 SQL 展示
│   │   ├── DataTable.tsx         # 结果表格
│   │   ├── AutoChart.tsx         # 自动图表推断与渲染
│   │   ├── ClarificationForm.tsx # 歧义澄清表单
│   │   ├── ApprovalPanel.tsx     # SQL 审核面板
│   │   └── Sidebar.tsx           # 左侧会话栏
│   ├── pages/
│   │   ├── ChatPage.tsx          # 主聊天页面
│   │   └── AdminPage.tsx         # 预留：管理后台
│   ├── hooks/
│   │   └── useQuery.ts           # 查询状态机 Hook
│   ├── types/
│   │   └── api.ts                # TypeScript 类型定义
│   ├── utils/
│   │   └── chartHelper.ts        # 图表推断逻辑
│   ├── App.tsx                   # 路由入口
│   └── main.tsx                  # 应用挂载点
├── package.json
├── vite.config.ts
└── tailwind.config.js
```

## 4. 界面设计

### 4.1 布局

**三栏混合布局**：
- **左侧边栏 (Sidebar)**: 会话历史列表（底部预留"管理后台"入口）。
- **主聊天区 (ChatPage)**: 对话流，用户消息靠右，AI 消息靠左。
- **底部输入框**: 固定在页面底部。

**AI 消息默认展示**：
1. 文字总结
2. 数据表格 / 自动图表（优先可视化）
3. 折叠面板：生成的 SQL、执行详情、澄清历史（默认收起）

### 4.2 关键交互

- **SQL 审核中断**: 当后端返回 `pending_approval` 时，AI 消息区域变为 `ApprovalPanel`，展示 SQL + 解释 + 通过/拒绝/编辑后通过 三个操作。
- **歧义澄清中断**: 当后端返回 `needs_clarification` 时，在当前对话流中插入 `ClarificationForm`，用户填写后调用 `/clarify` 继续。
- **新对话**: 生成新的 `thread_id`，Sidebar 更新列表，`localStorage` 持久化会话标题和 ID。
- **恢复会话**: 点击历史会话时，调用 `GET /status/{thread_id}` 恢复状态。

## 5. 数据流

### 5.1 查询状态机

```
用户输入 → useQuery: idle → submitting
                ↓
          POST /api/v1/query
                ↓
      ┌──────────┼──────────┐
  completed  needs_clarification  pending_approval
      ↓            ↓                  ↓
 展示结果   ClarificationForm    ApprovalPanel
                ↓                  ↓
         POST /clarify        POST /approve
                ↓                  ↓
           (回到判断)           (回到判断)
```

### 5.2 会话管理

- `thread_id` 由前端生成（UUID）。
- 会话列表存储在 `localStorage`，标题取自用户第一条输入的前 20 字。
- 页面刷新后，通过 `/status/{thread_id}` 恢复当前中断状态（澄清或审核）。

### 5.3 跨域配置

FastAPI 后端需增加 `CORSMiddleware`，允许前端部署域名访问 API：

```python
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "https://your-frontend-domain.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

## 6. 核心组件详细说明

### 6.1 ChatMessage
- 区分 `role: user`（靠右，灰色背景）和 `role: assistant`（靠左，蓝色背景）。
- AI 消息内部可嵌套 `AutoChart`、`DataTable`、`SqlPanel`。

### 6.2 AutoChart
- 接收 `execution_result` 的表格数据。
- **推断规则**（按优先级）：
  1. 含日期/时间列 + 数值列 → `LineChart`
  2. 含分类列 + 数值列 → `BarChart`
  3. 仅两列且为占比关系 → `PieChart`
  4. 多列数值 → 以第一分类列为 X 轴的 `BarChart`
- **降级策略**: 无法推断时，隐藏图表，仅渲染 `DataTable`。

### 6.3 ClarificationForm
- 接收 `clarification_info.questions` 数组。
- 每个问题渲染为输入框或单选。
- 提交时打包为 `ClarifyRequest` 格式。

### 6.4 ApprovalPanel
- 展示 `generated_sql`（带语法高亮）和 `sql_explanation`。
- 提供操作按钮:
  - **通过**: `decision: "approve"`
  - **拒绝**: `decision: "reject"`
  - **编辑后通过**: 弹出可编辑的 SQL 代码框，`decision: "approve"` + `edited_sql`

### 6.5 Sidebar
- 顶部: "+ 新对话" 按钮。
- 中部: 历史会话列表（标题 + 时间）。
- 底部: 设置入口 / 预留管理后台链接。

## 7. 错误处理

1. **网络错误**: 在对话流中插入系统错误消息（红色提示），状态回退到可重试。
2. **后端业务错误**: 后端返回 `status: "error"` 时，直接展示 `error` 字段文本。
3. **图表渲染失败**: `AutoChart` 推断失败不抛异常，优雅降级为表格。
4. **状态恢复失败**: 调用 `/status/{thread_id}` 报错时，提示用户"该会话已过期或丢失"。

## 8. 测试策略

### 8.1 组件测试 (Vitest + RTL)
- `ClarificationForm`: 验证表单渲染与提交回调参数。
- `ApprovalPanel`: 验证三种操作按钮触发对应事件。
- `AutoChart`: 验证数据-图表类型映射和降级行为。

### 8.2 API 集成测试 (MSW)
- 模拟完整流程: `query → needs_clarification → clarify → completed`。
- 验证 `useQuery` Hook 的状态跳转和 API 调用次数。

### 8.3 E2E 测试 (Playwright，后续补充)
- 覆盖黄金路径: 用户在输入框输入 → 点击发送 → 看到结果表格。

### 8.4 联调测试
- 前端 `npm run dev` + 后端 `python -m src.main` 本地联调，验证 CORS、数据格式、中断恢复。

## 9. 预留扩展点

- **路由扩展**: `App.tsx` 中使用 `react-router-dom`，后续加 `/admin` 路由即可引入 `AdminPage`。
- **权限扩展**: 当前 `user_role` 固定传入 `"analyst"`，后续可接入登录态。
- **多数据源切换**: 当前不展示 datasource 选择器，UI 预留位置，后续开启。
- **主题/国际化**: Tailwind 的 dark mode 和 i18n 库在架构层面兼容，当前阶段默认中文亮色主题。
