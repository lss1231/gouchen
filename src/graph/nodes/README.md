# LangGraph Nodes

本目录包含 NL2SQL pipeline 的 8 个核心节点。

## Pipeline 拓扑

```
START -> intent -> clarification -> schema -> generate_sql -> review -> execute -> summarizer -> formatter -> END
```

- `clarification` 支持最多 3 轮循环（条件路由）
- `execute` 出错时直接跳到 `END`，否则继续到 `summarizer`

## 节点职责

| 节点 | 文件 | 职责 |
|------|------|------|
| intent | `intent.py` | 解析用户查询意图，提取 `QueryIntent`（指标、维度、过滤条件、时间范围、分析类型） |
| clarification | `clarification.py` | 检测歧义，生成澄清问题；支持多轮对话 |
| schema | `schema.py` | 从 `SchemaStore` 检索相关 Doris 表结构 |
| generate_sql | `sql_generator.py` | 基于意图和表结构生成 Doris SQL |
| review | `review.py` | HITL 中断点：触发人工审核（`interrupt`） |
| execute | `executor.py` | 执行 SQL，处理成功/失败结果 |
| summarizer | `summarizer.py` | 将执行结果转换为自然语言总结 |
| formatter | `formatter.py` | 格式化输出，推荐图表类型（KPI/折线/柱状/饼图/表格） |

## 状态定义

所有节点通过 `NL2SQLState`（定义于 `../state.py`）共享数据。关键字段：

- `query`: 用户原始问题
- `intent`: `QueryIntent`，包含指标/维度/过滤/时间范围/分析类型
- `relevant_tables`: 检索到的相关表结构列表
- `generated_sql`: 生成的 SQL
- `sql_explanation`: SQL 解释
- `execution_result`: SQL 执行结果
- `summary`: 自然语言总结
- `formatted_result`: 格式化输出（含图表推荐）
- `error`: 错误信息
- `clarification_needed` / `clarification_questions` / `clarification_responses`: 澄清相关
- `needs_approval` / `approval_decision`: 审核相关

## 添加新节点的步骤

1. 在 `src/graph/nodes/` 下新建 `{node_name}.py`
2. 实现 `def {node_name}_node(state: NL2SQLState) -> dict:` 函数
3. 返回一个字典，包含你想写入 `NL2SQLState` 的字段更新
4. 在 `../builder.py` 中：
   - `from .nodes import {node_name}_node`
   - `workflow.add_node("{node_name}", wrap_node({node_name}_node, "{node_name}"))`
   - 添加合适的边（`add_edge` 或 `add_conditional_edges`）
5. 如需新增 state 字段，同步修改 `../state.py` 中的 `NL2SQLState`
6. 编写单元测试（参考 `tests/test_state.py`）

## 注意事项

- 所有节点都会被 `logging_middleware.wrap_node()` 包裹，自动记录 trace
- `review` 节点使用 `langgraph.types.interrupt` 实现 HITL 暂停
- `clarification` 和 `review` 的恢复通过 API 层的 `Command(resume=...)` 完成
- 保持相对导入：`from ..state import NL2SQLState`
