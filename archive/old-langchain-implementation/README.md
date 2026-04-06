# 旧版本代码归档

## 说明

此目录包含钩沉 (Gouchen) NL2SQL 项目的旧版本实现，基于 **LangChain + Deep Agents** 架构。

## 归档时间

2025-04-06

## 归档原因

项目已迁移到 **LangGraph** 架构，原因：
1. 确定性流程控制更适合企业级 NL2SQL 场景
2. 原生支持 HITL (Human-in-the-Loop) 中断和恢复
3. 状态持久化通过 Checkpointer 实现，更简洁

## 文件清单

```
old-langchain-implementation/
├── agent.py              # Deep Agents 主入口 (create_deep_agent)
├── tools/
│   ├── intent_parser.py  # 意图解析 Tool
│   ├── schema_retriever.py  # Schema 检索 Tool
│   ├── sql_generator.py  # SQL 生成 Tool
│   └── sql_executor.py   # SQL 执行 Tool
└── README.md             # 本文件
```

## 与当前版本的区别

| 维度 | 旧版本 (LangChain/Deep Agents) | 当前版本 (LangGraph) |
|------|------------------------------|-------------------|
| 架构 | Agent + Tools | StateGraph + Nodes |
| 流程控制 | LLM 决定下一步 | 预定义图结构 |
| HITL | HumanInTheLoopMiddleware | interrupt() + Command(resume=...) |
| 状态管理 | StateBackend | TypedDict + Checkpointer |
| 确定性 | 低 (LLM 可能跳转错误) | 高 (代码控制流程) |

## 参考价值

如需参考旧实现：
- Tool 定义方式
- Prompt 设计
- Deep Agents 配置模式

## 当前版本路径

新版本代码位于：`gouchen/src/graph/`

---

**注意**：此目录代码不再维护，仅供历史参考。
