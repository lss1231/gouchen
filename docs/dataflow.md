# 钩沉 NL2SQL 数据流文档

本文档详细描述了钩沉系统中自然语言到SQL查询的完整数据流转过程。

---

## 一、总体架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              用户 (自然语言查询)                               │
│                         "上个月华东区的销售额是多少"                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ ① API 入口层 (src/main.py)                                                  │
│    FastAPI → POST /api/v1/query                                             │
│    ├─ Lifespan: 启动时加载 schema_store (data/schema/ecommerce_schema.json)  │
│    └─ 索引 6 张表到内存 (dim_date, dim_region, dim_category, dim_product,    │
│                    fact_order, fact_order_item)                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ ② 工作流编排层 (src/api/routes/query.py)                                     │
│                                                                              │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │ Step 1: Intent Parser (意图解析)                                     │   │
│   │ ─────────────────────────────────                                   │   │
│   │ Input:  "上个月华东区的销售额是多少"                                  │   │
│   │ Tool:   parse_intent.invoke()                                       │   │
│   │ LLM:    Kimi API (temperature=0.1)                                  │   │
│   │ Output: QueryIntent JSON                                            │   │
│   │ {                                                                   │   │
│   │   "metrics": ["销售额"],                                             │   │
│   │   "dimensions": ["地区"],                                            │   │
│   │   "filters": [{"region": "华东区"}],                                  │   │
│   │   "time_range": {"type": "last_month"},                             │   │
│   │   "aggregation": "sum",                                             │   │
│   │   "limit": 1000                                                     │   │
│   │ }                                                                   │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                      │                                       │
│                                      ▼                                       │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │ Step 2: Schema Retriever (表结构检索)                                │   │
│   │ ─────────────────────────────────                                   │   │
│   │ Input:  QueryIntent                                                 │   │
│   │ Tool:   retrieve_schema.invoke()                                    │   │
│   │ Engine: SchemaStore (关键词匹配, 非向量)                              │   │
│   │         ├─ _extract_keywords(): "销售额"→["order","amount","sales"]  │   │
│   │         ├─ _score_table(): 匹配 fact_order (score=3)                │   │
│   │         └─ _score_table(): 匹配 dim_region (score=3)                │   │
│   │ Output: TableMetadata[] JSON                                        │   │
│   │ {                                                                   │   │
│   │   "tables": [{                                                      │   │
│   │     "table_name": "fact_order",                                     │   │
│   │     "datasource": "mysql",  ◄── 关键! 从这里获取正确数据源            │   │
│   │     "fields": [...]                                                 │   │
│   │   }]                                                                │   │
│   │ }                                                                   │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                      │                                       │
│                                      ▼                                       │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │ Step 3: SQL Generator (SQL生成)                                      │   │
│   │ ─────────────────────────────────                                   │   │
│   │ Input:  intent_json + schema_json                                   │   │
│   │ Tool:   generate_sql.invoke()                                       │   │
│   │ LLM:    Kimi API + SQL_GENERATOR_PROMPT                             │   │
│   │                                                                              │
│   │ 安全层 (双重验证):                                                    │   │
│   │ ├─ LLM Prompt: "只允许生成SELECT语句"                                │   │
│   │ ├─ _is_safe_sql(): 检查 SQL 以 SELECT 开头, 无危险关键词               │   │
│   │ └─ 强制覆盖 datasource: 从 schema.tables[0].datasource 获取            │   │
│   │                                                                              │
│   │ Output: GeneratedSQL JSON                                           │   │
│   │ {                                                                   │   │
│   │   "sql": "SELECT SUM(paid_amount) AS 销售额... ",                     │   │
│   │   "datasource": "mysql",   ◄── 强制从 schema 获取                     │   │
│   │   "tables": ["fact_order", "dim_region"],                           │   │
│   │   "explanation": "查询上个月华东区的销售总额..."                       │   │
│   │ }                                                                   │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                      │                                       │
│                                      ▼                                       │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │ Step 4: HITL 等待状态 (Human-in-the-Loop)                            │   │
│   │ ─────────────────────────────────                                   │   │
│   │ Store: pending_executions[execution_id] = {                          │   │
│   │   query, intent, schema, sql_data,                                  │   │
│   │   sql, datasource, explanation                                      │   │
│   │ }                                                                   │   │
│   │                                                                              │
│   │ Response: {pending_approval: true, execution_id: "xxx"}             │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                    用户审批 /api/v1/approve
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ ③ SQL 执行层 (src/tools/sql_executor.py)                                    │
│                                                                              │
│   Input:  execution_id → 从 pending_executions 取出                         │
│                                                                              │
│   安全验证层:                                                               │
│   ├─ _validate_sql_safety():                                              │
│   │   ├─ 必须以 SELECT 开头                                                │
│   │   ├─ 禁止 DROP/DELETE/UPDATE/INSERT/CREATE/ALTER/TRUNCATE             │
│   │   ├─ 禁止 UNION (防注入)                                               │
│   │   └─ 禁止多语句 (分号检查)                                              │
│   │                                                                         │
│   数据源路由:                                                               │
│   ├─ datasource == "mysql" → settings.mysql_url → SQLAlchemy MySQL         │
│   └─ datasource == "doris" → settings.doris_url → SQLAlchemy Doris         │
│                                                                              │
│   Output: QueryResult JSON                                                │
│   {                                                                        │
│     "sql": "SELECT SUM...",                                                │
│     "execution_time_ms": 45,                                               │
│     "row_count": 1,                                                        │
│     "columns": [{"name": "销售额", "type": "DECIMAL"}],                    │
│     "rows": [{"销售额": 81992.00}]                                         │
│   }                                                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              返回结果给客户端                                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 二、数据模型流转

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  QueryIntent │ ──→ │TableMetadata │ ──→ │ GeneratedSQL │ ──→ │ QueryResult  │
│  (意图解析)   │     │  (表元数据)   │     │  (生成SQL)   │     │  (执行结果)  │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
     │                    │                    │                    │
     metrics             table_name           sql                 rows
     dimensions          datasource           explanation         columns
     filters             fields               tables              execution_time
     time_range                                                    row_count
```

### 2.1 模型定义 (src/models.py)

| 模型 | 用途 | 关键字段 |
|------|------|----------|
| `QueryIntent` | 存储解析后的查询意图 | `metrics`, `dimensions`, `filters`, `time_range`, `aggregation`, `limit` |
| `TableMetadata` | 表结构元数据 | `table_name`, `table_cn_name`, `datasource`, `fields` |
| `GeneratedSQL` | 生成的SQL及元信息 | `sql`, `datasource`, `tables`, `explanation` |
| `QueryResult` | SQL执行结果 | `sql`, `execution_time_ms`, `row_count`, `columns`, `rows` |

---

## 三、详细执行流程

### 3.1 启动流程

```
main.py
  └── lifespan() [启动事件]
        └── load_tables_from_json("data/schema/ecommerce_schema.json")
              └── 解析 6 张表定义
                    └── SchemaStore.index_tables(tables)
                          └── 存入内存: _tables = tables
```

### 3.2 查询流程 (POST /api/v1/query)

```
query.py::query()
  │
  ├── Step 1: Intent Parser
  │     └── parse_intent.invoke({"query": "..."})
  │           └── ChatOpenAI.invoke(prompt)
  │                 └── 返回 QueryIntent JSON
  │
  ├── Step 2: Schema Retriever
  │     └── retrieve_schema.invoke({"query": "...", "top_k": 3})
  │           └── SchemaStore.retrieve(query)
  │                 ├── _extract_keywords(query) → 关键词列表
  │                 ├── _score_table(table, keywords) → 评分排序
  │                 └── 返回 TableMetadata[]
  │
  ├── Step 3: SQL Generator
  │     └── generate_sql.invoke({"intent_json": "...", "schema_json": "..."})
  │           ├── ChatOpenAI.invoke(messages)
  │           ├── _is_safe_sql(sql) → 安全检查
  │           ├── 强制覆盖 datasource (从 schema 获取)
  │           └── GeneratedSQL.model_dump()
  │
  └── Step 4: 存储等待审批
        └── pending_executions[execution_id] = {...}
              └── 返回 {pending_approval: true, execution_id: "..."}
```

### 3.3 审批执行流程 (POST /api/v1/approve)

```
query.py::approve_action()
  │
  ├── 从 pending_executions 取出 execution
  │
  ├── if decision == "reject"
  │     └── 删除 pending, 返回拒绝结果
  │
  └── if decision == "approve"
        └── execute_sql.invoke({"sql_json": "..."})
              ├── _validate_sql_safety(sql) → 二次安全验证
              ├── 根据 datasource 选择数据库连接
              │     ├── "mysql" → settings.mysql_url
              │     └── "doris" → settings.doris_url
              ├── SQLAlchemy create_engine(db_url)
              ├── conn.execute(text(sql))
              └── 返回 QueryResult JSON
```

---

## 四、安全机制层级

| 层级 | 文件位置 | 机制 | 说明 |
|------|----------|------|------|
| 1 | `sql_generator.py:14` | LLM Prompt 约束 | "只允许生成SELECT语句，禁止DROP/DELETE等" |
| 2 | `sql_generator.py:89` | `_is_safe_sql()` | 运行时检查SQL以SELECT开头，无危险关键词 |
| 3 | `sql_generator.py:98` | 强制datasource覆盖 | 从schema.tables[0].datasource获取，**不信任LLM** |
| 4 | `sql_executor.py:40` | `_validate_sql_safety()` | 二次验证SQL安全性，更严格的检查 |
| 5 | `sql_executor.py:163` | 禁止UNION | 防止SQL注入攻击 |

### 4.1 危险关键词列表

```python
# sql_generator.py + sql_executor.py
dangerous_keywords = [
    "DROP", "DELETE", "UPDATE", "INSERT",
    "CREATE", "ALTER", "TRUNCATE",
    "GRANT", "REVOKE", "EXEC", "EXECUTE",
    "SP_", "XP_", "--", "/*", "*/", "UNION"
]
```

---

## 五、核心工具链

| 工具 | 文件 | 功能 | 输入 | 输出 |
|------|------|------|------|------|
| `parse_intent` | `intent_parser.py` | 自然语言意图解析 | query: str | QueryIntent JSON |
| `retrieve_schema` | `schema_retriever.py` | 检索相关表结构 | query, top_k | TableMetadata[] JSON |
| `generate_sql` | `sql_generator.py` | 生成SQL语句 | intent_json, schema_json | GeneratedSQL JSON |
| `execute_sql` | `sql_executor.py` | 执行SQL并返回结果 | sql_json | QueryResult JSON |

---

## 六、状态存储

### 6.1 pending_executions (内存存储)

```python
# src/api/routes/query.py:12
pending_executions = {}

# 存储结构:
{
    "{thread_id}_{id}": {
        "thread_id": str,
        "query": str,           # 原始查询
        "intent": dict,         # QueryIntent
        "schema": dict,         # TableMetadata[]
        "sql_data": dict,       # GeneratedSQL
        "sql": str,             # 生成的SQL
        "datasource": str,      # mysql/doris
        "explanation": str      # SQL解释
    }
}
```

### 6.2 SchemaStore (单例模式)

```python
# src/services/schema_store.py:123
_schema_store = None  # 全局单例

def get_schema_store() -> SchemaStore:
    if _schema_store is None:
        _schema_store = SchemaStore()
    return _schema_store
```

---

## 七、数据源配置

### 7.1 MySQL 连接

```python
# src/config.py
mysql_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

# 默认配置 (.env)
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=123456
MYSQL_DATABASE=gouchen
```

### 7.2 Doris 连接

```python
# src/config.py
doris_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

# 默认配置 (.env)
DORIS_HOST=localhost
DORIS_PORT=9030
DORIS_USER=root
DORIS_PASSWORD=
DORIS_DATABASE=gouchen
```

### 7.3 数据源选择逻辑

```python
# sql_generator.py:94-99
schema_data = json.loads(schema_json)
tables = schema_data.get("tables", [])
if tables:
    first_table = tables[0]
    correct_datasource = first_table.get("datasource", "mysql")
    sql_data["datasource"] = correct_datasource  # 强制覆盖
```

**重要**: 数据源从表元数据中获取，不依赖LLM判断。

---

## 八、Schema 文件结构

```json
{
  "tables": [
    {
      "table_name": "fact_order",
      "table_cn_name": "订单事实表",
      "description": "存储订单核心数据",
      "datasource": "mysql",
      "fields": [
        {
          "field_name": "order_id",
          "field_cn_name": "订单ID",
          "field_type": "VARCHAR",
          "description": "订单唯一标识"
        }
      ]
    }
  ]
}
```

---

## 九、调用时序图

```
用户    FastAPI    IntentParser    SchemaRetriever    SQLGenerator    SQLExecutor    MySQL
 │         │            │                 │                │               │            │
 │──query──▶│            │                 │                │               │            │
 │         │──invoke───▶│                 │                │               │            │
 │         │            │────Kimi API────▶│                │               │            │
 │         │            │◀───Intent───────│                │               │            │
 │         │◀───────────│                 │                │               │            │
 │         │                           invoke                              │            │
 │         │──────────────────────────────▶│                               │            │
 │         │                               │────SchemaStore.retrieve()────▶│            │
 │         │                               │◀────────tables────────────────│            │
 │         │◀──────────────────────────────│                               │            │
 │         │                                                              invoke
 │         │───────────────────────────────────────────────────────────────▶│
 │         │                                                              │───Kimi───▶
 │         │                                                              │◀──SQL────│
 │         │◀───────────────────────────────────────────────────────────────│
 │◀──pending──│                                                              │            │
 │         │                                                                (等待审批)
 │──approve──▶│                                                              │            │
 │         │────────────────────────────────────────────────────────────────invoke──────▶
 │         │                                                                               │
 │         │                                                            validate + execute │
 │         │                                                                               │
 │         │◀──────────────────────────────────────────────────────────────────────────────│
 │◀──result──│                                                                               │
 │         │                                                                                │
```

---

## 十、追踪调试指南

### 10.1 日志输出点

| 阶段 | 文件 | 日志 | 说明 |
|------|------|------|------|
| 启动 | `main.py:20` | `Indexed {n} tables` | SchemaStore 初始化完成 |
| Step 1 | `query.py:41` | `Step 1: Parsing intent...` | 开始意图解析 |
| Step 1 | `query.py:47` | `Intent: {...}` | 意图解析结果 |
| Step 2 | `query.py:50` | `Step 2: Retrieving schema...` | 开始检索表结构 |
| Step 2 | `query.py:60` | `Schema: {...}` | 检索到的表 |
| Step 3 | `query.py:63` | `Step 3: Generating SQL...` | 开始生成SQL |
| Step 3 | `query.py:73` | `Generated SQL: ...` | 生成的SQL |
| 执行 | `query.py:139` | `Executing SQL: ...` | 开始执行SQL |

### 10.2 关键断点位置

1. **意图解析失败**: `intent_parser.py:67` - 检查LLM返回的JSON格式
2. **schema检索失败**: `schema_store.py:66` - 检查关键词提取和评分
3. **SQL生成失败**: `sql_generator.py:85` - 检查LLM返回和JSON解析
4. **SQL安全检查失败**: `sql_executor.py:40` - 检查SQL是否符合安全规则
5. **数据库执行失败**: `sql_executor.py:65` - 检查SQL语法和数据库连接

---

*文档版本: 0.1.0*  
*最后更新: 2026-04-05*
