# 钩沉 (Gouchen) NL2SQL 项目结构

## 目录结构

```
gouchen/
├── src/                          # 核心源代码
│   ├── api/                      # FastAPI 接口层
│   │   └── routes/               # API 路由 (health, query)
│   ├── graph/                    # LangGraph 工作流
│   │   ├── nodes/                # 图节点 (intent, schema, sql_generator, etc.)
│   │   ├── builder.py            # 图构建器
│   │   └── state.py              # 状态定义
│   ├── services/                 # 业务服务层
│   │   ├── qdrant_client.py      # Qdrant 向量数据库客户端
│   │   ├── schema_embedding.py   # Schema 向量化服务
│   │   ├── audit.py              # 审计日志服务
│   │   └── permission.py         # 权限服务
│   ├── config.py                 # 配置管理
│   ├── models.py                 # Pydantic 模型定义
│   └── main.py                   # 应用入口
│
├── scripts/                      # 工具脚本
│   ├── data_generation/          # 数据生成脚本
│   │   ├── generate_doris_data.py    # Doris 测试数据生成
│   │   ├── etl_doris_dw.py           # ETL 数据导入
│   │   └── import_data.py            # 数据导入工具
│   ├── setup/                    # 初始化设置脚本
│   │   └── setup_qdrant.py       # Qdrant 向量库初始化
│   ├── testing/                  # 测试辅助脚本
│   │   ├── test_qdrant_search.py     # 向量搜索测试
│   │   └── test_schema_consistency.py # Schema 一致性检查
│   └── deprecated/               # 已废弃脚本（待清理）
│
├── tests/                        # 测试代码
│   ├── test_cases/               # Phase 1 测试用例
│   │   ├── phase1_test_cases.json    # 测试用例定义
│   │   └── run_phase1_tests.py       # 测试执行器
│   └── deprecated/               # 已废弃测试（待清理）
│
├── data/                         # 数据文件
│   ├── schema/                   # Schema 定义
│   │   └── doris_schema_enhanced.json  # Doris 表结构定义
│   ├── sql/                      # SQL 脚本
│   └── vector_store/             # 本地向量存储（可选）
│
├── docs/                         # 文档
│   └── deprecated/               # 过时文档
│
├── archive/                      # 归档旧代码
│   └── old-langchain-implementation/  # 早期实现
│
├── docker/                       # Docker 配置
├── .env                          # 环境变量配置
├── .env.example                  # 环境变量示例
├── docker-compose.yml            # Docker Compose 配置
├── pyproject.toml                # Python 项目配置
└── README.md                     # 项目说明
```

## 核心模块说明

### 1. API 层 (`src/api/`)
- **routes/query.py**: 主查询接口 `/api/v1/query/`
- **routes/health.py**: 健康检查接口

### 2. LangGraph 工作流 (`src/graph/`)
- **nodes/intent.py**: 意图解析（LLM-based）
- **nodes/schema.py**: Schema 检索（向量搜索）
- **nodes/sql_generator.py**: SQL 生成
- **nodes/clarification.py**: 多轮澄清（HITL）
- **nodes/executor.py**: SQL 执行
- **nodes/ambiguity_detector.py**: 歧义检测

### 3. 服务层 (`src/services/`)
- **qdrant_client.py**: Qdrant 向量数据库操作
- **schema_embedding.py**: Schema 文本向量化
- **audit.py**: 查询审计日志
- **permission.py**: 数据权限控制

## 常用操作

### 启动服务
```bash
python -m src.main
```

### 运行测试
```bash
# 运行 Phase 1 全部测试
python tests/test_cases/run_phase1_tests.py

# 运行指定类别测试
python tests/test_cases/run_phase1_tests.py --category aggregation
```

### 初始化 Qdrant
```bash
python scripts/setup/setup_qdrant.py --recreate
```

### 生成测试数据
```bash
python scripts/data_generation/generate_doris_data.py --days 365 --orders 100000
```

## 废弃文件清理计划

以下目录/文件将在未来版本中删除：

- `scripts/deprecated/` - 临时测试脚本
- `tests/deprecated/` - 过时测试用例
- `docs/deprecated/` - 过时文档
- `archive/` - 旧实现归档
- `data/schema/backup_*.json` - Schema 备份文件
