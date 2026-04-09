"""SQL generator node for NL2SQL graph - Doris only."""

import re
from typing import Any, Dict, List

from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

from ...config import get_settings


class SQLGenerationResult(BaseModel):
    """Schema for SQL generation output using structured output."""

    sql: str = Field(
        description="生成的SQL查询语句。必须是SELECT语句，禁止INSERT/UPDATE/DELETE/DROP/ALTER等修改操作。当analysis_type=mom或yoy时，必须包含增长率计算。"
    )
    explanation: str = Field(
        description="SQL查询的解释说明。当analysis_type=mom或yoy时，必须说明增长率是如何计算的。"
    )


# SQL Generator prompt
SQL_GENERATOR_PROMPT = """你是一个专业的SQL生成助手。请根据以下信息生成 Doris SQL 查询。

## 【重要】字段使用约束
**只能使用"可用表结构"中列出的字段，禁止编造字段！**

生成SQL前，请：
1. 仔细阅读"可用表结构"中的字段列表
2. 确认要查询的字段确实存在于表中
3. 如果表中没有该字段，使用聚合函数计算（如 SUM/COUNT）

## 用户查询意图
- 指标: {metrics}
- 维度: {dimensions}
- 过滤条件: {filters}
- 时间范围: {time_range}
- 聚合方式: {aggregation}
- 限制条数: {limit}
- 分析类型: {analysis_type}
- 对比时间段: {compare_periods}

## 可用表结构（**严格使用以下字段，禁止编造**）
{schema}

## 分析类型说明

### 当 analysis_type = "single" (单时间查询)：
普通聚合查询，返回该时间段的汇总数据。

### 当 analysis_type = "mom" (环比分析)：
**必须计算增长率！** SQL结构要求：
1. 分别计算两个时间段的指标值
2. 计算增长率公式：((本期值 - 上期值) / 上期值) * 100
3. 返回格式必须包含：current_period_value, previous_period_value, growth_rate

### 当 analysis_type = "yoy" (同比分析)：
类似环比，但对比去年同期，同样必须计算同比增长率。

### 当 analysis_type = "comparison" (多维度对比)：
返回各维度的指标值对比。

## 安全规则（必须遵守）
1. 只生成SELECT查询，禁止INSERT/UPDATE/DELETE/DROP/ALTER等修改操作
2. **不要使用参数占位符(?)，直接在SQL中嵌入具体的值**
3. 字符串值使用单引号包裹
4. 避免使用SELECT *，必须明确指定字段
5. 时间范围必须在WHERE子句中明确指定
6. 如果涉及多个表，使用JOIN并确保有适当的连接条件"""

# Forbidden keywords for safety check
FORBIDDEN_KEYWORDS = [
    "insert", "update", "delete", "drop", "alter", "truncate",
    "create", "grant", "revoke", "exec", "execute", "sp_",
    "xp_", "--", ";/*", "*/", "@@", "@variable"
]


def format_schema_for_prompt(tables: List[Dict[str, Any]]) -> str:
    """Format table schema for LLM prompt."""
    schema_parts = []

    for table in tables:
        table_name = table.get("table_name", "")
        table_cn_name = table.get("table_cn_name", "")
        description = table.get("description", "")
        fields = table.get("fields", [])

        schema_part = f"\n表名: {table_name}"
        if table_cn_name:
            schema_part += f" ({table_cn_name})"
        if description:
            schema_part += f"\n描述: {description}"

        schema_part += "\n字段:"
        for field in fields:
            field_name = field.get("field_name", "")
            field_cn_name = field.get("field_cn_name", "")
            field_type = field.get("field_type", "")
            field_desc = field.get("description", "")

            field_str = f"  - {field_name}"
            if field_type:
                field_str += f" ({field_type})"
            if field_cn_name:
                field_str += f" [{field_cn_name}]"
            if field_desc:
                field_str += f": {field_desc}"
            schema_part += f"\n{field_str}"

        schema_parts.append(schema_part)

    return "\n".join(schema_parts)


def validate_sql_safety(sql: str) -> tuple[bool, str]:
    """Validate SQL for safety issues."""
    sql_lower = sql.lower().strip()

    # Check for forbidden keywords
    for keyword in FORBIDDEN_KEYWORDS:
        if keyword in sql_lower:
            return False, f"SQL contains forbidden keyword: {keyword}"

    # Must start with SELECT or WITH (CTE)
    if not (sql_lower.startswith("select") or sql_lower.startswith("with")):
        return False, "SQL must start with SELECT or WITH"

    # Check for multiple statements
    if ";" in sql and not sql_lower.endswith(";"):
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        if len(statements) > 1:
            return False, "Multiple SQL statements are not allowed"

    return True, ""


def sql_generator_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Generate Doris SQL from intent and schema."""
    intent = state.get("intent", {})
    relevant_tables = state.get("relevant_tables", [])

    if not relevant_tables:
        return {
            "generated_sql": None,
            "sql_explanation": "No relevant tables found for query generation.",
            "error": "No relevant tables available.",
            "datasource": "doris",
        }

    # Format schema for prompt
    schema_str = format_schema_for_prompt(relevant_tables)

    # Extract intent fields
    metrics = intent.get("metrics", [])
    dimensions = intent.get("dimensions", [])
    filters = intent.get("filters", [])
    time_range = intent.get("time_range", {})
    aggregation = intent.get("aggregation", "sum")
    limit = intent.get("limit", 1000)
    analysis_type = intent.get("analysis_type", "single")
    compare_periods = intent.get("compare_periods", [])

    try:
        # Generate SQL using LLM with structured output
        settings = get_settings()
        llm = ChatOpenAI(
            model=settings.llm_model,
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            temperature=0.0,
        ).with_structured_output(SQLGenerationResult)

        compare_periods_str = str(compare_periods) if compare_periods else "[]"

        prompt = SQL_GENERATOR_PROMPT.format(
            metrics=metrics,
            dimensions=dimensions,
            filters=filters,
            time_range=time_range,
            aggregation=aggregation,
            limit=limit,
            analysis_type=analysis_type,
            compare_periods=compare_periods_str,
            schema=schema_str,
        )

        result = llm.invoke(prompt)
        sql = result.sql
        explanation = result.explanation

        # Validate SQL safety
        is_safe, error_msg = validate_sql_safety(sql)
        if not is_safe:
            return {
                "generated_sql": None,
                "sql_explanation": f"SQL generation failed safety check: {error_msg}",
                "error": f"SQL safety check failed: {error_msg}",
                "datasource": "doris",
            }

        return {
            "generated_sql": sql,
            "sql_explanation": explanation,
            "datasource": "doris",
        }

    except Exception as e:
        return {
            "generated_sql": None,
            "sql_explanation": f"SQL generation failed: {str(e)}",
            "error": f"SQL generation error: {str(e)}",
            "datasource": "doris",
        }
