"""SQL generator node for NL2SQL graph."""

import re
from typing import Any, Dict, List

from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

from ...config import get_settings


class SQLGenerationResult(BaseModel):
    """Schema for SQL generation output using structured output."""

    sql: str = Field(
        description="生成的SQL查询语句。必须是SELECT语句，禁止INSERT/UPDATE/DELETE/DROP/ALTER等修改操作。"
    )
    explanation: str = Field(
        description="SQL查询的解释说明，描述查询的逻辑和返回的内容"
    )


# SQL Generator prompt with safety rules
SQL_GENERATOR_PROMPT = """你是一个专业的SQL生成助手。请根据以下信息生成安全的SQL查询：

## 用户查询意图
- 指标: {metrics}
- 维度: {dimensions}
- 过滤条件: {filters}
- 时间范围: {time_range}
- 聚合方式: {aggregation}
- 限制条数: {limit}

## 可用表结构
{schema}

## 安全规则（必须遵守）
1. 只生成SELECT查询，禁止任何INSERT/UPDATE/DELETE/DROP/ALTER等修改操作
2. 所有字符串参数必须使用参数化查询或正确的转义
3. 避免使用SELECT *，必须明确指定字段
4. 时间范围必须在WHERE子句中明确指定
5. 如果涉及多个表，使用JOIN并确保有适当的连接条件"""

# Forbidden keywords for safety check
FORBIDDEN_KEYWORDS = [
    "insert", "update", "delete", "drop", "alter", "truncate",
    "create", "grant", "revoke", "exec", "execute", "sp_",
    "xp_", "--", ";/*", "*/", "@@", "@variable"
]


def format_schema_for_prompt(tables: List[Dict[str, Any]]) -> str:
    """Format table schema for LLM prompt.

    Args:
        tables: List of table metadata dicts

    Returns:
        Formatted schema string
    """
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
    """Validate SQL for safety issues.

    Args:
        sql: SQL string to validate

    Returns:
        Tuple of (is_safe, error_message)
    """
    sql_lower = sql.lower().strip()

    # Check for forbidden keywords
    for keyword in FORBIDDEN_KEYWORDS:
        if keyword in sql_lower:
            return False, f"SQL contains forbidden keyword: {keyword}"

    # Must start with SELECT
    if not sql_lower.startswith("select"):
        return False, "SQL must start with SELECT"

    # Check for multiple statements
    if ";" in sql and not sql_lower.endswith(";"):
        # Allow trailing semicolon, but not multiple statements
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        if len(statements) > 1:
            return False, "Multiple SQL statements are not allowed"

    return True, ""


def sql_generator_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Generate SQL from intent and schema with safety checks.

    Args:
        state: Current graph state

    Returns:
        State updates with generated SQL and explanation
    """
    intent = state.get("intent", {})
    relevant_tables = state.get("relevant_tables", [])

    if not relevant_tables:
        return {
            "generated_sql": None,
            "sql_explanation": "No relevant tables found for query generation.",
            "error": "No relevant tables available.",
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

    try:
        # Generate SQL using LLM with structured output
        settings = get_settings()
        llm = ChatOpenAI(
            model=settings.llm_model,
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            temperature=0.0,
        ).with_structured_output(SQLGenerationResult)

        prompt = SQL_GENERATOR_PROMPT.format(
            metrics=metrics,
            dimensions=dimensions,
            filters=filters,
            time_range=time_range,
            aggregation=aggregation,
            limit=limit,
            schema=schema_str,
        )

        result = llm.invoke(prompt)  # 直接返回 SQLGenerationResult 对象
        sql = result.sql
        explanation = result.explanation

        # Validate SQL safety
        is_safe, error_msg = validate_sql_safety(sql)
        if not is_safe:
            return {
                "generated_sql": None,
                "sql_explanation": f"SQL generation failed safety check: {error_msg}",
                "error": f"SQL safety check failed: {error_msg}",
            }

        return {
            "generated_sql": sql,
            "sql_explanation": explanation,
        }

    except Exception as e:
        return {
            "generated_sql": None,
            "sql_explanation": f"SQL generation failed: {str(e)}",
            "error": f"SQL generation error: {str(e)}",
        }
