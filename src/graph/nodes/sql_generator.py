"""SQL generator node for NL2SQL graph - Doris only."""

from typing import Any, Dict, List

from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

from ...config import get_settings
from ...services.metric_knowledge import get_metric_knowledge_service
from ...services.few_shot_store import get_few_shot_store
from ...services.success_case_store import get_success_case_store
from ...utils.sql_safety import validate_sql_safety


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
- 原始指标: {metrics}
- 解析后的标准指标: {resolved_metrics}
- 维度: {dimensions}
- 过滤条件: {filters}
- 时间范围: {time_range}
- 聚合方式: {aggregation}
- 限制条数: {limit}
- 分析类型: {analysis_type}
- 对比时间段: {compare_periods}

## 指标定义参考
{metric_definitions}

指标定义说明：
- 如果指标定义中提供了 formula，请优先使用该公式
- 如果指标定义中指定了 applicable_tables，请优先从这些表中选择数据
- 如果指标已预计算在 ADS/DWS 层表中，优先使用预计算字段，避免在 DWD/ODS 层重新计算

## 参考案例（请学习以下案例的 SQL 写法）
{few_shot_examples}

## 历史成功案例（参考相似查询的成功 SQL）
{success_cases}

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
    resolved_metrics = intent.get("resolved_metrics", [])
    dimensions = intent.get("dimensions", [])
    filters = intent.get("filters", [])
    time_range = intent.get("time_range", {})
    aggregation = intent.get("aggregation", "sum")
    limit = intent.get("limit", 1000)
    analysis_type = intent.get("analysis_type", "single")
    compare_periods = intent.get("compare_periods", [])

    # Load metric definitions for resolved metrics
    metric_service = get_metric_knowledge_service()
    metric_definitions_str = metric_service.format_metrics_for_prompt(resolved_metrics)

    # Load few-shot examples
    few_shot_store = get_few_shot_store()
    few_shot_examples = few_shot_store.search(query=state.get("query", ""), top_k=2, category=analysis_type)
    # If no examples matched by category, fallback to general search
    if not few_shot_examples:
        few_shot_examples = few_shot_store.search(query=state.get("query", ""), top_k=2)
    few_shot_str = few_shot_store.format_for_prompt(few_shot_examples)

    # Load dynamic success cases
    success_case_store = get_success_case_store()
    success_cases = success_case_store.search(query=state.get("query", ""), top_k=2)
    success_cases_str = success_case_store.format_for_prompt(success_cases)

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
            resolved_metrics=resolved_metrics,
            metric_definitions=metric_definitions_str,
            few_shot_examples=few_shot_str,
            success_cases=success_cases_str,
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
