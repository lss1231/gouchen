"""Summarizer node for NL2SQL graph - generates natural language summary of query results."""

import json
from typing import Any, Dict, List, Optional

from langchain_openai import ChatOpenAI

from ...config import get_settings
from ..state import NL2SQLState


SUMMARIZER_PROMPT = """你是一位专业的数据分析师，请根据以下信息生成简洁的中文总结。

用户查询：{query}
SQL：{sql}
查询结果统计：
- 返回行数：{row_count}
- 列信息：{columns_info}
- 数据样本（前5行）：
{sample_data}

数据类型分析：{data_type}

请用1-2句话概括数据的关键发现，要求：
1. 语言简洁流畅，控制在100字以内
2. 准确反映数据内容，不要编造数据
3. 根据数据类型选择合适的表述风格：
   - 单值（KPI）：直接陈述数值，如"上个月销售额为XXX元"
   - 时间序列：描述趋势变化，如"销售额呈上升趋势，从月初的X元增长到月末的Y元"
   - 分组对比：指出最高/最低值及占比，如"华东地区销售额最高，达到X元，占总销售额的Y%"

请直接输出总结内容，不要添加任何前缀或解释。"""


def analyze_data_type(columns: List[Dict[str, str]], rows: List[Dict[str, Any]]) -> str:
    """Analyze data type to determine summary style.

    Args:
        columns: List of column metadata
        rows: List of data rows

    Returns:
        Data type description
    """
    if not rows or not columns:
        return "空数据"

    row_count = len(rows)

    # Count numeric and categorical columns
    numeric_cols = []
    categorical_cols = []
    date_cols = []

    for col in columns:
        col_name = col.get("name", "")
        if rows:
            sample_value = rows[0].get(col_name, "")
            if isinstance(sample_value, (int, float)):
                numeric_cols.append(col_name)
            elif any(keyword in col_name.lower() for keyword in ["date", "time", "day", "month", "year"]):
                date_cols.append(col_name)
            else:
                categorical_cols.append(col_name)

    # Determine data type
    if row_count == 1 and len(numeric_cols) == 1:
        return "单值指标(KPI)"

    if date_cols and numeric_cols:
        return "时间序列数据"

    if categorical_cols and numeric_cols:
        return "分组对比数据"

    if len(numeric_cols) >= 2:
        return "多指标数据"

    return "通用表格数据"


def format_sample_data(rows: List[Dict[str, Any]], max_rows: int = 5) -> str:
    """Format sample data for prompt.

    Args:
        rows: List of data rows
        max_rows: Maximum number of rows to include

    Returns:
        Formatted sample data string
    """
    if not rows:
        return "无数据"

    sample = rows[:max_rows]
    lines = []
    for i, row in enumerate(sample, 1):
        row_str = json.dumps(row, ensure_ascii=False, default=str)
        lines.append(f"  第{i}行: {row_str}")

    return "\n".join(lines)


def format_columns_info(columns: List[Dict[str, str]]) -> str:
    """Format columns info for prompt.

    Args:
        columns: List of column metadata

    Returns:
        Formatted columns info string
    """
    if not columns:
        return "无列信息"

    col_names = [col.get("name", "") for col in columns]
    return ", ".join(col_names)


def generate_summary(
    query: str,
    sql: str,
    execution_result: Dict[str, Any],
    llm: ChatOpenAI,
) -> str:
    """Generate natural language summary using LLM.

    Args:
        query: Original user query
        sql: Generated SQL
        execution_result: Execution result with columns and rows
        llm: ChatOpenAI instance

    Returns:
        Generated summary string
    """
    columns = execution_result.get("columns", [])
    rows = execution_result.get("rows", [])
    row_count = execution_result.get("row_count", 0)

    # Analyze data type
    data_type = analyze_data_type(columns, rows)

    # Format data for prompt
    columns_info = format_columns_info(columns)
    sample_data = format_sample_data(rows)

    # Build prompt
    prompt = SUMMARIZER_PROMPT.format(
        query=query,
        sql=sql,
        row_count=row_count,
        columns_info=columns_info,
        sample_data=sample_data,
        data_type=data_type,
    )

    try:
        response = llm.invoke(prompt)
        summary = response.content.strip()
        return summary
    except Exception:
        # Fallback to basic summary on error
        return generate_fallback_summary(query, row_count, data_type, rows, columns)


def generate_fallback_summary(
    query: str,
    row_count: int,
    data_type: str,
    rows: List[Dict[str, Any]],
    columns: List[Dict[str, str]],
) -> str:
    """Generate fallback summary when LLM fails.

    Args:
        query: Original user query
        row_count: Number of rows returned
        data_type: Data type description
        rows: List of data rows
        columns: List of column metadata

    Returns:
        Basic summary string
    """
    if row_count == 0:
        return f"查询未返回任何数据。"

    if data_type == "单值指标(KPI)" and rows:
        # Extract the numeric value
        for col in columns:
            col_name = col.get("name", "")
            value = rows[0].get(col_name)
            if isinstance(value, (int, float)):
                return f"根据查询，{query}的结果为{value}。"

    if data_type == "时间序列数据":
        return f"查询返回了{row_count}条时间序列数据，展示了相关指标的变化趋势。"

    if data_type == "分组对比数据":
        return f"查询返回了{row_count}个分组的数据对比结果。"

    return f"查询成功返回了{row_count}条数据。"


def summarizer_node(state: NL2SQLState) -> Dict[str, Any]:
    """Generate natural language summary of query results.

    Args:
        state: Current graph state

    Returns:
        State updates with summary
    """
    query = state.get("query", "")
    execution_result = state.get("execution_result")
    generated_sql = state.get("generated_sql", "")

    # If no execution result or error, skip summarization
    if not execution_result:
        return {"summary": None}

    if state.get("error"):
        return {"summary": f"查询执行出错: {state.get('error')}"}

    try:
        # Initialize LLM
        settings = get_settings()
        llm = ChatOpenAI(
            model=settings.llm_model,
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            temperature=0.3,
            max_tokens=200,
        )

        # Generate summary
        summary = generate_summary(
            query=query,
            sql=generated_sql,
            execution_result=execution_result,
            llm=llm,
        )

        return {"summary": summary}

    except Exception as e:
        # Return error message as summary on failure
        return {"summary": f"总结生成失败: {str(e)}"}
