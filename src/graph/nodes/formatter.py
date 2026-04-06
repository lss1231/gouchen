"""Result formatter node for NL2SQL graph."""

from typing import Any, Dict, List, Optional


def recommend_chart(columns: List[Dict[str, str]], rows: List[Dict[str, Any]]) -> str:
    """Recommend chart type based on data features.

    Args:
        columns: List of column metadata
        rows: List of data rows

    Returns:
        Recommended chart type
    """
    if not rows or not columns:
        return "table"

    # Count numeric and categorical columns
    numeric_cols = []
    categorical_cols = []
    date_cols = []

    for col in columns:
        col_name = col.get("name", "").lower()
        # Detect column types based on data
        if rows:
            sample_value = rows[0].get(col.get("name"), "")
            if isinstance(sample_value, (int, float)):
                numeric_cols.append(col)
            elif any(keyword in col_name for keyword in ["date", "time", "day", "month", "year"]):
                date_cols.append(col)
            else:
                categorical_cols.append(col)

    # Chart recommendation logic
    row_count = len(rows)

    # Single value - use KPI card
    if row_count == 1 and len(numeric_cols) == 1:
        return "kpi"

    # Time series data - use line chart
    if date_cols and numeric_cols:
        return "line"

    # Category comparison - use bar chart
    if categorical_cols and numeric_cols and row_count <= 20:
        return "bar"

    # Many categories - use horizontal bar
    if categorical_cols and numeric_cols and row_count > 20:
        return "bar_horizontal"

    # Two numeric columns - scatter plot
    if len(numeric_cols) >= 2:
        return "scatter"

    # Default to table
    return "table"


def generate_echarts_option(
    chart_type: str,
    columns: List[Dict[str, str]],
    rows: List[Dict[str, Any]],
    title: str = "",
) -> Optional[Dict[str, Any]]:
    """Generate ECharts configuration for the recommended chart.

    Args:
        chart_type: Type of chart
        columns: List of column metadata
        rows: List of data rows
        title: Chart title

    Returns:
        ECharts option dict or None
    """
    if not rows or not columns:
        return None

    # Get column names
    col_names = [col.get("name", "") for col in columns]
    if not col_names:
        return None

    # Identify data types
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

    # Default x-axis is first categorical or date column
    x_axis_col = categorical_cols[0] if categorical_cols else (date_cols[0] if date_cols else col_names[0])
    # Default y-axis is first numeric column
    y_axis_col = numeric_cols[0] if numeric_cols else col_names[1] if len(col_names) > 1 else col_names[0]

    if chart_type == "line":
        option = {
            "title": {"text": title or "趋势图"},
            "tooltip": {"trigger": "axis"},
            "xAxis": {
                "type": "category",
                "data": [str(row.get(x_axis_col, "")) for row in rows],
            },
            "yAxis": {"type": "value"},
            "series": [{
                "data": [row.get(y_axis_col, 0) for row in rows],
                "type": "line",
                "smooth": True,
            }],
        }
    elif chart_type == "bar":
        option = {
            "title": {"text": title or "柱状图"},
            "tooltip": {"trigger": "axis"},
            "xAxis": {
                "type": "category",
                "data": [str(row.get(x_axis_col, "")) for row in rows[:20]],  # Limit to 20
            },
            "yAxis": {"type": "value"},
            "series": [{
                "data": [row.get(y_axis_col, 0) for row in rows[:20]],
                "type": "bar",
            }],
        }
    elif chart_type == "bar_horizontal":
        option = {
            "title": {"text": title or "条形图"},
            "tooltip": {"trigger": "axis"},
            "xAxis": {"type": "value"},
            "yAxis": {
                "type": "category",
                "data": [str(row.get(x_axis_col, "")) for row in rows[:30]],
            },
            "series": [{
                "data": [row.get(y_axis_col, 0) for row in rows[:30]],
                "type": "bar",
            }],
        }
    elif chart_type == "pie":
        option = {
            "title": {"text": title or "饼图"},
            "tooltip": {"trigger": "item"},
            "series": [{
                "type": "pie",
                "data": [
                    {"name": str(row.get(x_axis_col, "")), "value": row.get(y_axis_col, 0)}
                    for row in rows[:10]
                ],
            }],
        }
    elif chart_type == "scatter":
        x_col = numeric_cols[0] if numeric_cols else col_names[0]
        y_col = numeric_cols[1] if len(numeric_cols) > 1 else col_names[1] if len(col_names) > 1 else col_names[0]
        option = {
            "title": {"text": title or "散点图"},
            "tooltip": {"trigger": "item"},
            "xAxis": {"type": "value"},
            "yAxis": {"type": "value"},
            "series": [{
                "data": [[row.get(x_col, 0), row.get(y_col, 0)] for row in rows],
                "type": "scatter",
            }],
        }
    elif chart_type == "kpi":
        value = rows[0].get(y_axis_col, 0) if rows else 0
        option = {
            "title": {"text": title or "关键指标"},
            "series": [{
                "type": "gauge",
                "detail": {"formatter": "{value}"},
                "data": [{"value": value, "name": y_axis_col}],
            }],
        }
    else:
        # Default table view - no ECharts option needed
        return None

    return option


def formatter_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Format execution result with chart recommendation.

    Args:
        state: Current graph state

    Returns:
        State updates with formatted result
    """
    execution_result = state.get("execution_result")
    query = state.get("query", "")

    if not execution_result:
        return {
            "formatted_result": None,
        }

    columns = execution_result.get("columns", [])
    rows = execution_result.get("rows", [])

    # Recommend chart type
    chart_type = recommend_chart(columns, rows)

    # Generate ECharts option
    echarts_option = generate_echarts_option(
        chart_type=chart_type,
        columns=columns,
        rows=rows,
        title=query[:20] + "..." if len(query) > 20 else query,
    )

    formatted_result = {
        "sql": execution_result.get("sql", ""),
        "execution_time_ms": execution_result.get("execution_time_ms", 0),
        "row_count": execution_result.get("row_count", 0),
        "columns": columns,
        "rows": rows,
        "chart_recommendation": chart_type,
        "echarts_option": echarts_option,
    }

    return {"formatted_result": formatted_result}
