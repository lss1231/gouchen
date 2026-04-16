"""Intent parsing node for NL2SQL graph - LLM-based with metric knowledge."""

from typing import Any, Dict, List

from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

from ...config import get_settings
from ...services.metric_knowledge import get_metric_knowledge_service


class QueryIntentSchema(BaseModel):
    """Schema for query intent extraction using structured output."""

    metrics: List[str] = Field(
        description="要查询的原始指标字段列表（口语化表达），如 ['销售额', '订单数']"
    )
    resolved_metrics: List[str] = Field(
        description="解析后的标准指标名称列表，必须来自可用指标库，如 ['gmv', 'order_count']",
        default_factory=list,
    )
    dimensions: List[str] = Field(
        description="分组维度列表，如 ['date', 'province', 'category']",
        default_factory=lambda: ["date"],
    )
    filters: List[Dict[str, Any]] = Field(
        description="过滤条件列表，每个条件包含 field, operator, value",
        default_factory=list,
    )
    time_range: Dict[str, Any] = Field(
        description="时间范围，支持多种格式：{type: 'yesterday'}, {type: 'last_7_days'}, {type: 'specific_date', date: '2024-12-21'}, {type: 'specific_month', month: '2024-04'}",
        default_factory=lambda: {"type": "last_30_days", "days": 30},
    )
    aggregation: str = Field(
        description="聚合方式：sum, count, avg, max, min",
        default="sum",
    )
    limit: int = Field(
        description="返回条数限制",
        default=1000,
    )
    analysis_type: str = Field(
        description="分析类型：single(单查询), mom(环比), yoy(同比), comparison(对比分析：地区/品类/用户群体等)",
        default="single",
    )
    compare_periods: List[Dict[str, Any]] = Field(
        description="对比时间段列表，用于环比/同比分析",
        default_factory=list,
    )


def _get_intent_prompt(query: str) -> str:
    """Build intent prompt with metric catalog injected."""
    metric_service = get_metric_knowledge_service()
    metrics_catalog = metric_service.get_metrics_catalog()

    return f"""你是数据分析意图解析专家，负责将用户的自然语言查询转换为结构化的分析意图。

## 可用指标库
以下是我们系统中定义的标准指标及其别名，请优先将用户查询中的指标映射到这些标准名称：
{metrics_catalog}

## 任务
从用户查询中提取以下要素：
1. **metrics**: 要查询的指标字段（保留用户的原始口语化表达）
2. **resolved_metrics**: 将 metrics 映射到"可用指标库"中的标准指标名称。如果用户提到多个指标，请逐一映射。如果无法匹配，留空列表 []
3. **dimensions**: 分组维度（如 date, province, category, user_level）
4. **time_range**: 时间范围，支持自然语言（如 昨天, 近7天, 2024-12-21, 本月）
5. **filters**: 过滤条件（如 province='北京', user_level=2）
6. **analysis_type**: 分析类型
   - single: 单时间段/单维度查询（默认）
   - mom: 环比分析（包含"环比/比上月"等关键词）
   - yoy: 同比分析（包含"同比/比去年/同期"等关键词）
   - comparison: 对比分析（地区/品类/用户群体等任意维度对比，如"北京和上海对比"、"VIP和普通用户对比"）
7. **aggregation**: 聚合方式（sum/count/avg/max/min，默认sum）

## 数据源
所有查询使用 Doris 数据仓库。

## 输出格式
直接输出 JSON 对象，不要包含任何解释说明。

用户查询: {query}"""


def intent_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Parse query intent using LLM with structured output.

    Args:
        state: Current graph state containing "query"

    Returns:
        State updates with parsed intent
    """
    query = state.get("query", "")

    try:
        settings = get_settings()
        llm = ChatOpenAI(
            model=settings.llm_model,
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            temperature=0.0,
        ).with_structured_output(QueryIntentSchema)

        prompt = _get_intent_prompt(query)
        parsed = llm.invoke(prompt)

        metrics = parsed.metrics if parsed.metrics else ["order_amount"]
        dimensions = parsed.dimensions if parsed.dimensions else ["date"]
        filters = parsed.filters if parsed.filters else []
        time_range = parsed.time_range if parsed.time_range else {"type": "last_30_days", "days": 30}
        aggregation = parsed.aggregation if parsed.aggregation else "sum"
        limit = parsed.limit if parsed.limit else 1000
        analysis_type = parsed.analysis_type if parsed.analysis_type else "single"
        compare_periods = parsed.compare_periods if parsed.compare_periods else []

        # Resolve metrics via knowledge service if LLM didn't provide resolved_metrics
        resolved_metrics = parsed.resolved_metrics if parsed.resolved_metrics else []
        if not resolved_metrics:
            metric_service = get_metric_knowledge_service()
            resolved_metrics = metric_service.resolve_from_list(metrics)
            # Also try to resolve from the full query text
            query_resolved = metric_service.resolve(query)
            for r in query_resolved:
                if r not in resolved_metrics:
                    resolved_metrics.append(r)

        # Convert to intent dict
        intent = {
            "metrics": metrics,
            "resolved_metrics": resolved_metrics,
            "dimensions": dimensions,
            "filters": filters,
            "time_range": time_range,
            "aggregation": aggregation,
            "limit": limit,
            "analysis_type": analysis_type,
            "compare_periods": compare_periods,
        }

    except Exception as e:
        # Fallback: use default intent
        print(f"Intent parsing failed: {e}, using default")
        intent = {
            "metrics": ["gmv"],
            "resolved_metrics": [],
            "dimensions": ["date"],
            "filters": [],
            "time_range": {"type": "last_30_days", "days": 30},
            "aggregation": "sum",
            "limit": 1000,
            "analysis_type": "single",
            "compare_periods": [],
        }

    return {"intent": intent}
