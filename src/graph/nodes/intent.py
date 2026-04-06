"""Intent parsing node for NL2SQL graph."""

from typing import Any, Dict, List, Optional

from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

from ...config import get_settings
from ..state import QueryIntent


class QueryIntentSchema(BaseModel):
    """Schema for query intent extraction using structured output."""

    metrics: List[str] = Field(
        description="要查询的指标字段列表（如 order_amount, paid_amount, order_count 等）"
    )
    dimensions: List[str] = Field(
        description="分组维度列表（如 date, region, category 等）",
        default_factory=lambda: ["date"],
    )
    filters: List[Dict[str, Any]] = Field(
        description="过滤条件列表，每个条件包含 field, operator, value",
        default_factory=list,
    )
    time_range: Dict[str, Any] = Field(
        description="时间范围，包含 type 和 days",
        default_factory=lambda: {"type": "last_30_days", "days": 30},
    )
    aggregation: str = Field(
        description="聚合方式（sum, count, avg, max, min）",
        default="sum",
    )
    limit: int = Field(
        description="返回条数限制",
        default=1000,
    )

# Time pattern mappings
TIME_PATTERNS = {
    "今天": {"time_range": {"type": "today", "days": 1}},
    "昨天": {"time_range": {"type": "yesterday", "days": 1}},
    "本周": {"time_range": {"type": "this_week", "days": 7}},
    "上周": {"time_range": {"type": "last_week", "days": 7}},
    "近7天": {"time_range": {"type": "last_7_days", "days": 7}},
    "近30天": {"time_range": {"type": "last_30_days", "days": 30}},
    "本月": {"time_range": {"type": "this_month", "days": 30}},
    "上月": {"time_range": {"type": "last_month", "days": 30}},
    "上个月": {"time_range": {"type": "last_month", "days": 30}},
    "今年": {"time_range": {"type": "this_year", "days": 365}},
    "去年": {"time_range": {"type": "last_year", "days": 365}},
}

# Metric keyword mappings
METRIC_KEYWORDS = {
    "销售额": ["order_amount", "sales_amount", "gmv", "paid_amount"],
    "销售": ["order_amount", "sales_amount", "gmv"],
    "收入": ["revenue", "income", "paid_amount"],
    "订单数": ["order_count", "order_num"],
    "订单量": ["order_count", "order_num"],
    "用户数": ["user_count", "customer_count"],
    "客户数": ["customer_count", "user_count"],
    "利润": ["profit", "gross_profit", "net_profit"],
    "成本": ["cost", "total_cost"],
    "gmv": ["gmv", "order_amount"],
    "客单价": ["avg_order_amount", "customer_unit_price"],
}

# Dimension keyword mappings
DIMENSION_KEYWORDS = {
    "时间": ["date", "order_date", "create_time", "day"],
    "日期": ["date", "order_date", "day"],
    "月份": ["month", "order_month"],
    "年份": ["year", "order_year"],
    "地区": ["region", "province", "city", "area"],
    "省份": ["province", "region"],
    "城市": ["city", "region"],
    "品类": ["category", "category_name", "product_category"],
    "类目": ["category", "category_name"],
    "商品": ["product", "product_name", "sku"],
    "产品": ["product", "product_name"],
    "品牌": ["brand", "brand_name"],
    "渠道": ["channel", "sales_channel"],
    "店铺": ["shop", "store", "shop_name"],
}


def extract_time_pattern(query: str) -> Optional[Dict[str, Any]]:
    """Extract time pattern from query using keyword matching."""
    for pattern, config in TIME_PATTERNS.items():
        if pattern in query:
            return config["time_range"]
    return None


def extract_metric_keywords(query: str) -> List[str]:
    """Extract metric keywords from query."""
    metrics = []
    for keyword, fields in METRIC_KEYWORDS.items():
        if keyword in query:
            metrics.extend(fields)
    return list(set(metrics)) if metrics else ["order_amount"]


def extract_dimension_keywords(query: str) -> List[str]:
    """Extract dimension keywords from query."""
    dimensions = []
    for keyword, fields in DIMENSION_KEYWORDS.items():
        if keyword in query:
            dimensions.extend(fields)
    return list(set(dimensions)) if dimensions else ["date"]


INTENT_PROMPT = """你是一个专业的数据分析意图解析助手。请从用户的自然语言查询中提取查询意图。

支持的指标(metrics)：order_amount(销售额), paid_amount(实付金额), order_count(订单数), user_count(用户数), profit(利润), cost(成本)
支持的维度(dimensions)：date(日期), region(地区), province(省份), city(城市), category(品类), product(商品), brand(品牌), channel(渠道)
支持的时间范围(time_range)：today(今天), yesterday(昨天), this_week(本周), last_week(上周), last_7_days(近7天), last_30_days(近30天), this_month(本月), last_month(上月)
支持的聚合方式(aggregation)：sum(求和), count(计数), avg(平均), max(最大), min(最小)

用户查询: {query}

请提取查询意图。"""


def intent_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Parse query intent using rule + LLM hybrid approach with structured output.

    Args:
        state: Current graph state

    Returns:
        State updates with parsed intent
    """
    query = state.get("query", "")

    # First, try rule-based extraction
    time_range = extract_time_pattern(query)
    metrics = extract_metric_keywords(query)
    dimensions = extract_dimension_keywords(query)

    # Build default intent from rules
    default_intent: QueryIntent = {
        "metrics": metrics,
        "dimensions": dimensions,
        "filters": [],
        "time_range": time_range or {"type": "last_30_days", "days": 30},
        "aggregation": "sum",
        "limit": 1000,
    }

    try:
        # Try LLM-based extraction with structured output
        settings = get_settings()
        llm = ChatOpenAI(
            model=settings.llm_model,
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            temperature=0.0,
        ).with_structured_output(QueryIntentSchema)  # 使用结构化输出

        prompt = INTENT_PROMPT.format(query=query)
        parsed = llm.invoke(prompt)  # 直接返回 QueryIntentSchema 对象

        # Merge with default intent (LLM takes precedence)
        intent: QueryIntent = {
            "metrics": parsed.metrics or default_intent["metrics"],
            "dimensions": parsed.dimensions or default_intent["dimensions"],
            "filters": parsed.filters or default_intent["filters"],
            "time_range": parsed.time_range or default_intent["time_range"],
            "aggregation": parsed.aggregation or default_intent["aggregation"],
            "limit": parsed.limit or default_intent["limit"],
        }

    except Exception:
        # Fallback to rule-based intent if LLM fails
        intent = default_intent

    return {"intent": intent}
