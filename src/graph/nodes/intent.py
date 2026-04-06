"""Intent parsing node for NL2SQL graph."""

import json
import re
from typing import Any, Dict, List, Optional

from langchain_openai import ChatOpenAI

from ...config import get_settings
from ..state import QueryIntent

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


INTENT_PROMPT = """你是一个专业的数据分析意图解析助手。请从用户的自然语言查询中提取以下信息：

1. metrics: 用户想查询的指标字段列表（如销售额、订单数、用户数等）
2. dimensions: 用户想按哪些维度分组（如时间、地区、品类等）
3. filters: 过滤条件列表，每个条件包含 field, operator, value
4. time_range: 时间范围，包含 type 和 days
5. aggregation: 聚合方式（sum, count, avg, max, min）
6. limit: 返回条数限制

请严格按以下JSON格式返回（不要包含任何其他文本）：
{
    "metrics": ["field1", "field2"],
    "dimensions": ["field1", "field2"],
    "filters": [{"field": "...", "operator": "...", "value": "..."}],
    "time_range": {"type": "...", "days": 30},
    "aggregation": "sum",
    "limit": 1000
}

用户查询: {query}
"""


def intent_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Parse query intent using rule + LLM hybrid approach.

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
    default_intent = {
        "metrics": metrics,
        "dimensions": dimensions,
        "filters": [],
        "time_range": time_range or {"type": "last_30_days", "days": 30},
        "aggregation": "sum",
        "limit": 1000,
    }

    try:
        # Try LLM-based extraction
        settings = get_settings()
        llm = ChatOpenAI(
            model=settings.llm_model,
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            temperature=0.0,
        )

        prompt = INTENT_PROMPT.format(query=query)
        response = llm.invoke(prompt)

        # Parse JSON response
        content = response.content
        # Extract JSON from response (handle markdown code blocks)
        json_match = re.search(r'```json\s*(.*?)\s*```', content, re.DOTALL)
        if json_match:
            content = json_match.group(1)
        else:
            # Try to find JSON object directly
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                content = json_match.group(0)

        parsed_intent = json.loads(content)

        # Merge with default intent (LLM takes precedence)
        intent = {
            "metrics": parsed_intent.get("metrics") or default_intent["metrics"],
            "dimensions": parsed_intent.get("dimensions") or default_intent["dimensions"],
            "filters": parsed_intent.get("filters") or default_intent["filters"],
            "time_range": parsed_intent.get("time_range") or default_intent["time_range"],
            "aggregation": parsed_intent.get("aggregation") or default_intent["aggregation"],
            "limit": parsed_intent.get("limit") or default_intent["limit"],
        }

    except Exception as e:
        # Fallback to rule-based intent if LLM fails
        intent = default_intent

    return {"intent": intent}
