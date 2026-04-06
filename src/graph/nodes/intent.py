"""Intent parsing node for NL2SQL graph."""

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

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
    analysis_type: str = Field(
        description="分析类型：single(单时间查询), mom(环比), yoy(同比), comparison(对比)",
        default="single",
    )
    compare_periods: List[Dict[str, Any]] = Field(
        description="对比时间段列表，用于环比/同比分析，如 [{'month': '2024-04'}, {'month': '2024-03'}]",
        default_factory=list,
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


def extract_specific_months(query: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract specific year-month from query like '2024年4月', '3月', '去年4月'.

    Returns:
        Tuple of (current_period, compare_period) in format 'YYYY-MM' or None
    """
    current_year = datetime.now().year

    # Pattern: 2024年4月 or 2024年04月
    full_date_pattern = r'(\d{4})年(\d{1,2})月'
    matches = re.findall(full_date_pattern, query)

    if matches:
        # Found explicit year-month like "2024年4月"
        if len(matches) >= 2:
            # Two periods mentioned: e.g., "2024年4月环比3月"
            year1, month1 = matches[0]
            # Second might be just "3月" without year
            year2, month2 = matches[1] if len(matches[1]) == 2 else (year1, matches[1][1])
            current = f"{year1}-{int(month1):02d}"
            compare = f"{year2}-{int(month2):02d}"
            return current, compare
        else:
            # Only one period found, need to infer the other
            year, month = matches[0]
            current = f"{year}-{int(month):02d}"
            # Check if it's mom (环比) - compare with previous month
            if "环比" in query or "比" in query:
                # Calculate previous month
                m, y = int(month), int(year)
                if m == 1:
                    prev_month = f"{y-1}-12"
                else:
                    prev_month = f"{y}-{m-1:02d}"
                return current, prev_month
            # Check if it's yoy (同比) - compare with same month last year
            elif "同比" in query or "去年" in query:
                prev_year = f"{int(year)-1}-{int(month):02d}"
                return current, prev_year
            return current, None

    # Pattern: just "4月" or "3月份"
    month_only_pattern = r'(\d{1,2})月'
    month_matches = re.findall(month_only_pattern, query)

    if month_matches:
        months = [int(m) for m in month_matches]
        if len(months) >= 2:
            # Two months mentioned
            current = f"{current_year}-{months[0]:02d}"
            compare = f"{current_year}-{months[1]:02d}"
            return current, compare
        else:
            # Only one month, infer the other
            month = months[0]
            current = f"{current_year}-{month:02d}"
            if "环比" in query or "比" in query:
                if month == 1:
                    prev_month = f"{current_year-1}-12"
                else:
                    prev_month = f"{current_year}-{month-1:02d}"
                return current, prev_month
            elif "同比" in query or "去年" in query:
                prev_year = f"{current_year-1}-{month:02d}"
                return current, prev_year
            return current, None

    # Pattern: 去年4月
    last_year_pattern = r'去年(\d{1,2})月'
    last_year_match = re.search(last_year_pattern, query)
    if last_year_match:
        month = int(last_year_match.group(1))
        compare = f"{current_year-1}-{month:02d}"
        # Current is this year's same month
        current = f"{current_year}-{month:02d}"
        return current, compare

    return None, None


def build_compare_periods(query: str) -> Tuple[List[Dict[str, Any]], str]:
    """Build compare_periods and determine analysis_type from query.

    Returns:
        Tuple of (compare_periods list, analysis_type)
    """
    current_month, compare_month = extract_specific_months(query)

    if current_month and compare_month:
        # Has two periods - it's a comparison
        if "同比" in query:
            return [{"month": current_month}, {"month": compare_month}], "yoy"
        else:
            return [{"month": current_month}, {"month": compare_month}], "mom"
    elif current_month:
        # Only one period - single query
        return [{"month": current_month}], "single"

    # Check for relative time patterns
    if "环比" in query or ("比" in query and "上月" in query):
        # Default to last month vs previous month
        now = datetime.now()
        if now.month == 1:
            current = f"{now.year-1}-12"
            compare = f"{now.year-1}-11"
        else:
            current = f"{now.year}-{now.month:02d}"
            compare = f"{now.year}-{now.month-1:02d}"
        return [{"month": current}, {"month": compare}], "mom"

    if "同比" in query:
        # Default to this month vs same month last year
        now = datetime.now()
        current = f"{now.year}-{now.month:02d}"
        compare = f"{now.year-1}-{now.month:02d}"
        return [{"month": current}, {"month": compare}], "yoy"

    return [], "single"


INTENT_PROMPT = """你是一个专业的数据分析意图解析助手。请从用户的自然语言查询中提取查询意图。

支持的指标(metrics)：order_amount(销售额), paid_amount(实付金额), order_count(订单数), user_count(用户数), profit(利润), cost(成本)
支持的维度(dimensions)：date(日期), region(地区), province(省份), city(城市), category(品类), product(商品), brand(品牌), channel(渠道)
支持的聚合方式(aggregation)：sum(求和), count(计数), avg(平均), max(最大), min(最小)

**【重要】时间提取规则：**
必须提取查询中的具体时间点，不要默认使用"最近30天"。

1. **具体时间格式识别：**
   - "2024年4月" → 提取为 {"month": "2024-04"}
   - "3月" → 假设为今年 {"month": "2024-03"}
   - "去年4月" → 提取为 {"month": "2023-04"}
   - "上个月" → 提取具体月份

2. **对比时间段提取：**
   - "4月环比3月" → compare_periods: [{"month": "2024-04"}, {"month": "2024-03"}]
   - "今年4月同比去年" → compare_periods: [{"month": "2024-04"}, {"month": "2023-04"}]

**【重要】分析类型识别：**
- **single**: 单时间查询（如"2024年4月销售额"）
- **mom**: 环比分析，包含"环比"、"比上月"、"增长多少"等关键词
- **yoy**: 同比分析，包含"同比"、"比去年"、"同期"等关键词
- **comparison**: 多维度对比（如"北京和上海对比"）

**当前查询：**
用户查询: {query}

请提取：
1. 具体的分析类型
2. 如果是环比/同比，务必填写 compare_periods 为具体的年月格式（如 {"month": "2024-04"}）
3. 从查询中直接提取具体时间，不要默认使用相对时间"""


def build_time_range_from_months(compare_periods: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Build time_range dict from compare_periods for single period queries."""
    if not compare_periods:
        return {"type": "last_30_days", "days": 30}

    months = [p.get("month") for p in compare_periods if p.get("month")]
    if not months:
        return {"type": "last_30_days", "days": 30}

    # Sort to get earliest and latest
    months.sort()
    if len(months) == 1:
        # Single month query
        return {"type": "specific_month", "month": months[0]}
    else:
        # Multiple months - range
        return {"type": "specific_months", "start_month": months[0], "end_month": months[-1]}


def intent_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Parse query intent using rule + LLM hybrid approach with structured output.

    Args:
        state: Current graph state

    Returns:
        State updates with parsed intent
    """
    query = state.get("query", "")

    # Extract specific time periods using rules
    rule_compare_periods, rule_analysis_type = build_compare_periods(query)

    # First, try rule-based extraction
    time_range = extract_time_pattern(query)
    # If no relative time pattern found but we have specific months, use those
    if not time_range and rule_compare_periods:
        time_range = build_time_range_from_months(rule_compare_periods)

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
        "analysis_type": rule_analysis_type,
        "compare_periods": rule_compare_periods,
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

        # Merge with default intent (LLM takes precedence, but use rules as fallback)
        intent: QueryIntent = {
            "metrics": parsed.metrics if parsed.metrics else default_intent["metrics"],
            "dimensions": parsed.dimensions if parsed.dimensions else default_intent["dimensions"],
            "filters": parsed.filters if parsed.filters else default_intent["filters"],
            "time_range": parsed.time_range if parsed.time_range.get("type") != "last_30_days" else default_intent["time_range"],
            "aggregation": parsed.aggregation if parsed.aggregation else default_intent["aggregation"],
            "limit": parsed.limit if parsed.limit else default_intent["limit"],
            "analysis_type": parsed.analysis_type if parsed.analysis_type != "single" else default_intent["analysis_type"],
            "compare_periods": parsed.compare_periods if parsed.compare_periods else default_intent["compare_periods"],
        }

    except Exception:
        # Fallback to rule-based intent if LLM fails
        intent = default_intent

    return {"intent": intent}
