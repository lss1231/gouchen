"""Ambiguity detection for query clarification."""

from typing import Any, Dict, List, Optional

from ..state import QueryIntent

# Ambiguity type definitions
AMBIGUITY_TYPES = {
    "missing_metric": {
        "field": "metric",
        "message": "请明确您要查询的具体指标",
        "examples": ["销售额", "订单数", "用户数", "利润"],
    },
    "missing_time": {
        "field": "time",
        "message": "请明确时间范围",
        "examples": ["近7天", "本月", "上季度", "2024年4月"],
    },
    "missing_dimension": {
        "field": "dimension",
        "message": "请明确分组维度",
        "examples": ["按地区", "按品类", "按月份"],
    },
    "ambiguous_metric": {
        "field": "metric",
        "message": "查询涉及多个指标，请明确主要指标",
        "examples": [],
    },
    "ambiguous_time": {
        "field": "time",
        "message": "时间范围不明确",
        "examples": ["最近", "近期", "前段时间"],
    },
}


def detect_ambiguities(
    query: str,
    intent: QueryIntent,
    clarification_history: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Detect ambiguities in the query and generate clarification questions.

    Detection rules:
    1. Missing metric: intent.metrics is empty or query has no metric keywords
    2. Missing time: intent.time_range is empty or has fuzzy relative time
    3. Dimension ambiguity: query contains "按..." but dimension not specified
    4. Fuzzy time words: query contains "最近", "近期" etc.
    5. Multi-metric ambiguity: query contains multiple metric keywords

    Args:
        query: Original user query
        intent: Parsed intent
        clarification_history: Previous clarifications (to avoid re-asking)

    Returns:
        List of ambiguity issues, each with field, question, type
    """
    ambiguities = []

    # Check if query contains metric keywords
    metric_keywords = [
        "销售额", "销售", "订单", "收入", "gmv", "利润", "成本", "用户", "客户",
        "采购", "库存", "仓库", "出入库", "周转", "缺货", "供应商",
        "支付", "工单", "ticket", "NPS", "MRR", "ARR", "LTV",
        "留存", "流失", "订阅", "转化", "SKU", "商品", "类目",
        "渠道", "营销", "花费", "ROI", "CAC", "DAU", "活跃",
        "会员", "客单价", "金额", "数量", "评分", "健康度",
    ]
    has_metric_keyword = any(kw in query for kw in metric_keywords)

    # 1. Detect missing metric
    # Trigger if: (not clarified AND (no metrics OR vague query))
    metrics = intent.get("metrics", [])
    has_metrics = bool(metrics)
    # If query is very vague (like "最近的情况"), we need clarification even if intent has defaults
    is_vague_query = not has_metric_keyword and len(query) <= 4

    # Check if already clarified first
    if not _is_field_clarified("metric", clarification_history):
        if not has_metrics or is_vague_query:
            ambiguities.append({
                "type": "missing_metric",
                "field": "metric",
                "question": "您想查询什么指标？",
                "options": ["销售额", "订单数", "用户数", "利润", "客单价"],
                "context": "需要明确主要分析指标",
            })

    # 2. Detect time ambiguity
    time_range = intent.get("time_range", {})
    time_type = time_range.get("type", "")

    # Check if already clarified first
    if not _is_field_clarified("time", clarification_history):
        # Skip if query contains specific time patterns (like "7日留存率" where 7日 is part of metric)
        specific_time_patterns = ["留存率", "留存"]
        has_specific_metric = any(pattern in query for pattern in specific_time_patterns)

        if not has_specific_metric and (not time_type or time_type == "last_30_days"):
            # Check for fuzzy time words, but exclude queries with explicit time quantifiers
            fuzzy_time_words = ["最近", "近期", "前段时间"]
            explicit_time_quantifiers = [
                "7天", "7日", "30天", "一个月", "一周", "一年",
                "本月", "上月", "本季度", "上季度", "今年", "去年",
                "昨天", "前天", "明天", "后天",
            ]
            has_fuzzy_time = any(word in query for word in fuzzy_time_words)
            has_explicit_time = any(q in query for q in explicit_time_quantifiers)

            if has_fuzzy_time and not has_explicit_time:
                ambiguities.append({
                    "type": "ambiguous_time",
                    "field": "time",
                    "question": "请明确具体的时间范围",
                    "options": ["近7天", "近30天", "本月", "上月", "本季度"],
                    "context": f"查询包含模糊时间词，当前推断为: {time_type}",
                })

    # 3. Detect missing dimension
    if "按" in query and not intent.get("dimensions"):
        if not _is_field_clarified("dimension", clarification_history):
            ambiguities.append({
                "type": "missing_dimension",
                "field": "dimension",
                "question": "您希望按什么维度查看数据？",
                "options": ["时间(日/周/月)", "地区", "品类", "渠道", "品牌"],
                "context": "查询包含'按'但未明确维度",
            })

    # 4. Detect multi-metric ambiguity (disabled - allow multiple metrics)
    # Note: Multi-metric queries are now allowed without clarification
    # Users can specify multiple metrics naturally, SQL generator will handle it

    return ambiguities


def _is_field_clarified(field: str, history: List[Dict[str, Any]]) -> bool:
    """Check if field has already been clarified in history."""
    for round_info in history:
        if round_info.get("field") == field:
            return True
    return False


def update_intent_with_clarifications(
    intent: QueryIntent,
    clarification_history: List[Dict[str, Any]],
) -> QueryIntent:
    """
    Update intent based on clarification history.

    Updates corresponding intent fields based on user answers:
    - metric related -> updates metrics
    - time related -> updates time_range
    - dimension related -> updates dimensions
    """
    updated_intent = dict(intent)

    for round_info in clarification_history:
        field = round_info.get("field")
        answer = round_info.get("answer", "")

        if field == "metric" or field == "primary_metric":
            # Map user answer to specific fields
            metric_mapping = {
                "销售额": ["gmv"],
                "订单数": ["order_count"],
                "用户数": ["order_user_count"],
                "利润": ["profit_amount"],
                "客单价": ["actual_amount"],
                "留存率": ["retention_rate"],
            }
            if answer in metric_mapping:
                updated_intent["metrics"] = metric_mapping[answer]

        elif field == "time":
            # Map time options to time_range
            time_mapping = {
                "近7天": {"type": "last_7_days", "days": 7},
                "近30天": {"type": "last_30_days", "days": 30},
                "本月": {"type": "this_month", "days": 30},
                "上月": {"type": "last_month", "days": 30},
            }
            if answer in time_mapping:
                updated_intent["time_range"] = time_mapping[answer]

        elif field == "dimension":
            # Map dimension options
            dim_mapping = {
                "时间(日/周/月)": ["stat_date"],
                "地区": ["province_id", "region_id"],
                "品类": ["category_id"],
                "渠道": ["pay_type"],
                "品牌": ["brand_id"],
            }
            if answer in dim_mapping:
                updated_intent["dimensions"] = dim_mapping[answer]

    return updated_intent
