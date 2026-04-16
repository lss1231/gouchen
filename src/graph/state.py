"""State definitions for NL2SQL graph."""

import operator
from typing import Any, Dict, List, Optional

from typing_extensions import Annotated, TypedDict


class ClarificationRound(TypedDict):
    """Single round of clarification Q&A."""

    question: str  # System question
    answer: str  # User answer
    field: str  # Field being clarified (metric/time/dimension/filter)


class QueryIntent(TypedDict):
    """Query intent extracted from natural language query."""

    metrics: List[str]
    resolved_metrics: List[str]
    dimensions: List[str]
    filters: List[Dict[str, Any]]
    time_range: Optional[Dict[str, Any]]
    aggregation: str
    limit: int
    analysis_type: str  # single, mom, yoy, comparison
    compare_periods: List[Dict[str, Any]]


class NL2SQLState(TypedDict):
    """State for NL2SQL graph execution."""

    query: str
    thread_id: str
    user_role: str
    intent: Optional[QueryIntent]
    relevant_tables: Annotated[List[Dict], operator.add]
    generated_sql: Optional[str]
    sql_explanation: Optional[str]
    needs_approval: bool
    approval_decision: Optional[str]
    execution_result: Optional[Dict[str, Any]]
    summary: Optional[str]  # 自然语言总结
    formatted_result: Optional[Dict[str, Any]]
    error: Optional[str]
    audit_log_id: Optional[str]
    start_time: Optional[float]
    # Clarification fields
    clarification_needed: bool
    clarification_questions: List[Dict[str, Any]]
    clarification_responses: List[Dict[str, Any]]
    clarification_history: List[ClarificationRound]
    max_clarification_rounds: int
    current_clarification_round: int
