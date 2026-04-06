"""State definitions for NL2SQL graph."""

import operator
from typing import Any, Dict, List, Optional

from typing_extensions import Annotated, TypedDict


class QueryIntent(TypedDict):
    """Query intent extracted from natural language query."""

    metrics: List[str]
    dimensions: List[str]
    filters: List[Dict[str, Any]]
    time_range: Optional[Dict[str, str]]
    aggregation: str
    limit: int


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
    error: Optional[str]
    audit_log_id: Optional[str]
    start_time: Optional[float]
