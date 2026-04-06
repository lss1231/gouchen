"""Tests for state definitions."""

import pytest

from src.graph.state import NL2SQLState, QueryIntent


def test_create_nl2sql_state():
    """Test creating a valid NL2SQLState instance."""
    state: NL2SQLState = {
        "query": "Show me total sales by region",
        "thread_id": "test-thread-123",
        "user_role": "analyst",
        "intent": None,
        "relevant_tables": [],
        "generated_sql": None,
        "sql_explanation": None,
        "needs_approval": True,
        "approval_decision": None,
        "execution_result": None,
        "error": None,
        "audit_log_id": None,
        "start_time": None,
    }

    assert state["query"] == "Show me total sales by region"
    assert state["needs_approval"] is True


def test_create_nl2sql_state_with_intent():
    """Test creating NL2SQLState with QueryIntent."""
    intent: QueryIntent = {
        "metrics": ["total_sales"],
        "dimensions": ["region"],
        "filters": [],
        "time_range": None,
        "aggregation": "sum",
        "limit": 100,
    }

    state: NL2SQLState = {
        "query": "Show me total sales by region",
        "thread_id": "test-thread-456",
        "user_role": "admin",
        "intent": intent,
        "relevant_tables": [],
        "generated_sql": None,
        "sql_explanation": None,
        "needs_approval": False,
        "approval_decision": None,
        "execution_result": None,
        "error": None,
        "audit_log_id": None,
        "start_time": None,
    }

    assert state["query"] == "Show me total sales by region"
    assert state["needs_approval"] is False
    assert state["intent"] is not None
    assert state["intent"]["metrics"] == ["total_sales"]
    assert state["intent"]["dimensions"] == ["region"]


def test_relevant_tables_reducer():
    """Test that relevant_tables uses operator.add for accumulation."""
    from typing import Dict, List

    from typing_extensions import Annotated

    # Verify the type annotation includes Annotated with operator.add
    import operator

    from src.graph.state import NL2SQLState

    hints = NL2SQLState.__annotations__
    relevant_tables_hint = hints["relevant_tables"]

    # Check that it's an Annotated type
    assert hasattr(relevant_tables_hint, "__metadata__")
    assert hasattr(relevant_tables_hint, "__origin__")
    # The origin should be List[Dict] (a generic alias)
    assert relevant_tables_hint.__origin__ == List[Dict]
    # Check that operator.add is in the metadata (the reducer)
    assert operator.add in relevant_tables_hint.__metadata__
