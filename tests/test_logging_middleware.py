"""Tests for graph node logging middleware."""

import pytest

from src.graph.logging_middleware import wrap_node
from src.services.tracer import get_tracer


@pytest.fixture(autouse=True)
def reset_tracer_singleton(monkeypatch):
    """Reset the tracer singleton before each test."""
    import src.services.tracer as tracer_module

    monkeypatch.setattr(tracer_module, "_tracer", None)


def test_wrap_node_logs_success(tmp_path):
    tracer = get_tracer(log_dir=tmp_path)
    tracer.start_trace("t1", "q", "admin")

    def fake_node(state):
        return {"intent": {"metrics": ["sales"]}}

    wrapped = wrap_node(fake_node, "intent")
    result = wrapped({"thread_id": "t1", "query": "q"})

    assert result["intent"]["metrics"] == ["sales"]
    trace = tracer.get_trace("t1")
    assert any(e["event_type"] == "start" and e["node_name"] == "intent" for e in trace["events"])
    assert any(e["event_type"] == "success" and e["node_name"] == "intent" for e in trace["events"])


def test_wrap_node_logs_error(tmp_path):
    tracer = get_tracer(log_dir=tmp_path)
    tracer.start_trace("t2", "q", "admin")

    def failing_node(state):
        raise ValueError("boom")

    wrapped = wrap_node(failing_node, "schema")
    with pytest.raises(ValueError, match="boom"):
        wrapped({"thread_id": "t2"})

    trace = tracer.get_trace("t2")
    error_event = next(e for e in trace["events"] if e["event_type"] == "error")
    assert error_event["node_name"] == "schema"
    assert "boom" in error_event["error"]


def test_wrap_node_logs_interrupt(tmp_path):
    tracer = get_tracer(log_dir=tmp_path)
    tracer.start_trace("t3", "q", "admin")

    from langgraph.errors import GraphInterrupt

    def interrupt_node(state):
        raise GraphInterrupt("need input")

    wrapped = wrap_node(interrupt_node, "clarification")
    with pytest.raises(GraphInterrupt):
        wrapped({"thread_id": "t3"})

    trace = tracer.get_trace("t3")
    interrupt_event = next(e for e in trace["events"] if e["event_type"] == "interrupt")
    assert interrupt_event["node_name"] == "clarification"
