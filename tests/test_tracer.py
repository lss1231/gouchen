"""Tests for QueryTracer service."""

import json
from pathlib import Path

from src.services.tracer import QueryTracer, get_tracer


def test_start_trace_creates_file(tmp_path):
    tracer = QueryTracer(log_dir=tmp_path)
    tracer.start_trace("thread-1", "上个月销售额", "analyst")

    files = list(tmp_path.rglob("*.json"))
    assert len(files) == 1

    data = json.loads(files[0].read_text(encoding="utf-8"))
    assert data["trace_id"] == "thread-1"
    assert data["query"] == "上个月销售额"
    assert data["user_role"] == "analyst"
    assert data["status"] == "running"
    assert len(data["events"]) == 1
    assert data["events"][0]["event_type"] == "trace_started"


def test_log_node_event_appends_event(tmp_path):
    tracer = QueryTracer(log_dir=tmp_path)
    tracer.start_trace("thread-2", "query", "admin")
    tracer.log_node_event("thread-2", "intent", "success", {"intent": {"metrics": ["gmv"]}})

    trace = tracer.get_trace("thread-2")
    assert len(trace["events"]) == 2
    assert trace["events"][1]["node_name"] == "intent"
    assert trace["events"][1]["event_type"] == "success"


def test_finish_trace_updates_status(tmp_path):
    tracer = QueryTracer(log_dir=tmp_path)
    tracer.start_trace("thread-3", "query", "admin")
    tracer.finish_trace("thread-3", "completed", {"generated_sql": "SELECT 1"})

    trace = tracer.get_trace("thread-3")
    assert trace["status"] == "completed"
    assert trace["final_state"]["generated_sql"] == "SELECT 1"


def test_list_traces_returns_recent_first(tmp_path):
    tracer = QueryTracer(log_dir=tmp_path)
    tracer.start_trace("a", "q1", "admin")
    tracer.start_trace("b", "q2", "admin")

    traces = tracer.list_traces(limit=10)
    assert len(traces) == 2
    assert traces[0]["trace_id"] == "b"
