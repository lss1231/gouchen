"""Query trace logging service for node-level observability."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


class QueryTracer:
    """Service for logging fine-grained query execution traces."""

    def __init__(self, log_dir: Optional[Path] = None):
        if log_dir is None:
            log_dir = Path(__file__).parent.parent.parent / "data" / "traces"
        self._log_dir = log_dir
        self._log_dir.mkdir(parents=True, exist_ok=True)

    def _trace_path(self, trace_id: str) -> Path:
        date_str = datetime.now().strftime("%Y-%m-%d")
        day_dir = self._log_dir / date_str
        day_dir.mkdir(parents=True, exist_ok=True)
        return day_dir / f"{trace_id}.json"

    def _now_iso(self) -> str:
        return datetime.now().isoformat()

    def start_trace(self, trace_id: str, query: str, user_role: str) -> None:
        trace = {
            "trace_id": trace_id,
            "query": query,
            "user_role": user_role,
            "start_time": self._now_iso(),
            "end_time": None,
            "status": "running",
            "events": [
                {
                    "timestamp": self._now_iso(),
                    "event_type": "trace_started",
                    "node_name": "api",
                }
            ],
            "final_state": None,
        }
        path = self._trace_path(trace_id)
        path.write_text(json.dumps(trace, ensure_ascii=False, indent=2), encoding="utf-8")

    def log_node_event(
        self,
        trace_id: str,
        node_name: str,
        event_type: str,
        state_snapshot: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        path = self._trace_path(trace_id)
        if not path.exists():
            return

        trace = json.loads(path.read_text(encoding="utf-8"))
        event = {
            "timestamp": self._now_iso(),
            "event_type": event_type,
            "node_name": node_name,
        }
        if state_snapshot is not None:
            event["state_snapshot"] = _safe_snapshot(state_snapshot)
        if error is not None:
            event["error"] = error
        trace["events"].append(event)
        path.write_text(json.dumps(trace, ensure_ascii=False, indent=2), encoding="utf-8")

    def finish_trace(
        self,
        trace_id: str,
        status: str,
        final_state: Optional[Dict[str, Any]] = None,
    ) -> None:
        path = self._trace_path(trace_id)
        if not path.exists():
            return

        trace = json.loads(path.read_text(encoding="utf-8"))
        trace["status"] = status
        trace["end_time"] = self._now_iso()
        if final_state is not None:
            trace["final_state"] = _safe_snapshot(final_state)
        trace["events"].append({
            "timestamp": self._now_iso(),
            "event_type": "trace_finished",
            "node_name": "api",
            "state_snapshot": _safe_snapshot(final_state) if final_state else None,
        })
        path.write_text(json.dumps(trace, ensure_ascii=False, indent=2), encoding="utf-8")

    def get_trace(self, trace_id: str) -> Optional[Dict[str, Any]]:
        path = self._trace_path(trace_id)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def list_traces(self, limit: int = 100) -> List[Dict[str, Any]]:
        traces = []
        for day_dir in sorted(self._log_dir.iterdir(), reverse=True):
            if not day_dir.is_dir():
                continue
            for file_path in sorted(day_dir.iterdir(), key=lambda p: (p.stat().st_mtime, p.name), reverse=True):
                try:
                    trace = json.loads(file_path.read_text(encoding="utf-8"))
                    traces.append(trace)
                except (json.JSONDecodeError, OSError):
                    continue
                if len(traces) >= limit:
                    break
            if len(traces) >= limit:
                break
        return traces


def _safe_snapshot(state: Dict[str, Any]) -> Dict[str, Any]:
    """Create a JSON-safe snapshot of state, dropping non-serializable values."""
    try:
        raw = json.loads(json.dumps(state, ensure_ascii=False, default=str))
        return _prune_snapshot(raw)
    except (TypeError, ValueError):
        return {"_snapshot_error": "failed to serialize state"}


def _prune_snapshot(state: Dict[str, Any]) -> Dict[str, Any]:
    """Prune large fields to keep trace files small and readable."""
    pruned = dict(state)

    # Relevant tables: keep only table names and descriptions
    if "relevant_tables" in pruned and isinstance(pruned["relevant_tables"], list):
        pruned["relevant_tables"] = [
            {
                "table_name": t.get("table_name"),
                "table_cn_name": t.get("table_cn_name"),
                "description": t.get("description"),
            }
            for t in pruned["relevant_tables"]
            if isinstance(t, dict)
        ]

    # Execution result: summarize, truncate rows
    if "execution_result" in pruned and isinstance(pruned["execution_result"], dict):
        er = pruned["execution_result"]
        pruned["execution_result"] = {
            "sql": er.get("sql"),
            "execution_time_ms": er.get("execution_time_ms"),
            "row_count": er.get("row_count"),
            "columns": er.get("columns"),
            "rows": er.get("rows", [])[:5],
            "_truncated": len(er.get("rows", [])) > 5,
        }

    # Formatted result: summarize, truncate rows
    if "formatted_result" in pruned and isinstance(pruned["formatted_result"], dict):
        fr = pruned["formatted_result"]
        pruned["formatted_result"] = {
            "sql": fr.get("sql"),
            "execution_time_ms": fr.get("execution_time_ms"),
            "row_count": fr.get("row_count"),
            "chart_recommendation": fr.get("chart_recommendation"),
            "summary": fr.get("summary"),
            "rows": fr.get("rows", [])[:5],
            "_truncated": len(fr.get("rows", [])) > 5,
        }

    return pruned


_tracer: Optional[QueryTracer] = None


def get_tracer(log_dir: Optional[Path] = None) -> QueryTracer:
    global _tracer
    if _tracer is None:
        _tracer = QueryTracer(log_dir)
    return _tracer
