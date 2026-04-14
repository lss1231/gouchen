"""Logging middleware for LangGraph nodes."""

from typing import Any, Dict, Callable

from langgraph.errors import GraphInterrupt

from ..services.tracer import get_tracer


def _extract_trace_snapshot(node_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract only the most relevant fields for trace logging per node."""
    if node_name == "intent":
        return {"intent": data.get("intent")}
    if node_name == "clarification":
        return {
            "clarification_needed": data.get("clarification_needed"),
            "clarification_questions": data.get("clarification_questions"),
            "current_clarification_round": data.get("current_clarification_round"),
        }
    if node_name == "schema":
        tables = data.get("relevant_tables", [])
        return {
            "relevant_tables": [
                {"table_name": t.get("table_name"), "table_cn_name": t.get("table_cn_name")}
                for t in tables
                if isinstance(t, dict)
            ],
            "error": data.get("error"),
        }
    if node_name == "generate_sql":
        return {
            "generated_sql": data.get("generated_sql"),
            "sql_explanation": data.get("sql_explanation"),
            "error": data.get("error"),
        }
    if node_name == "review":
        return {
            "needs_approval": data.get("needs_approval"),
            "approval_decision": data.get("approval_decision"),
            "feedback": data.get("feedback"),
        }
    if node_name == "execute":
        er = data.get("execution_result")
        if isinstance(er, dict):
            return {
                "execution_result": {
                    "sql": er.get("sql"),
                    "row_count": er.get("row_count"),
                    "execution_time_ms": er.get("execution_time_ms"),
                    "datasource": er.get("datasource"),
                },
                "error": data.get("error"),
            }
        return {"execution_result": None, "error": data.get("error")}
    if node_name == "summarizer":
        return {"summary": data.get("summary")}
    if node_name == "formatter":
        fr = data.get("formatted_result")
        if isinstance(fr, dict):
            return {
                "formatted_result": {
                    "row_count": fr.get("row_count"),
                    "chart_recommendation": fr.get("chart_recommendation"),
                    "summary": fr.get("summary"),
                }
            }
        return {"formatted_result": None}
    # Fallback: only keep error for unknown nodes
    return {"error": data.get("error")}


def wrap_node(node_func: Callable[[Dict[str, Any]], Dict[str, Any]], node_name: str):
    """Wrap a graph node function to automatically log execution events."""

    def wrapped(state: Dict[str, Any]) -> Dict[str, Any]:
        tracer = get_tracer()
        thread_id = state.get("thread_id", "unknown")

        # Start event: minimal, only error matters
        tracer.log_node_event(
            trace_id=thread_id,
            node_name=node_name,
            event_type="start",
            state_snapshot={},
        )

        try:
            result = node_func(state)
            snapshot = _extract_trace_snapshot(node_name, result)
            tracer.log_node_event(
                trace_id=thread_id,
                node_name=node_name,
                event_type="success",
                state_snapshot=snapshot,
            )
            return result
        except GraphInterrupt as e:
            tracer.log_node_event(
                trace_id=thread_id,
                node_name=node_name,
                event_type="interrupt",
                error=str(e),
            )
            raise
        except Exception as e:
            tracer.log_node_event(
                trace_id=thread_id,
                node_name=node_name,
                event_type="error",
                error=str(e),
            )
            raise

    return wrapped
