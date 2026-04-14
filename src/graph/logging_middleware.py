"""Logging middleware for LangGraph nodes."""

from typing import Any, Dict, Callable

from langgraph.errors import GraphInterrupt

from ..services.tracer import get_tracer


def wrap_node(node_func: Callable[[Dict[str, Any]], Dict[str, Any]], node_name: str):
    """Wrap a graph node function to automatically log execution events."""

    def wrapped(state: Dict[str, Any]) -> Dict[str, Any]:
        tracer = get_tracer()
        thread_id = state.get("thread_id", "unknown")

        tracer.log_node_event(
            trace_id=thread_id,
            node_name=node_name,
            event_type="start",
            state_snapshot=state,
        )

        try:
            result = node_func(state)
            tracer.log_node_event(
                trace_id=thread_id,
                node_name=node_name,
                event_type="success",
                state_snapshot=result,
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
