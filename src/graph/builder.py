"""Graph builder for NL2SQL pipeline."""

from typing import Literal

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph

from .logging_middleware import wrap_node
from .nodes import (
    clarification_node,
    executor_node,
    formatter_node,
    intent_node,
    review_node,
    schema_node,
    sql_generator_node,
    summarizer_node,
)
from .state import NL2SQLState


def route_on_error(state: NL2SQLState) -> Literal["summarizer", END]:
    """Route to summarizer on success, or END on error."""
    if state.get("error"):
        return END
    return "summarizer"


def route_after_clarification(state: NL2SQLState) -> Literal["clarification", "schema"]:
    """Route back to clarification if more rounds needed, else to schema."""
    if state.get("clarification_needed") and state.get("current_clarification_round", 0) < 3:
        return "clarification"
    return "schema"


def build_graph() -> StateGraph:
    """Build and compile the NL2SQL StateGraph with clarification support."""
    workflow = StateGraph(NL2SQLState)

    # Add all nodes wrapped with logging middleware
    workflow.add_node("intent", wrap_node(intent_node, "intent"))
    workflow.add_node("clarification", wrap_node(clarification_node, "clarification"))
    workflow.add_node("schema", wrap_node(schema_node, "schema"))
    workflow.add_node("generate_sql", wrap_node(sql_generator_node, "generate_sql"))
    workflow.add_node("review", wrap_node(review_node, "review"))
    workflow.add_node("execute", wrap_node(executor_node, "execute"))
    workflow.add_node("summarizer", wrap_node(summarizer_node, "summarizer"))
    workflow.add_node("formatter", wrap_node(formatter_node, "formatter"))

    # Define edges with clarification loop
    workflow.add_edge(START, "intent")

    # Intent -> Clarification (always, clarification node decides if needed)
    workflow.add_edge("intent", "clarification")

    # Clarification -> Schema (conditional loopback for multi-round)
    workflow.add_conditional_edges("clarification", route_after_clarification)

    # Rest of the flow
    workflow.add_edge("schema", "generate_sql")
    workflow.add_edge("generate_sql", "review")
    workflow.add_edge("review", "execute")
    workflow.add_conditional_edges("execute", route_on_error)
    workflow.add_edge("summarizer", "formatter")
    workflow.add_edge("formatter", END)

    # Compile with memory checkpointer
    memory = MemorySaver()
    return workflow.compile(checkpointer=memory)


# Singleton instance
_graph = None


def get_graph():
    """Get or create the compiled graph singleton."""
    global _graph
    if _graph is None:
        _graph = build_graph()
    return _graph
