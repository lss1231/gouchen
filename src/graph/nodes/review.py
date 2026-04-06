"""Human-in-the-loop review node for NL2SQL graph."""

from typing import Any, Dict

from langgraph.types import interrupt


def review_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Human-in-the-loop review node using interrupt().

    This node pauses the graph execution and waits for human approval
    of the generated SQL before proceeding to execution.

    Args:
        state: Current graph state

    Returns:
        State updates with approval decision
    """
    generated_sql = state.get("generated_sql")
    sql_explanation = state.get("sql_explanation", "")
    query = state.get("query", "")

    if not generated_sql:
        # No SQL to review, skip approval
        return {
            "needs_approval": False,
            "approval_decision": "skipped",
        }

    # Prepare review information for human
    review_info = {
        "action": "review_sql",
        "query": query,
        "generated_sql": generated_sql,
        "explanation": sql_explanation,
        "message": "Please review the generated SQL query. Reply with 'approve' to execute, or provide feedback to regenerate.",
    }

    # Interrupt and wait for human input
    # The interrupt function pauses execution and returns the user's response
    user_response = interrupt(review_info)

    # Parse user response
    response_lower = user_response.lower().strip() if isinstance(user_response, str) else ""

    if response_lower in ["approve", "approved", "yes", "y", "ok", "确认", "批准", "同意"]:
        return {
            "needs_approval": True,
            "approval_decision": "approved",
        }
    elif response_lower in ["reject", "rejected", "no", "n", "deny", "拒绝", "否决", "不同意"]:
        return {
            "needs_approval": True,
            "approval_decision": "rejected",
            "error": "SQL query was rejected by human reviewer.",
        }
    else:
        # Treat as feedback for regeneration
        return {
            "needs_approval": True,
            "approval_decision": "feedback",
            "feedback": user_response,
        }
