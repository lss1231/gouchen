"""Query API endpoints using LangGraph with HITL interrupt."""
from typing import Any, Dict, Optional

from fastapi import APIRouter
from langgraph.types import Command
from pydantic import BaseModel, Field

from ...graph.builder import get_graph

router = APIRouter()


class QueryRequest(BaseModel):
    """Request to create a new query."""
    query: str = Field(..., description="Natural language query")
    thread_id: Optional[str] = Field("default", description="Thread ID for conversation")
    user_role: str = Field("analyst", description="User role for permission checking")
    datasource: Optional[str] = Field(None, description="Target datasource (mysql/doris)")


class ApproveRequest(BaseModel):
    """Request to approve or reject a pending query."""
    thread_id: str = Field(..., description="Thread ID of the query")
    decision: str = Field(..., description="Decision: approve, reject, or feedback")
    edited_sql: Optional[str] = Field(None, description="Edited SQL if modifying")


class QueryResponse(BaseModel):
    """Response for query endpoint."""
    status: str = Field(..., description="Status: completed, pending_approval, or error")
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    thread_id: Optional[str] = None
    pending_info: Optional[Dict[str, Any]] = None


class StatusResponse(BaseModel):
    """Response for status endpoint."""
    thread_id: str
    status: str
    current_state: Optional[Dict[str, Any]] = None
    next_node: Optional[str] = None
    error: Optional[str] = None


@router.post("/query", response_model=QueryResponse)
async def create_query(request: QueryRequest) -> QueryResponse:
    """
    Create and execute a natural language query.

    The query goes through the LangGraph pipeline:
    1. Parse intent
    2. Retrieve schema
    3. Generate SQL
    4. Human review (interrupt)
    5. Execute SQL
    6. Format result

    If the graph is interrupted at the review step, returns pending_approval status.
    """
    try:
        graph = get_graph()
        config = {
            "configurable": {
                "thread_id": request.thread_id,
            }
        }

        # Initial state
        initial_state = {
            "query": request.query,
            "thread_id": request.thread_id,
            "user_role": request.user_role,
            "intent": None,
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

        # Invoke graph
        result = graph.invoke(initial_state, config)

        # Check if interrupted (pending approval)
        state = graph.get_state(config)
        if state.next:
            # Graph is paused at interrupt
            pending_info = {
                "query": result.get("query", request.query),
                "generated_sql": result.get("generated_sql"),
                "sql_explanation": result.get("sql_explanation"),
                "message": "SQL generated and pending approval. Use /api/v1/approve to approve or reject.",
            }
            return QueryResponse(
                status="pending_approval",
                thread_id=request.thread_id,
                pending_info=pending_info,
            )

        # Graph completed
        formatted_result = result.get("formatted_result")
        return QueryResponse(
            status="completed",
            thread_id=request.thread_id,
            result={
                "query": result.get("query"),
                "generated_sql": result.get("generated_sql"),
                "sql_explanation": result.get("sql_explanation"),
                "execution_result": result.get("execution_result"),
                "formatted_result": formatted_result,
                "summary": formatted_result.get("summary") if formatted_result else None,
                "approval_decision": result.get("approval_decision"),
            },
        )

    except Exception as e:
        import traceback
        return QueryResponse(
            status="error",
            thread_id=request.thread_id,
            error=f"Query processing failed: {str(e)}\n{traceback.format_exc()}",
        )


@router.post("/approve", response_model=QueryResponse)
async def approve_query(request: ApproveRequest) -> QueryResponse:
    """
    Approve, reject, or provide feedback for a pending query.

    Uses Command(resume=...) to continue the graph execution
    from the interrupt point.
    """
    try:
        graph = get_graph()
        config = {
            "configurable": {
                "thread_id": request.thread_id,
            }
        }

        # Prepare resume payload
        resume_payload: Dict[str, Any] = {
            "action": request.decision,
        }
        if request.edited_sql:
            resume_payload["edited_sql"] = request.edited_sql

        # Continue graph with Command
        result = graph.invoke(
            Command(resume=resume_payload),
            config,
        )

        # Check if there was an error
        if result.get("error"):
            return QueryResponse(
                status="error",
                thread_id=request.thread_id,
                error=result.get("error"),
            )

        # Prepare result with formatted_result and summary
        formatted_result = result.get("formatted_result")
        return QueryResponse(
            status="completed",
            thread_id=request.thread_id,
            result={
                "query": result.get("query"),
                "generated_sql": result.get("generated_sql"),
                "sql_explanation": result.get("sql_explanation"),
                "execution_result": result.get("execution_result"),
                "formatted_result": formatted_result,
                "summary": formatted_result.get("summary") if formatted_result else None,
                "approval_decision": result.get("approval_decision"),
            },
        )

    except Exception as e:
        import traceback
        return QueryResponse(
            status="error",
            thread_id=request.thread_id,
            error=f"Approval processing failed: {str(e)}\n{traceback.format_exc()}",
        )


@router.get("/status/{thread_id}", response_model=StatusResponse)
async def get_status(thread_id: str) -> StatusResponse:
    """
    Check the current status of a query by thread_id.

    Returns the current state and next node if the graph is paused.
    """
    try:
        graph = get_graph()
        config = {
            "configurable": {
                "thread_id": thread_id,
            }
        }

        state = graph.get_state(config)

        # Get current state values
        state_values = state.values if state.values else {}

        # Determine status
        if state.next:
            status = "pending_approval"
            next_node = list(state.next)[0] if state.next else None
        else:
            status = "completed"
            next_node = None

        return StatusResponse(
            thread_id=thread_id,
            status=status,
            current_state={
                "query": state_values.get("query"),
                "generated_sql": state_values.get("generated_sql"),
                "sql_explanation": state_values.get("sql_explanation"),
                "approval_decision": state_values.get("approval_decision"),
                "error": state_values.get("error"),
            },
            next_node=next_node,
        )

    except Exception as e:
        import traceback
        return StatusResponse(
            thread_id=thread_id,
            status="error",
            error=f"Status check failed: {str(e)}\n{traceback.format_exc()}",
        )
