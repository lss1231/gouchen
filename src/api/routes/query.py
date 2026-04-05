"""Query API endpoints."""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from src.agent import get_nl2sql_agent

router = APIRouter()


class QueryRequest(BaseModel):
    query: str
    thread_id: Optional[str] = "default"


class QueryResponse(BaseModel):
    success: bool
    result: Optional[dict] = None
    error: Optional[str] = None
    pending_approval: bool = False


@router.post("/query", response_model=QueryResponse)
async def query(request: QueryRequest):
    """
    Execute natural language query.

    If SQL execution requires approval, returns pending_approval=True.
    Use /approve endpoint to approve/reject pending operations.
    """
    try:
        agent = get_nl2sql_agent()

        config = {"configurable": {"thread_id": request.thread_id}}

        result = agent.invoke({
            "messages": [{"role": "user", "content": request.query}]
        }, config=config)

        # Check if there's an interrupt pending (HITL)
        state = agent.get_state(config)
        if state.next and "__interrupt__" in str(state.next):
            return QueryResponse(
                success=True,
                pending_approval=True,
                result={"message": "SQL execution pending approval", "state": state}
            )

        # Extract final response
        messages = result.get("messages", [])
        if messages:
            final_message = messages[-1].content
        else:
            final_message = "No response"

        return QueryResponse(
            success=True,
            result={"response": final_message}
        )

    except Exception as e:
        return QueryResponse(success=False, error=str(e))


@router.post("/approve")
async def approve_action(thread_id: str, decision: str = "approve", feedback: str = ""):
    """
    Approve or reject pending SQL execution.

    Args:
        thread_id: The conversation thread ID
        decision: "approve", "reject", or "edit"
        feedback: Optional feedback message
    """
    from langgraph.types import Command

    agent = get_nl2sql_agent()
    config = {"configurable": {"thread_id": thread_id}}

    if decision == "approve":
        cmd = Command(resume={"decisions": [{"type": "approve"}]})
    elif decision == "reject":
        cmd = Command(resume={"decisions": [{"type": "reject", "message": feedback}]})
    else:
        return {"error": "Invalid decision"}

    result = agent.invoke(cmd, config=config)

    return {
        "success": True,
        "result": result.get("messages", [])[-1].content if result.get("messages") else ""
    }
