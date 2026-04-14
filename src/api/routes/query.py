"""Query API endpoints using LangGraph with HITL interrupt and clarification."""
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from langgraph.types import Command
from pydantic import BaseModel, Field

from ...graph.builder import get_graph
from ...services.tracer import get_tracer

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


class ClarificationAnswer(BaseModel):
    """Single clarification answer."""
    field: str = Field(..., description="Field being clarified (metric/time/dimension)")
    answer: str = Field(..., description="User's answer")


class ClarifyRequest(BaseModel):
    """Request to respond to clarification questions."""
    thread_id: str = Field(..., description="Thread ID of the query")
    answers: List[ClarificationAnswer] = Field(..., description="Answers to clarification questions")


class QueryResponse(BaseModel):
    """Response for query endpoint."""
    status: str = Field(..., description="Status: completed, pending_approval, needs_clarification, or error")
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    thread_id: Optional[str] = None
    pending_info: Optional[Dict[str, Any]] = None
    clarification_info: Optional[Dict[str, Any]] = None


class StatusResponse(BaseModel):
    """Response for status endpoint."""
    thread_id: str
    status: str
    current_state: Optional[Dict[str, Any]] = None
    next_node: Optional[str] = None
    error: Optional[str] = None


class TraceSummary(BaseModel):
    trace_id: str
    query: str
    status: str
    start_time: str
    end_time: Optional[str] = None


class TraceListResponse(BaseModel):
    traces: List[TraceSummary]


@router.post("/query", response_model=QueryResponse)
async def create_query(request: QueryRequest) -> QueryResponse:
    """
    Create and execute a natural language query with clarification support.

    The query goes through the LangGraph pipeline:
    1. Parse intent
    2. Clarification (if ambiguous)
    3. Retrieve schema
    4. Generate SQL
    5. Human review (interrupt)
    6. Execute SQL
    7. Format result
    """
    try:
        import time
        tracer = get_tracer()
        tracer.start_trace(request.thread_id, request.query, request.user_role)

        graph = get_graph()
        config = {
            "configurable": {
                "thread_id": request.thread_id,
            }
        }

        start_time = time.time()
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
            "start_time": start_time,
            "clarification_needed": False,
            "clarification_questions": [],
            "clarification_responses": [],
            "clarification_history": [],
            "max_clarification_rounds": 3,
            "current_clarification_round": 0,
        }

        result = graph.invoke(initial_state, config)
        state = graph.get_state(config)

        if state.next and "clarification" in str(state.next):
            questions = []
            for task in state.tasks:
                if hasattr(task, 'interrupts') and task.interrupts:
                    for interrupt in task.interrupts:
                        if isinstance(interrupt, dict) and 'questions' in interrupt:
                            questions = interrupt['questions']
                            break

            if not questions:
                questions = result.get("clarification_questions", [])

            clarification_info = {
                "round": result.get("current_clarification_round", 1),
                "max_rounds": result.get("max_clarification_rounds", 3),
                "questions": questions,
                "current_intent": result.get("intent"),
                "message": "查询存在歧义，需要澄清",
            }
            tracer.log_node_event(
                trace_id=request.thread_id,
                node_name="api",
                event_type="interrupt",
                state_snapshot={"status": "needs_clarification", "clarification_info": clarification_info},
            )
            return QueryResponse(
                status="needs_clarification",
                thread_id=request.thread_id,
                clarification_info=clarification_info,
            )

        if state.next and "review" in str(state.next):
            pending_info = {
                "query": result.get("query", request.query),
                "generated_sql": result.get("generated_sql"),
                "sql_explanation": result.get("sql_explanation"),
                "message": "SQL generated and pending approval. Use /api/v1/approve to approve or reject.",
            }
            tracer.log_node_event(
                trace_id=request.thread_id,
                node_name="api",
                event_type="interrupt",
                state_snapshot={"status": "pending_approval", "pending_info": pending_info},
            )
            return QueryResponse(
                status="pending_approval",
                thread_id=request.thread_id,
                pending_info=pending_info,
            )

        formatted_result = result.get("formatted_result")
        tracer.finish_trace(request.thread_id, "completed", result)
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
                "clarification_history": result.get("clarification_history"),
            },
        )

    except Exception as e:
        import traceback
        error_msg = f"Query processing failed: {str(e)}\n{traceback.format_exc()}"
        try:
            tracer = get_tracer()
            tracer.finish_trace(request.thread_id, "error", {"error": error_msg})
        except Exception:
            pass
        return QueryResponse(
            status="error",
            thread_id=request.thread_id,
            error=error_msg,
        )


@router.post("/clarify", response_model=QueryResponse)
async def clarify_query(request: ClarifyRequest) -> QueryResponse:
    """
    Respond to clarification questions and continue query execution.

    Args:
        request: ClarifyRequest with thread_id and answers

    Returns:
        QueryResponse with status and result
    """
    try:
        tracer = get_tracer()
        tracer.log_node_event(
            trace_id=request.thread_id,
            node_name="api",
            event_type="resume",
            state_snapshot={"action": "clarification_response", "answers": [a.model_dump() for a in request.answers]},
        )

        graph = get_graph()
        config = {
            "configurable": {
                "thread_id": request.thread_id,
            }
        }

        resume_payload = {
            "action": "clarification_response",
            "answers": [{"field": ans.field, "answer": ans.answer} for ans in request.answers],
        }

        result = graph.invoke(
            Command(resume=resume_payload),
            config,
        )

        state = graph.get_state(config)
        if state.next and "clarification" in str(state.next):
            return QueryResponse(
                status="needs_clarification",
                thread_id=request.thread_id,
                clarification_info={
                    "round": result.get("current_clarification_round", 1),
                    "max_rounds": result.get("max_clarification_rounds", 3),
                    "questions": result.get("clarification_questions", []),
                    "current_intent": result.get("intent"),
                    "history": result.get("clarification_history", []),
                    "message": "需要进一步澄清",
                },
            )

        if state.next and "review" in str(state.next):
            return QueryResponse(
                status="pending_approval",
                thread_id=request.thread_id,
                pending_info={
                    "query": result.get("query"),
                    "generated_sql": result.get("generated_sql"),
                    "sql_explanation": result.get("sql_explanation"),
                    "clarification_history": result.get("clarification_history", []),
                    "message": "澄清完成，SQL已生成，等待审核",
                },
            )

        formatted_result = result.get("formatted_result")
        tracer.finish_trace(request.thread_id, "completed", result)
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
                "clarification_history": result.get("clarification_history", []),
            },
        )

    except Exception as e:
        import traceback
        error_msg = f"Clarification processing failed: {str(e)}\n{traceback.format_exc()}"
        try:
            get_tracer().finish_trace(request.thread_id, "error", {"error": error_msg})
        except Exception:
            pass
        return QueryResponse(
            status="error",
            thread_id=request.thread_id,
            error=error_msg,
        )


@router.post("/approve", response_model=QueryResponse)
async def approve_query(request: ApproveRequest) -> QueryResponse:
    """
    Approve, reject, or provide feedback for a pending query.

    Uses Command(resume=...) to continue the graph execution
    from the interrupt point.
    """
    try:
        tracer = get_tracer()
        tracer.log_node_event(
            trace_id=request.thread_id,
            node_name="api",
            event_type="resume",
            state_snapshot={"action": request.decision, "edited_sql": request.edited_sql},
        )

        graph = get_graph()
        config = {
            "configurable": {
                "thread_id": request.thread_id,
            }
        }

        resume_payload: Dict[str, Any] = {
            "action": request.decision,
        }
        if request.edited_sql:
            resume_payload["edited_sql"] = request.edited_sql

        result = graph.invoke(
            Command(resume=resume_payload),
            config,
        )

        if result.get("error"):
            tracer.finish_trace(request.thread_id, "error", result)
            return QueryResponse(
                status="error",
                thread_id=request.thread_id,
                error=result.get("error"),
            )

        formatted_result = result.get("formatted_result")
        tracer.finish_trace(request.thread_id, "completed", result)
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
                "clarification_history": result.get("clarification_history", []),
            },
        )

    except Exception as e:
        import traceback
        error_msg = f"Approval processing failed: {str(e)}\n{traceback.format_exc()}"
        try:
            get_tracer().finish_trace(request.thread_id, "error", {"error": error_msg})
        except Exception:
            pass
        return QueryResponse(
            status="error",
            thread_id=request.thread_id,
            error=error_msg,
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
            next_nodes = list(state.next)
            next_node = next_nodes[0] if next_nodes else None

            if "clarification" in str(next_node):
                status = "needs_clarification"
            elif "review" in str(next_node):
                status = "pending_approval"
            else:
                status = "running"
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
                "clarification_history": state_values.get("clarification_history"),
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


@router.get("/traces", response_model=TraceListResponse)
async def list_traces(limit: int = 100) -> TraceListResponse:
    """List recent query execution traces."""
    tracer = get_tracer()
    traces = tracer.list_traces(limit=limit)
    return TraceListResponse(
        traces=[
            TraceSummary(
                trace_id=t["trace_id"],
                query=t["query"],
                status=t["status"],
                start_time=t["start_time"],
                end_time=t.get("end_time"),
            )
            for t in traces
        ]
    )


@router.get("/traces/{trace_id}")
async def get_trace(trace_id: str):
    """Get full trace details for a specific query."""
    tracer = get_tracer()
    trace = tracer.get_trace(trace_id)
    if trace is None:
        raise HTTPException(status_code=404, detail="Trace not found")
    return trace
