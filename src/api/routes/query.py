"""Query API endpoints with manual tool orchestration."""
import json
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional

from ...tools import parse_intent, retrieve_schema, generate_sql, execute_sql

router = APIRouter()

# Store pending executions (in production, use Redis/database)
pending_executions = {}


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
    Execute natural language query with manual tool orchestration.

    Workflow:
    1. Parse intent from query
    2. Retrieve relevant schema
    3. Generate SQL
    4. Return SQL for approval (HITL)
    """
    try:
        # Step 1: Parse intent
        print(f"Step 1: Parsing intent for query: {request.query}")
        intent_result = parse_intent.invoke({"query": request.query})
        intent_data = json.loads(intent_result)

        if "error" in intent_data:
            return QueryResponse(success=False, error=f"Intent parsing failed: {intent_data['error']}")

        print(f"Intent: {intent_data}")

        # Step 2: Retrieve schema
        print("Step 2: Retrieving schema...")
        schema_result = retrieve_schema.invoke({
            "query": request.query,
            "top_k": 3
        })
        schema_data = json.loads(schema_result)

        if "error" in schema_data:
            return QueryResponse(success=False, error=f"Schema retrieval failed: {schema_data['error']}")

        print(f"Schema: {schema_data}")

        # Step 3: Generate SQL
        print("Step 3: Generating SQL...")
        sql_result = generate_sql.invoke({
            "intent_json": intent_result,
            "schema_json": schema_result
        })
        sql_data = json.loads(sql_result)

        if "error" in sql_data or not sql_data.get("sql"):
            return QueryResponse(success=False, error=f"SQL generation failed: {sql_data.get('error', 'Unknown error')}")

        print(f"Generated SQL: {sql_data['sql']}")

        # Step 4: Store for HITL approval
        execution_id = f"{request.thread_id}_{id(sql_data)}"
        pending_executions[execution_id] = {
            "thread_id": request.thread_id,
            "query": request.query,
            "intent": intent_data,
            "schema": schema_data,
            "sql_data": sql_data,
            "sql": sql_data["sql"],
            "datasource": sql_data.get("datasource", "mysql"),
            "explanation": sql_data.get("explanation", "")
        }

        return QueryResponse(
            success=True,
            pending_approval=True,
            result={
                "message": "SQL generated and pending approval",
                "execution_id": execution_id,
                "query": request.query,
                "generated_sql": sql_data["sql"],
                "explanation": sql_data.get("explanation", ""),
                "tables": sql_data.get("tables", []),
                "intent": intent_data
            }
        )

    except Exception as e:
        import traceback
        return QueryResponse(
            success=False,
            error=f"Query processing failed: {str(e)}\n{traceback.format_exc()}"
        )


@router.post("/approve")
async def approve_action(execution_id: str, decision: str = "approve", feedback: str = ""):
    """
    Approve or reject pending SQL execution.

    Args:
        execution_id: The execution ID from the query response
        decision: "approve" or "reject"
        feedback: Optional feedback message
    """
    try:
        if execution_id not in pending_executions:
            return {"success": False, "error": "Execution not found or expired"}

        execution = pending_executions[execution_id]

        if decision == "reject":
            del pending_executions[execution_id]
            return {
                "success": True,
                "result": {
                    "status": "rejected",
                    "message": f"SQL execution rejected: {feedback}",
                    "sql": execution["sql"]
                }
            }

        if decision == "approve":
            # Execute the SQL
            print(f"Executing SQL: {execution['sql']}")
            sql_json = json.dumps({
                "sql": execution["sql"],
                "datasource": execution["datasource"]
            }, ensure_ascii=False)

            result = execute_sql.invoke({"sql_json": sql_json})
            result_data = json.loads(result)

            del pending_executions[execution_id]

            return {
                "success": True,
                "result": {
                    "status": "executed",
                    "query": execution["query"],
                    "sql": execution["sql"],
                    "execution_result": result_data
                }
            }

        return {"success": False, "error": "Invalid decision. Use 'approve' or 'reject'"}

    except Exception as e:
        import traceback
        return {
            "success": False,
            "error": f"Approval processing failed: {str(e)}\n{traceback.format_exc()}"
        }


@router.get("/pending")
async def list_pending():
    """List all pending executions (for admin/debugging)."""
    return {
        "pending_count": len(pending_executions),
        "executions": [
            {
                "execution_id": k,
                "query": v["query"],
                "sql": v["sql"]
            }
            for k, v in pending_executions.items()
        ]
    }
