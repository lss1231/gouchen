"""SQL executor node for NL2SQL graph - Doris only."""

import time
from typing import Any, Dict, List

from sqlalchemy import create_engine, text

from ...config import get_settings
from ...utils.sql_safety import validate_sql_safety

_engine = None


def _get_engine():
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_engine(settings.database_url, pool_pre_ping=True)
    return _engine


def executor_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Execute SQL query against Doris with safety validation."""
    generated_sql = state.get("generated_sql")
    approval_decision = state.get("approval_decision")

    # Check if SQL was approved (if HITL was used)
    if approval_decision == "rejected":
        return {
            "execution_result": None,
            "error": "SQL execution skipped: query was rejected.",
        }

    if not generated_sql:
        return {
            "execution_result": None,
            "error": "No SQL query to execute.",
        }

    # Validate SQL safety before execution
    is_safe, error_msg = validate_sql_safety(generated_sql)
    if not is_safe:
        return {
            "execution_result": None,
            "error": f"SQL safety check failed: {error_msg}",
        }

    try:
        settings = get_settings()

        # Create Doris database connection
        engine = _get_engine()

        # Execute query with timing
        start_time = time.time()

        with engine.connect() as conn:
            result = conn.execute(text(generated_sql))

            # Get column information
            columns = []
            if result.returns_rows:
                for col in result.keys():
                    columns.append({
                        "name": col,
                        "type": "string",
                    })

                # Fetch rows
                rows = []
                for row in result.fetchall():
                    row_dict = {}
                    for i, col in enumerate(result.keys()):
                        value = row[i]
                        # Handle non-serializable types
                        if hasattr(value, "isoformat"):  # datetime
                            row_dict[col] = value.isoformat()
                        else:
                            row_dict[col] = value
                    rows.append(row_dict)
            else:
                columns = []
                rows = []

        execution_time_ms = int((time.time() - start_time) * 1000)

        execution_result = {
            "sql": generated_sql,
            "execution_time_ms": execution_time_ms,
            "row_count": len(rows),
            "columns": columns,
            "rows": rows,
            "datasource": "doris",
        }

        return {"execution_result": execution_result}

    except Exception as e:
        return {
            "execution_result": None,
            "error": f"SQL execution failed: {str(e)}",
        }
