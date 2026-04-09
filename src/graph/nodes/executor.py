"""SQL executor node for NL2SQL graph - Doris only."""

import time
from typing import Any, Dict, List

import sqlparse
from sqlalchemy import create_engine, text

from ...config import get_settings


# Forbidden keywords for additional safety validation
FORBIDDEN_KEYWORDS = [
    "insert", "update", "delete", "drop", "alter", "truncate",
    "create", "grant", "revoke", "exec", "execute", "sp_",
    "xp_", "--", ";/*", "*/", "@@", "@variable"
]


def validate_sql_safety(sql: str) -> tuple[bool, str]:
    """Validate SQL for safety issues."""
    if not sql or not isinstance(sql, str):
        return False, "SQL query is empty or invalid"

    sql_lower = sql.lower().strip()

    # Must start with SELECT or WITH (CTE)
    if not (sql_lower.startswith("select") or sql_lower.startswith("with")):
        return False, "SQL must start with SELECT or WITH"

    # Parse SQL to check for forbidden statements
    try:
        parsed = sqlparse.parse(sql)
        for statement in parsed:
            first_token = None
            for token in statement.tokens:
                if not token.is_whitespace:
                    first_token = token
                    break

            if first_token:
                token_value = str(first_token).lower()
                if token_value in ["insert", "update", "delete", "drop", "alter", "truncate", "create", "grant", "revoke"]:
                    return False, f"Forbidden SQL statement type: {token_value}"

    except Exception as e:
        return False, f"SQL parsing error: {str(e)}"

    # Additional keyword check
    for keyword in FORBIDDEN_KEYWORDS:
        if keyword in sql_lower:
            return False, f"SQL contains forbidden keyword: {keyword}"

    return True, ""


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
        engine = create_engine(settings.database_url, pool_pre_ping=True)

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
