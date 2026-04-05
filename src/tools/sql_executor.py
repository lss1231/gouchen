"""SQL Executor tool for NL2SQL."""
import json
import time
from typing import Any, Dict, List
from langchain.tools import tool
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from ..config import get_settings
from ..models import DatasourceType


@tool
def execute_sql(sql_json: str) -> str:
    """Execute SQL query and return results.

    Args:
        sql_json: JSON string containing SQL execution info with fields:
            - sql: The SQL query to execute
            - datasource: Data source type (mysql/doris)
            - tables: List of tables used (optional)
            - explanation: SQL explanation (optional)

    Returns:
        JSON string containing query results with fields:
        - sql: The executed SQL query
        - execution_time_ms: Execution time in milliseconds
        - row_count: Number of rows returned
        - columns: List of column info with name and type
        - rows: List of data rows
        - error: Error message if execution failed
    """
    try:
        # Parse input
        data = json.loads(sql_json)
        sql = data.get("sql", "")
        datasource = data.get("datasource", "")

        # Validate SQL safety
        if not _validate_sql_safety(sql):
            return json.dumps({
                "sql": sql,
                "execution_time_ms": 0,
                "row_count": 0,
                "columns": [],
                "rows": [],
                "error": "SQL validation failed: Only SELECT statements are allowed. DROP/DELETE/UPDATE/INSERT/CREATE/ALTER/TRUNCATE are prohibited."
            }, ensure_ascii=False)

        # Get database URL based on datasource
        settings = get_settings()
        if datasource == DatasourceType.MYSQL:
            db_url = settings.mysql_url
        elif datasource == DatasourceType.DORIS:
            db_url = settings.doris_url
        else:
            # Try to infer from SQL or default to MySQL
            db_url = settings.mysql_url

        # Execute query
        engine = create_engine(db_url)

        start_time = time.time()
        with engine.connect() as conn:
            result = conn.execute(text(sql))

            # Get column info
            columns = []
            if result.cursor:
                for col in result.cursor.description:
                    columns.append({
                        "name": col[0],
                        "type": str(col[1]) if col[1] else "unknown"
                    })

            # Fetch all rows
            rows = []
            for row in result:
                row_dict = {}
                for i, col in enumerate(columns):
                    row_dict[col["name"]] = row[i]
                rows.append(row_dict)

        execution_time_ms = int((time.time() - start_time) * 1000)

        return json.dumps({
            "sql": sql,
            "execution_time_ms": execution_time_ms,
            "row_count": len(rows),
            "columns": columns,
            "rows": rows[:1000],  # Limit to 1000 rows for safety
            "error": None
        }, ensure_ascii=False, default=str)

    except SQLAlchemyError as e:
        return json.dumps({
            "sql": sql if 'sql' in locals() else "",
            "execution_time_ms": 0,
            "row_count": 0,
            "columns": [],
            "rows": [],
            "error": f"Database error: {str(e)}"
        }, ensure_ascii=False)
    except Exception as e:
        return json.dumps({
            "sql": sql if 'sql' in locals() else "",
            "execution_time_ms": 0,
            "row_count": 0,
            "columns": [],
            "rows": [],
            "error": f"Execution error: {str(e)}"
        }, ensure_ascii=False)


def _validate_sql_safety(sql: str) -> bool:
    """Validate SQL for safety - only allow SELECT statements.

    Args:
        sql: SQL query string

    Returns:
        True if SQL is safe, False otherwise
    """
    if not sql or not isinstance(sql, str):
        return False

    # Normalize SQL for checking
    sql_normalized = sql.strip().upper()

    # Must start with SELECT
    if not sql_normalized.startswith("SELECT"):
        return False

    # List of dangerous SQL keywords to check
    dangerous_keywords = [
        "DROP",
        "DELETE",
        "UPDATE",
        "INSERT",
        "CREATE",
        "ALTER",
        "TRUNCATE",
        "GRANT",
        "REVOKE",
        "EXEC",
        "EXECUTE",
        "SP_",
        "XP_",
        "--",
        ";--",
        "/*",
        "*/",
        "UNION",
        "UNION ALL",
    ]

    # Check for dangerous keywords
    for keyword in dangerous_keywords:
        # Use word boundary check for keywords
        import re
        pattern = r'\b' + re.escape(keyword) + r'\b'
        if re.search(pattern, sql_normalized):
            return False

    # Check for multiple statements (semicolon not in string literals)
    # Remove string literals for checking
    sql_no_strings = re.sub(r"'[^']*'", "''", sql_normalized)
    if ';' in sql_no_strings and not sql_no_strings.endswith(';'):
        return False

    return True
