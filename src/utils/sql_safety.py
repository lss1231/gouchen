"""Shared SQL safety validation utilities."""

import re

import sqlparse


# Forbidden keywords for additional safety validation
FORBIDDEN_KEYWORDS = [
    "insert", "update", "delete", "drop", "alter", "truncate",
    "create", "grant", "revoke", "exec", "execute", "sp_",
    "xp_", "--", ";/*", "*/", "@@", "@variable"
]


def validate_sql_safety(sql: str) -> tuple[bool, str]:
    """Validate SQL for safety issues.

    Checks:
    1. SQL is non-empty and a string
    2. Starts with SELECT or WITH
    3. No forbidden statement types (via sqlparse)
    4. No forbidden keywords (with word boundaries to avoid false positives)
    5. No multiple statements
    """
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
                if token_value in [
                    "insert", "update", "delete", "drop", "alter",
                    "truncate", "create", "grant", "revoke",
                ]:
                    return False, f"Forbidden SQL statement type: {token_value}"

    except Exception as e:
        return False, f"SQL parsing error: {str(e)}"

    # Additional keyword check (use word boundaries to avoid false positives)
    for keyword in FORBIDDEN_KEYWORDS:
        if re.search(r"\b" + re.escape(keyword) + r"\b", sql_lower):
            return False, f"SQL contains forbidden keyword: {keyword}"

    # Check for multiple statements
    if ";" in sql and not sql_lower.endswith(";"):
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        if len(statements) > 1:
            return False, "Multiple SQL statements are not allowed"

    return True, ""
