"""Schema retrieval node for NL2SQL graph."""

from typing import Any, Dict, List

from ...services.schema_store import get_schema_store
from ...services.permission import get_permission_service


def schema_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """Retrieve relevant schema with permission filtering."""
    query = state.get("query", "")
    user_role = state.get("user_role", "viewer")

    # Get services
    schema_store = get_schema_store()
    permission_service = get_permission_service()

    # Retrieve relevant tables based on query
    relevant_tables = schema_store.retrieve(query, top_k=5)

    # Filter tables by user role permissions
    filtered_tables = []
    for table in relevant_tables:
        if permission_service.can_query_table(user_role, table.table_name):
            table_dict = {
                "table_name": table.table_name,
                "table_cn_name": table.table_cn_name,
                "description": table.description,
                "datasource": "doris",
                "fields": table.fields,
            }
            filtered_tables.append(table_dict)

    # If no tables found after filtering, return error
    if not filtered_tables:
        return {
            "relevant_tables": [],
            "error": f"No accessible tables found for role '{user_role}'. Please check permissions.",
        }

    return {"relevant_tables": filtered_tables}
