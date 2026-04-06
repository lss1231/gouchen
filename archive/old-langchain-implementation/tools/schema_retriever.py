"""Schema Retriever tool for NL2SQL."""
import json
from langchain.tools import tool

from ..services.schema_store import get_schema_store
from ..models import TableMetadata


@tool
def retrieve_schema(query: str, top_k: int = 3) -> str:
    """Retrieve relevant database schema tables for the query.

    Args:
        query: User's natural language query
        top_k: Number of top relevant tables to retrieve (default: 3)

    Returns:
        JSON string containing list of relevant tables with fields:
        - table_name: Physical table name
        - table_cn_name: Chinese table name
        - datasource: Data source type (mysql/doris)
        - fields: List of table fields
    """
    try:
        schema_store = get_schema_store()
        tables = schema_store.retrieve(query, top_k=top_k)

        # Convert TableMetadata objects to dicts
        tables_data = []
        for table in tables:
            tables_data.append({
                "table_name": table.table_name,
                "table_cn_name": table.table_cn_name,
                "datasource": table.datasource,
                "fields": table.fields
            })

        return json.dumps({
            "tables": tables_data,
            "count": len(tables_data)
        }, ensure_ascii=False)

    except Exception as e:
        return json.dumps({
            "error": str(e),
            "query": query,
            "tables": [],
            "count": 0
        }, ensure_ascii=False)
