"""Schema RAG using vector-based retrieval (Doris only)."""

import json
from pathlib import Path
from typing import List

from ..models import TableMetadata


class SchemaStore:
    """Schema store using vector-based retrieval only."""

    def __init__(self):
        """Initialize schema store."""
        self._tables: List[TableMetadata] = []
        self._embedding_service: "SchemaEmbeddingService" = None  # type: ignore[annotation-unchecked]

    def _get_embedding_service(self) -> "SchemaEmbeddingService":
        """Lazy load embedding service."""
        if self._embedding_service is None:
            from .schema_embedding import SchemaEmbeddingService

            self._embedding_service = SchemaEmbeddingService()
        return self._embedding_service

    def index_tables(self, tables: List[TableMetadata]) -> None:
        """Index tables by building the vector index."""
        self._tables = tables
        service = self._get_embedding_service()
        try:
            service.build_index(tables)
        except Exception as e:
            raise RuntimeError(f"Failed to build vector index: {e}") from e

    def retrieve(self, query: str, top_k: int = 3) -> List[TableMetadata]:
        """Retrieve relevant tables for query via vector search."""
        return self.search(query, top_k)

    def search(self, query: str, top_k: int = 3) -> List[TableMetadata]:
        """Search for relevant tables using vector similarity."""
        service = self._get_embedding_service()
        try:
            return service.search(query, top_k)
        except Exception as e:
            raise RuntimeError(f"Vector search failed: {e}") from e


_schema_store = None


def get_schema_store() -> SchemaStore:
    """Get or create schema store."""
    global _schema_store
    if _schema_store is None:
        _schema_store = SchemaStore()
    return _schema_store


def load_tables_from_json(path: Path) -> List[TableMetadata]:
    """Load table metadata from JSON file."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    tables = []
    for table_data in data.get("tables", []):
        # Set default datasource to DORIS
        if "datasource" not in table_data:
            table_data["datasource"] = "doris"
        tables.append(TableMetadata(**table_data))
    return tables
