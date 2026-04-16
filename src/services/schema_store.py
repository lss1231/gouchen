"""Schema RAG using vector-based retrieval with keyword hard mapping and rule rerank."""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from ..config import get_settings
from ..models import TableMetadata


class SchemaStore:
    """Schema store using vector-based retrieval with keyword hard mapping and rule rerank."""

    def __init__(self):
        """Initialize schema store."""
        self._tables: List[TableMetadata] = []
        self._embedding_service: "SchemaEmbeddingService" = None  # type: ignore[annotation-unchecked]
        self._keyword_mappings: List[Dict[str, Any]] = []
        self._load_keyword_mappings()

    def _load_keyword_mappings(self) -> None:
        """Load keyword-to-table hard mappings from YAML."""
        settings = get_settings()
        mapping_path = Path(settings.knowledge_dir) / "keyword_table_mappings.yaml"
        if mapping_path.exists():
            try:
                with open(mapping_path, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f)
                self._keyword_mappings = data.get("mappings", [])
            except Exception as e:
                print(f"Failed to load keyword mappings: {e}")
                self._keyword_mappings = []
        else:
            self._keyword_mappings = []

    def _get_embedding_service(self) -> "SchemaEmbeddingService":
        """Lazy load embedding service."""
        if self._embedding_service is None:
            from .schema_embedding import SchemaEmbeddingService

            self._embedding_service = SchemaEmbeddingService()
        return self._embedding_service

    def get_table_by_name(self, table_name: str) -> Optional[TableMetadata]:
        """Get a table by its name."""
        for table in self._tables:
            if table.table_name == table_name:
                return table
        return None

    def index_tables(self, tables: List[TableMetadata]) -> None:
        """Index tables by building the vector index."""
        self._tables = tables
        service = self._get_embedding_service()
        try:
            service.build_index(tables)
        except Exception as e:
            raise RuntimeError(f"Failed to build vector index: {e}") from e

    def retrieve(
        self, query: str, top_k: int = 3, resolved_metrics: Optional[List[str]] = None
    ) -> List[TableMetadata]:
        """Retrieve relevant tables for query via vector search with keyword mapping and rerank."""
        return self.search(query, top_k, resolved_metrics)

    def _apply_keyword_hard_mapping(self, query: str) -> List[str]:
        """Return table names that must be included based on keyword hard mapping."""
        forced_tables = []
        for mapping in self._keyword_mappings:
            for keyword in mapping.get("keywords", []):
                if keyword in query:
                    forced_tables.append(mapping["table"])
                    break
        return forced_tables

    def _apply_rerank(
        self,
        tables: List[TableMetadata],
        resolved_metrics: Optional[List[str]] = None,
    ) -> List[TableMetadata]:
        """Rerank tables by boosting those whose keywords match resolved metrics."""
        if not resolved_metrics:
            return tables

        boosted = []
        for table in tables:
            score = 0
            table_keywords = set(table.keywords or [])
            for metric in resolved_metrics:
                if metric in table_keywords:
                    score += 10
                # Also match metric against table_name and table_cn_name loosely
                if metric.lower() in table.table_name.lower():
                    score += 5
                if metric.lower() in table.table_cn_name.lower():
                    score += 5
            boosted.append((table, score))

        # Sort by boost score descending, then preserve original order for ties
        boosted.sort(key=lambda x: x[1], reverse=True)
        return [t for t, _ in boosted]

    def search(
        self,
        query: str,
        top_k: int = 3,
        resolved_metrics: Optional[List[str]] = None,
    ) -> List[TableMetadata]:
        """Search for relevant tables using vector similarity + keyword mapping + rerank."""
        service = self._get_embedding_service()

        # 1. Keyword hard mapping
        forced_table_names = self._apply_keyword_hard_mapping(query)
        forced_tables = []
        if forced_table_names and self._tables:
            forced_set = set()
            for table in self._tables:
                if table.table_name in forced_table_names and table.table_name not in forced_set:
                    forced_tables.append(table)
                    forced_set.add(table.table_name)

        # 2. Vector search
        vector_tables = []
        try:
            vector_tables = service.search(query, top_k)
        except Exception as e:
            raise RuntimeError(f"Vector search failed: {e}") from e

        # 3. Merge and deduplicate (forced tables first)
        seen = set()
        merged = []
        for table in forced_tables + vector_tables:
            if table.table_name not in seen:
                merged.append(table)
                seen.add(table.table_name)

        # 4. Rule-based rerank
        reranked = self._apply_rerank(merged, resolved_metrics)

        # 5. Return top_k
        return reranked[:top_k]


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
