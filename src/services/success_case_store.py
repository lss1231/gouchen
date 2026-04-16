"""Dynamic success case store for query-to-SQL examples."""

from typing import Any, Dict, List, Optional

from ..config import get_settings


class SuccessCase:
    """Represents a successfully executed query case."""

    def __init__(self, data: Dict):
        self.query = data.get("query", "")
        self.intent = data.get("intent", {})
        self.schema_tables = data.get("schema_tables", [])
        self.sql = data.get("sql", "")
        self.explanation = data.get("explanation", "")
        self.row_count = data.get("row_count", 0)
        self.data = data

    def to_search_text(self) -> str:
        """Convert case to searchable text for embedding."""
        parts = [
            f"查询: {self.query}",
            f"指标: {', '.join(self.intent.get('resolved_metrics', []))}",
            f"分析类型: {self.intent.get('analysis_type', 'single')}",
        ]
        return "\n".join(parts)

    def to_prompt_text(self) -> str:
        """Convert case to prompt-friendly text."""
        lines = [
            f"查询: {self.query}",
            f"SQL: {self.sql}",
        ]
        if self.explanation:
            lines.append(f"说明: {self.explanation}")
        return "\n".join(lines)


class SuccessCaseStore:
    """Store for dynamic success cases with vector-based retrieval."""

    def __init__(self):
        """Initialize success case store."""
        settings = get_settings()
        self._collection_name = getattr(
            settings, "qdrant_success_case_collection", "success_cases"
        )
        self._qdrant_client = None
        self._embeddings: List[List[float]] = []
        self._cases: List[SuccessCase] = []

    def _get_embedding_model(self):
        """Lazy load the embedding model via SchemaEmbeddingService."""
        from .schema_embedding import get_embedding_service

        service = get_embedding_service()
        return service._get_model()

    def _get_qdrant_client(self):
        """Lazy load Qdrant client for success case collection."""
        if self._qdrant_client is None:
            from .qdrant_client import QdrantSchemaClient

            settings = get_settings()
            self._qdrant_client = QdrantSchemaClient(
                host=settings.qdrant_host,
                port=settings.qdrant_port,
                collection_name=self._collection_name,
            )
            self._qdrant_client.vector_size = settings.qdrant_vector_size
        return self._qdrant_client

    def add_success_case(
        self,
        query: str,
        intent: Dict[str, Any],
        schema_tables: List[str],
        sql: str,
        explanation: str,
        row_count: int,
    ) -> None:
        """Add a successful query case to the store.

        Success criteria: SQL executed successfully and returned rows.
        """
        if row_count <= 0:
            return

        case_data = {
            "query": query,
            "intent": intent,
            "schema_tables": schema_tables,
            "sql": sql,
            "explanation": explanation,
            "row_count": row_count,
        }
        case = SuccessCase(case_data)
        self._cases.append(case)

        model = self._get_embedding_model()
        text = case.to_search_text()
        embedding = model.encode(text, normalize_embeddings=True)
        embedding_list = embedding.tolist()
        self._embeddings.append(embedding_list)

        try:
            qdrant = self._get_qdrant_client()
            qdrant.create_collection()

            from qdrant_client.models import PointStruct

            point_id = len(self._cases) - 1
            point = PointStruct(
                id=point_id,
                vector=embedding_list,
                payload=case_data,
            )
            qdrant._get_client().upsert(
                collection_name=self._collection_name,
                points=[point],
            )
            print(f"Added success case #{point_id} for query: {query}")
        except Exception as e:
            print(f"Failed to store success case in Qdrant: {e}")
            # Keep in local memory even if Qdrant fails

    def search(self, query: str, top_k: int = 2) -> List[SuccessCase]:
        """Search for relevant success cases by query similarity."""
        if not self._cases:
            return []

        model = self._get_embedding_model()
        query_embedding = model.encode(query, normalize_embeddings=True)

        # Try Qdrant first
        try:
            qdrant = self._get_qdrant_client()
            from qdrant_client.models import SearchRequest

            search_request = SearchRequest(
                vector=query_embedding.tolist(),
                limit=top_k,
                with_payload=True,
            )
            response = qdrant._get_client().http.search_api.search_points(
                collection_name=self._collection_name,
                search_request=search_request,
            )
            results = []
            for result in response.result:
                payload = result.payload
                data = {
                    "query": payload.get("query", ""),
                    "intent": payload.get("intent", {}),
                    "schema_tables": payload.get("schema_tables", []),
                    "sql": payload.get("sql", ""),
                    "explanation": payload.get("explanation", ""),
                    "row_count": payload.get("row_count", 0),
                }
                results.append(SuccessCase(data))
            return results
        except Exception:
            pass  # Fallback to local search

        # Local cosine similarity fallback
        import numpy as np

        if not self._embeddings:
            return []

        query_vec = np.array(query_embedding)
        emb_matrix = np.array(self._embeddings)

        similarities = np.dot(emb_matrix, query_vec) / (
            np.linalg.norm(emb_matrix, axis=1) * np.linalg.norm(query_vec) + 1e-10
        )
        top_indices = np.argsort(similarities)[::-1][:top_k]
        return [self._cases[int(i)] for i in top_indices]

    def format_for_prompt(self, cases: List[SuccessCase]) -> str:
        """Format success cases for injection into LLM prompt."""
        if not cases:
            return "（无历史成功案例）"
        parts = []
        for i, case in enumerate(cases, 1):
            parts.append(f"### 成功案例 {i}")
            parts.append(case.to_prompt_text())
        return "\n\n".join(parts)


# Singleton instance
_success_case_store: Optional[SuccessCaseStore] = None


def get_success_case_store() -> SuccessCaseStore:
    """Get or create the singleton success case store."""
    global _success_case_store
    if _success_case_store is None:
        _success_case_store = SuccessCaseStore()
    return _success_case_store
