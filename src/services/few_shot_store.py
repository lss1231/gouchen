"""Few-shot example store with vector-based retrieval."""

import json
from pathlib import Path
from typing import Dict, List, Optional

from ..config import get_settings


class FewShotExample:
    """Represents a single few-shot example."""

    def __init__(self, data: Dict):
        self.category = data.get("category", "")
        self.tags = data.get("tags", [])
        self.query = data.get("query", "")
        self.intent = data.get("intent", {})
        self.schema_tables = data.get("schema_tables", [])
        self.sql = data.get("sql", "")
        self.explanation = data.get("explanation", "")
        self.data = data

    def to_search_text(self) -> str:
        """Convert example to searchable text for embedding."""
        parts = [
            f"查询: {self.query}",
            f"类型: {self.category}",
            f"标签: {', '.join(self.tags)}",
            f"指标: {', '.join(self.intent.get('resolved_metrics', []))}",
            f"分析类型: {self.intent.get('analysis_type', 'single')}",
        ]
        return "\n".join(parts)

    def to_prompt_text(self) -> str:
        """Convert example to prompt-friendly text."""
        lines = [
            f"查询: {self.query}",
            f"SQL: {self.sql}",
        ]
        if self.explanation:
            lines.append(f"说明: {self.explanation}")
        return "\n".join(lines)


class FewShotStore:
    """Store for few-shot SQL examples with hybrid retrieval (Qdrant + local fallback)."""

    def __init__(self, few_shot_dir: Optional[Path] = None):
        """Initialize few-shot store.

        Args:
            few_shot_dir: Directory containing few-shot JSON files.
        """
        if few_shot_dir is None:
            settings = get_settings()
            few_shot_dir = Path(settings.knowledge_dir) / "few_shots"
        self._few_shot_dir = Path(few_shot_dir)
        self._examples: List[FewShotExample] = []
        self._embeddings: Optional[List[List[float]]] = None
        self._qdrant_client = None
        self._collection_name: Optional[str] = None
        self._load_examples()

    def _load_examples(self) -> None:
        """Load all few-shot JSON files from directory."""
        self._examples.clear()
        if not self._few_shot_dir.exists():
            return

        for json_file in sorted(self._few_shot_dir.rglob("*.json")):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self._examples.append(FewShotExample(data))
            except Exception as e:
                print(f"Failed to load few-shot file {json_file}: {e}")

    def _get_embedding_model(self):
        """Lazy load the embedding model via SchemaEmbeddingService."""
        from .schema_embedding import get_embedding_service

        service = get_embedding_service()
        return service._get_model()

    def _get_qdrant_client(self):
        """Lazy load Qdrant client for few-shot collection."""
        if self._qdrant_client is None:
            settings = get_settings()
            from .qdrant_client import QdrantSchemaClient

            self._collection_name = getattr(
                settings, "qdrant_fewshot_collection", "few_shot_examples"
            )
            self._qdrant_client = QdrantSchemaClient(
                host=settings.qdrant_host,
                port=settings.qdrant_port,
                collection_name=self._collection_name,
            )
            # Override vector size to match schema embeddings
            self._qdrant_client.vector_size = settings.qdrant_vector_size
        return self._qdrant_client

    def build_index(self) -> None:
        """Build vector index for all loaded examples."""
        if not self._examples:
            print("No few-shot examples to index")
            return

        model = self._get_embedding_model()
        texts = [ex.to_search_text() for ex in self._examples]

        print(f"Embedding {len(texts)} few-shot examples...")
        import numpy as np

        embeddings = model.encode(texts, normalize_embeddings=True)
        self._embeddings = embeddings.tolist()

        # Try to store in Qdrant; fallback to local if unavailable
        try:
            qdrant = self._get_qdrant_client()
            qdrant.create_collection()

            from qdrant_client.models import PointStruct

            points = []
            for i, (ex, emb) in enumerate(zip(self._examples, self._embeddings)):
                point = PointStruct(
                    id=i,
                    vector=emb,
                    payload={
                        "query": ex.query,
                        "category": ex.category,
                        "tags": ex.tags,
                        "sql": ex.sql,
                        "explanation": ex.explanation,
                        "intent": ex.intent,
                        "schema_tables": ex.schema_tables,
                    },
                )
                points.append(point)

            qdrant._get_client().upsert(
                collection_name=self._collection_name,
                points=points,
            )
            print(f"Stored {len(points)} few-shot examples to Qdrant")
        except Exception as e:
            print(f"Qdrant few-shot indexing failed, using local fallback: {e}")
            self._qdrant_client = None

    def search(
        self,
        query: str,
        top_k: int = 2,
        category: Optional[str] = None,
    ) -> List[FewShotExample]:
        """Search for relevant few-shot examples.

        Args:
            query: Natural language query string.
            top_k: Number of examples to return.
            category: Optional category filter.

        Returns:
            List of most similar FewShotExample objects.
        """
        if not self._examples:
            return []

        # Filter by category if specified
        candidates = self._examples
        if category:
            candidates = [ex for ex in candidates if ex.category == category]
            if not candidates:
                candidates = self._examples

        model = self._get_embedding_model()
        query_embedding = model.encode(query, normalize_embeddings=True)

        # Try Qdrant first
        if self._qdrant_client is not None and self._embeddings is not None:
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
                        "category": payload.get("category", ""),
                        "tags": payload.get("tags", []),
                        "query": payload.get("query", ""),
                        "intent": payload.get("intent", {}),
                        "schema_tables": payload.get("schema_tables", []),
                        "sql": payload.get("sql", ""),
                        "explanation": payload.get("explanation", ""),
                    }
                    results.append(FewShotExample(data))
                return results
            except Exception:
                pass  # Fallback to local search

        # Local cosine similarity fallback
        if self._embeddings is None:
            self.build_index()

        import numpy as np

        query_vec = np.array(query_embedding)
        emb_matrix = np.array(self._embeddings)
        # Compute cosine similarity
        similarities = np.dot(emb_matrix, query_vec) / (
            np.linalg.norm(emb_matrix, axis=1) * np.linalg.norm(query_vec) + 1e-10
        )
        top_indices = np.argsort(similarities)[::-1][:top_k]
        return [self._examples[int(i)] for i in top_indices]

    def get_examples_by_category(self, category: str) -> List[FewShotExample]:
        """Get all examples for a specific category."""
        return [ex for ex in self._examples if ex.category == category]

    def format_for_prompt(self, examples: List[FewShotExample]) -> str:
        """Format examples for injection into LLM prompt."""
        if not examples:
            return "（无参考案例）"
        parts = []
        for i, ex in enumerate(examples, 1):
            parts.append(f"### 案例 {i}")
            parts.append(ex.to_prompt_text())
        return "\n\n".join(parts)


# Singleton instance
_few_shot_store: Optional[FewShotStore] = None


def get_few_shot_store() -> FewShotStore:
    """Get or create the singleton few-shot store."""
    global _few_shot_store
    if _few_shot_store is None:
        _few_shot_store = FewShotStore()
        _few_shot_store.build_index()
    return _few_shot_store
