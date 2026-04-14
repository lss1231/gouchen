"""Schema embedding service using Qdrant for vector storage."""
import os
from typing import List, Optional

# Force Hugging Face offline mode to avoid network requests on startup.
# The model is already cached locally; this suppresses the HF Hub check.

from ..models import TableMetadata
from ..config import get_settings


class SchemaEmbeddingService:
    """Service for embedding and retrieving schema metadata using vectors."""

    def __init__(self):
        """Initialize the embedding service."""
        settings = get_settings()
        self._embedding_model_name = getattr(settings, 'embedding_model', 'BAAI/bge-small-zh-v1.5')

        # Lazy imports
        self._model = None
        self._qdrant_client = None

    def _get_model(self):
        """Lazy load the embedding model."""
        if self._model is None:
            # Suppress the background auto_conversion thread that phones home.
            # The model is already cached locally; we don't need HF Hub calls.
            os.environ["HF_HUB_OFFLINE"] = "1"
            os.environ["TRANSFORMERS_OFFLINE"] = "1"

            try:
                import transformers.safetensors_conversion as _stc

                _stc.auto_conversion = lambda *args, **kwargs: None
            except Exception:
                pass

            try:
                from sentence_transformers import SentenceTransformer
            except ImportError:
                raise ImportError("sentence-transformers is not installed")

            print(f"Loading embedding model: {self._embedding_model_name}...")
            self._model = SentenceTransformer(
                self._embedding_model_name,
                local_files_only=True,
            )
        return self._model

    def _get_qdrant_client(self):
        """Lazy load Qdrant client."""
        if self._qdrant_client is None:
            from .qdrant_client import QdrantSchemaClient
            self._qdrant_client = QdrantSchemaClient()
        return self._qdrant_client

    def _table_to_text(self, table: TableMetadata) -> str:
        """Convert table metadata to searchable text."""
        fields = table.fields
        table_name = table.table_name
        table_cn_name = table.table_cn_name
        description = table.description

        parts = [
            f"表名: {table_name}",
            f"中文名: {table_cn_name}",
            f"描述: {description}",
        ]

        # Add keywords if available (important for vector search matching)
        if table.keywords:
            parts.append(f"关键词: {', '.join(table.keywords)}")

        parts.append("字段:")
        for field in fields:
            field_name = field.field_name
            field_cn_name = field.field_cn_name
            field_desc = field.description
            field_desc_str = f"  - {field_name} ({field_cn_name}): {field_desc}"
            parts.append(field_desc_str)

        return "\n".join(parts)

    def build_index(self, tables: List[TableMetadata], skip_if_exists: bool = True) -> None:
        """Build vector index from tables.

        Args:
            tables: List of table metadata to index
            skip_if_exists: If True, skip indexing if Qdrant already has data
        """
        if not tables:
            return

        qdrant = self._get_qdrant_client()

        # Check if already indexed
        if skip_if_exists and qdrant.is_collection_ready():
            existing_count = qdrant.get_table_count()
            if existing_count >= len(tables):
                print(f"Vector index already exists with {existing_count} tables, skipping rebuild")
                return

        model = self._get_model()

        # Convert tables to texts
        texts = [self._table_to_text(table) for table in tables]

        print(f"Embedding {len(texts)} tables...")

        # Generate embeddings
        embeddings = model.encode(texts, normalize_embeddings=True, show_progress_bar=True)
        embeddings_list = embeddings.tolist()

        # Store in Qdrant
        qdrant.upsert_tables(tables, embeddings_list)
        print(f"Stored {len(tables)} tables to Qdrant")

    def search(self, query: str, top_k: int = 3) -> List[TableMetadata]:
        """Search for relevant tables using vector similarity."""
        qdrant = self._get_qdrant_client()

        if not qdrant.is_collection_ready():
            return []

        # Encode query
        model = self._get_model()
        query_embedding = model.encode(query, normalize_embeddings=True)

        # Search in Qdrant (doris only)
        return qdrant.search(query_embedding.tolist(), top_k=top_k)

    def is_indexed(self) -> bool:
        """Check if the vector index exists and has data."""
        try:
            qdrant = self._get_qdrant_client()
            return qdrant.is_collection_ready()
        except Exception:
            return False


# Singleton instance
_embedding_service: Optional[SchemaEmbeddingService] = None


def get_embedding_service() -> SchemaEmbeddingService:
    """Get or create the embedding service singleton."""
    global _embedding_service
    if _embedding_service is None:
        _embedding_service = SchemaEmbeddingService()
    return _embedding_service
