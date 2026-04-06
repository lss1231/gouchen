"""Schema embedding service using FAISS for vector retrieval."""
import json
import pickle
from pathlib import Path
from typing import List, Optional

from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS

from ..models import TableMetadata
from ..config import get_settings


class SchemaEmbeddingService:
    """Service for embedding and retrieving schema metadata using vectors."""

    def __init__(
        self,
        vector_store_path: Optional[str] = None,
    ):
        """Initialize the embedding service.

        Args:
            vector_store_path: Path to store FAISS index
        """
        settings = get_settings()
        self.vector_store_path = Path(vector_store_path or settings.vector_store_path)

        # Initialize OpenAI embeddings
        self._embeddings: Optional[OpenAIEmbeddings] = None

        # Initialize FAISS vector store
        self._vectorstore: Optional[FAISS] = None

    def _get_embeddings(self) -> OpenAIEmbeddings:
        """Lazy load the embedding model."""
        if self._embeddings is None:
            settings = get_settings()
            self._embeddings = OpenAIEmbeddings(
                api_key=settings.llm_api_key,
                base_url=settings.llm_base_url,
                model="text-embedding-3-small"
            )
        return self._embeddings

    def _table_to_text(self, table: TableMetadata) -> str:
        """Convert table metadata to searchable text.

        Combines table name, Chinese name, description, and field information
        into a single text for embedding.
        """
        parts = [
            f"表名: {table.table_name}",
            f"中文名: {table.table_cn_name}",
            f"描述: {table.description}",
            "字段:",
        ]

        for field in table.fields:
            field_desc = f"  - {field['field_name']} ({field['field_cn_name']}): {field['description']}"
            parts.append(field_desc)

        return "\n".join(parts)

    def build_index(self, tables: List[TableMetadata]) -> None:
        """Build vector index from tables.

        Args:
            tables: List of table metadata to index
        """
        if not tables:
            return

        embeddings = self._get_embeddings()

        # Prepare texts and metadata
        texts = []
        metadatas = []

        for table in tables:
            text = self._table_to_text(table)
            metadata = {
                "table_name": table.table_name,
                "table_cn_name": table.table_cn_name,
                "description": table.description,
                "datasource": table.datasource.value if hasattr(table.datasource, "value") else table.datasource,
                "fields": json.dumps(table.fields, ensure_ascii=False),
            }
            texts.append(text)
            metadatas.append(metadata)

        # Create FAISS vector store
        self._vectorstore = FAISS.from_texts(
            texts=texts,
            embedding=embeddings,
            metadatas=metadatas
        )

        # Save to disk
        self.vector_store_path.mkdir(parents=True, exist_ok=True)
        self._vectorstore.save_local(str(self.vector_store_path))

    def load_index(self) -> bool:
        """Load vector index from disk.

        Returns:
            True if index was loaded successfully, False otherwise
        """
        try:
            index_file = self.vector_store_path / "index.faiss"
            if not index_file.exists():
                return False

            embeddings = self._get_embeddings()
            self._vectorstore = FAISS.load_local(
                str(self.vector_store_path),
                embeddings,
                allow_dangerous_deserialization=True
            )
            return True
        except Exception:
            return False

    def search(self, query: str, top_k: int = 3) -> List[TableMetadata]:
        """Search for relevant tables using vector similarity.

        Args:
            query: Natural language query
            top_k: Number of results to return

        Returns:
            List of matching table metadata
        """
        if self._vectorstore is None:
            if not self.load_index():
                return []

        # Search
        results = self._vectorstore.similarity_search(query, k=top_k)

        # Convert back to TableMetadata
        tables = []
        for doc in results:
            metadata = doc.metadata
            table = TableMetadata(
                table_name=metadata["table_name"],
                table_cn_name=metadata["table_cn_name"],
                description=metadata["description"],
                datasource=metadata["datasource"],
                fields=json.loads(metadata["fields"]),
            )
            tables.append(table)

        return tables

    def is_indexed(self) -> bool:
        """Check if the vector index exists and has data."""
        if self._vectorstore is not None:
            return True
        return self.load_index()


# Singleton instance
_embedding_service: Optional[SchemaEmbeddingService] = None


def get_embedding_service() -> SchemaEmbeddingService:
    """Get or create the embedding service singleton."""
    global _embedding_service
    if _embedding_service is None:
        _embedding_service = SchemaEmbeddingService()
    return _embedding_service
