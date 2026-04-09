"""Qdrant vector database client for schema storage (Doris only)."""
from typing import List, Optional

from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    PointStruct,
    VectorParams,
    SearchRequest,
)

from ..models import TableMetadata, DatasourceType
from ..config import get_settings


class QdrantSchemaClient:
    """Client for storing and retrieving schema embeddings using Qdrant."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        collection_name: Optional[str] = None,
    ):
        """Initialize Qdrant client."""
        settings = get_settings()
        self.host = host or settings.qdrant_host
        self.port = port or settings.qdrant_port
        self.collection_name = collection_name or settings.qdrant_collection
        self.vector_size = settings.qdrant_vector_size

        self._client: Optional[QdrantClient] = None

    def _get_client(self) -> QdrantClient:
        """Lazy initialize Qdrant client."""
        if self._client is None:
            self._client = QdrantClient(host=self.host, port=self.port)
        return self._client

    def create_collection(self, recreate: bool = False) -> None:
        """Create collection if not exists."""
        client = self._get_client()

        collections = client.get_collections().collections
        exists = any(c.name == self.collection_name for c in collections)

        if exists and recreate:
            client.delete_collection(self.collection_name)
            exists = False

        if not exists:
            client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(
                    size=self.vector_size,
                    distance=Distance.COSINE,
                ),
            )
            print(f"Created collection: {self.collection_name}")

    def upsert_tables(
        self,
        tables: List[TableMetadata],
        embeddings: List[List[float]],
    ) -> None:
        """Upsert table embeddings to Qdrant."""
        if len(tables) != len(embeddings):
            raise ValueError("Tables and embeddings must have same length")

        client = self._get_client()
        self.create_collection()

        points = []
        for i, (table, embedding) in enumerate(zip(tables, embeddings)):
            # Handle TableMetadata Pydantic model
            keywords = getattr(table, 'keywords', None) or []

            point = PointStruct(
                id=i,
                vector=embedding,
                payload={
                    "table_name": table.table_name,
                    "table_cn_name": table.table_cn_name,
                    "description": table.description,
                    "datasource": "doris",
                    "fields": table.fields,
                    "keywords": keywords,
                },
            )
            points.append(point)

        client.upsert(
            collection_name=self.collection_name,
            points=points,
        )
        print(f"Upserted {len(points)} tables to Qdrant")

    def search(
        self,
        query_embedding: List[float],
        top_k: int = 3,
    ) -> List[TableMetadata]:
        """Search for similar tables."""
        client = self._get_client()

        search_request = SearchRequest(
            vector=query_embedding,
            limit=top_k,
            with_payload=True,
        )
        search_response = client.http.search_api.search_points(
            collection_name=self.collection_name,
            search_request=search_request,
        )

        tables = []
        for result in search_response.result:
            payload = result.payload
            table = TableMetadata(
                table_name=payload["table_name"],
                table_cn_name=payload["table_cn_name"],
                description=payload["description"],
                datasource=DatasourceType.DORIS,
                fields=payload.get("fields", []),
                keywords=payload.get("keywords", []),
            )
            tables.append(table)

        return tables

    def is_collection_ready(self) -> bool:
        """Check if collection exists and has data."""
        try:
            client = self._get_client()
            collections = client.get_collections().collections
            exists = any(c.name == self.collection_name for c in collections)
            if not exists:
                return False

            count = client.count(self.collection_name).count
            return count > 0
        except Exception:
            return False

    def get_table_count(self) -> int:
        """Get number of tables in collection."""
        try:
            client = self._get_client()
            return client.count(self.collection_name).count
        except Exception:
            return 0
