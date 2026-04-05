"""Schema RAG using Chroma for table retrieval."""
import json
from pathlib import Path
from typing import List

from langchain_chroma import Chroma
from langchain_core.documents import Document
from langchain_openai import OpenAIEmbeddings

from src.config import get_settings
from src.models import TableMetadata


class SchemaStore:
    """Vector store for schema metadata retrieval."""

    def __init__(self):
        settings = get_settings()
        self.embeddings = OpenAIEmbeddings(
            api_key=settings.llm_api_key,
            base_url=settings.llm_base_url,
            model="text-embedding-ada-002",
        )
        self.vectorstore = Chroma(
            collection_name="gouchen_schema",
            embedding_function=self.embeddings,
            client_settings={"chroma_server_host": settings.chroma_host,
                           "chroma_server_http_port": settings.chroma_port},
        )

    def _table_to_document(self, table: TableMetadata) -> Document:
        """Convert table metadata to Document for indexing."""
        content_parts = [
            f"表名: {table.table_name}",
            f"中文名: {table.table_cn_name}",
            f"描述: {table.description}",
            f"数据源: {table.datasource}",
            "字段列表:",
        ]

        for field in table.fields:
            content_parts.append(
                f"  - {field['field_name']} ({field['field_cn_name']}): "
                f"{field['data_type']} - {field['description']}"
            )

        content = "\n".join(content_parts)

        return Document(
            page_content=content,
            metadata={
                "table_name": table.table_name,
                "table_cn_name": table.table_cn_name,
                "datasource": table.datasource,
            }
        )

    def index_tables(self, tables: List[TableMetadata]) -> None:
        """Index tables to vector store."""
        documents = [self._table_to_document(t) for t in tables]

        # Clear existing and add new
        self.vectorstore.delete_collection()
        self.vectorstore = Chroma(
            collection_name="gouchen_schema",
            embedding_function=self.embeddings,
            client_settings={"chroma_server_host": get_settings().chroma_host,
                           "chroma_server_http_port": get_settings().chroma_port},
        )
        self.vectorstore.add_documents(documents)

    def retrieve(self, query: str, top_k: int = 3) -> List[TableMetadata]:
        """Retrieve relevant tables for query."""
        results = self.vectorstore.similarity_search(query, k=top_k)

        tables = []
        for doc in results:
            tables.append(TableMetadata(
                table_name=doc.metadata["table_name"],
                table_cn_name=doc.metadata["table_cn_name"],
                description="",
                datasource=doc.metadata["datasource"],
                fields=[]
            ))
        return tables


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
        tables.append(TableMetadata(**table_data))
    return tables
