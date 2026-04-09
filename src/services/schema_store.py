"""Schema RAG using vector-based retrieval (Doris only)."""
import json
import re
from pathlib import Path
from typing import List, Optional


from ..models import TableMetadata
from ..config import get_settings


class SchemaStore:
    """Schema store using vector-based retrieval."""

    def __init__(self, use_vector_search: Optional[bool] = None):
        """Initialize schema store."""
        self._tables: List[TableMetadata] = []
        self._embedding_service: Optional["SchemaEmbeddingService"] = None

        # Determine whether to use vector search
        if use_vector_search is None:
            settings = get_settings()
            self._use_vector_search = settings.use_vector_search
        else:
            self._use_vector_search = use_vector_search

    def _get_embedding_service(self) -> "SchemaEmbeddingService":
        """Lazy load embedding service."""
        if self._embedding_service is None:
            from .schema_embedding import SchemaEmbeddingService
            self._embedding_service = SchemaEmbeddingService()
        return self._embedding_service

    def index_tables(self, tables: List[TableMetadata]) -> None:
        """Index tables. If vector search is enabled, also builds the vector index."""
        self._tables = tables

        if self._use_vector_search:
            try:
                service = self._get_embedding_service()
                service.build_index(tables)
            except Exception as e:
                print(f"Warning: Vector indexing failed: {e}")
                self._use_vector_search = False

    def _score_table(self, table: TableMetadata, keywords: List[str]) -> float:
        """Score a table based on keyword matches."""
        score = 0.0

        texts = [
            table.table_name.lower(),
            table.table_cn_name.lower(),
            table.description.lower(),
        ]
        for field in table.fields:
            texts.append(field['field_name'].lower())
            texts.append(field['field_cn_name'].lower())
            texts.append(field['description'].lower())

        searchable_text = ' '.join(texts)

        for keyword in keywords:
            keyword = keyword.lower()
            if keyword in searchable_text:
                if keyword in table.table_name.lower() or keyword in table.table_cn_name.lower():
                    score += 3.0
                else:
                    score += 1.0

        return score

    def retrieve(self, query: str, top_k: int = 3) -> List[TableMetadata]:
        """Retrieve relevant tables for query."""
        return self.search(query, top_k)

    def search(self, query: str, top_k: int = 3) -> List[TableMetadata]:
        """Search for relevant tables based on query."""
        # Try vector search first if enabled
        if self._use_vector_search:
            try:
                service = self._get_embedding_service()
                if service.is_indexed():
                    return service.search(query, top_k)
            except Exception as e:
                print(f"Warning: Vector search failed: {e}")

        # Fall back to keyword matching
        return self._keyword_search(query, top_k)

    def _keyword_search(self, query: str, top_k: int = 3) -> List[TableMetadata]:
        """Search for relevant tables using keyword matching."""
        keywords = self._extract_keywords(query)

        scored_tables = [
            (table, self._score_table(table, keywords))
            for table in self._tables
        ]

        scored_tables.sort(key=lambda x: x[1], reverse=True)

        return [table for table, score in scored_tables[:top_k] if score > 0]

    def _extract_keywords(self, query: str) -> List[str]:
        """Extract keywords from query."""
        term_mapping = {
            '销售额': ['order', 'amount', 'sales', 'paid', 'gmv'],
            '销售': ['order', 'sales', 'amount'],
            '订单': ['order'],
            '收入': ['amount', 'paid'],
            'gmv': ['order', 'amount', 'sales'],
            '时间': ['date', 'time'],
            '日期': ['date'],
            '今天': ['date'],
            '昨天': ['date'],
            '本周': ['date', 'week'],
            '上周': ['date', 'week'],
            '近7天': ['date', 'week'],
            '近30天': ['date', 'month'],
            '本月': ['date', 'month'],
            '上月': ['date', 'month'],
            '上个月': ['date', 'month'],
            '今年': ['date', 'year'],
            '地区': ['region', 'area', 'province'],
            '省份': ['region', 'province'],
            '城市': ['region', 'city'],
            '华东': ['region', 'province', 'area'],
            '华北': ['region', 'province', 'area'],
            '华南': ['region', 'province', 'area'],
            '北京': ['region', 'province', 'city'],
            '上海': ['region', 'province', 'city'],
            '用户': ['user', 'customer'],
            '品类': ['category'],
            '类目': ['category'],
            '商品': ['product', 'item'],
            '品牌': ['brand'],
        }

        keywords = []
        query_lower = query.lower()

        for term, related_terms in term_mapping.items():
            if term in query:
                keywords.append(term)
                keywords.extend(related_terms)

        words = re.findall(r'\w+', query_lower)
        keywords.extend(words)

        return list(set(keywords))


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
