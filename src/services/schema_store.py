"""Schema RAG using keyword-based retrieval for MVP."""
import json
import re
from pathlib import Path
from typing import List

from ..models import TableMetadata


class SchemaStore:
    """Simple keyword-based schema store for MVP.

    Uses keyword matching instead of embeddings for simplicity.
    In production, switch to vector embeddings.
    """

    def __init__(self):
        self._tables: List[TableMetadata] = []

    def index_tables(self, tables: List[TableMetadata]) -> None:
        """Index tables."""
        self._tables = tables

    def _score_table(self, table: TableMetadata, keywords: List[str]) -> float:
        """Score a table based on keyword matches."""
        score = 0.0

        # Build searchable text
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

        # Score based on keyword matches
        for keyword in keywords:
            keyword = keyword.lower()
            if keyword in searchable_text:
                # Higher score for table name matches
                if keyword in table.table_name.lower() or keyword in table.table_cn_name.lower():
                    score += 3.0
                else:
                    score += 1.0

        return score

    def retrieve(self, query: str, top_k: int = 3) -> List[TableMetadata]:
        """Retrieve relevant tables for query using keyword matching."""
        return self.search(query, top_k)

    def search(self, query: str, top_k: int = 3) -> List[TableMetadata]:
        """Search for relevant tables based on query using keyword matching."""
        # Extract keywords from query
        keywords = self._extract_keywords(query)

        # Score all tables
        scored_tables = [
            (table, self._score_table(table, keywords))
            for table in self._tables
        ]

        # Sort by score and return top_k
        scored_tables.sort(key=lambda x: x[1], reverse=True)

        return [table for table, score in scored_tables[:top_k] if score > 0]

    def _extract_keywords(self, query: str) -> List[str]:
        """Extract keywords from query."""
        # Common business terms mapping
        term_mapping = {
            # Metrics
            '销售额': ['order', 'amount', 'sales', 'paid', '金额'],
            '销售': ['order', 'sales', 'amount'],
            '订单': ['order'],
            '收入': ['amount', 'paid'],
            'gmv': ['order', 'amount'],

            # Dimensions
            '时间': ['date', 'time'],
            '日期': ['date'],
            '今天': ['date'],
            '昨天': ['date'],
            '上周': ['date', 'week'],
            '本月': ['date', 'month'],
            '上个月': ['date', 'month'],

            '地区': ['region', 'area', 'province'],
            '省份': ['region', 'province'],
            '城市': ['region', 'city'],
            '华东': ['region'],
            '华北': ['region'],
            '华南': ['region'],

            '品类': ['category'],
            '类目': ['category'],
            '商品': ['product', 'item'],
            '产品': ['product'],

            # Tables
            '订单': ['order'],
            '明细': ['item'],
            '商品': ['product'],
        }

        keywords = []
        query_lower = query.lower()

        # Check for mapped terms
        for term, related_terms in term_mapping.items():
            if term in query:
                keywords.append(term)
                keywords.extend(related_terms)

        # Add original query words
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
        tables.append(TableMetadata(**table_data))
    return tables
