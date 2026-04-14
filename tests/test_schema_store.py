"""Tests for schema store."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.services.schema_store import SchemaStore, get_schema_store, load_tables_from_json


class TestSchemaStore:
    """Test SchemaStore functionality."""

    @pytest.fixture
    def schema_path(self):
        """Get path to test schema file."""
        return Path(__file__).parent.parent / "data" / "schema" / "doris_schema_enhanced.json"

    @pytest.fixture
    def store(self, schema_path):
        """Create a SchemaStore with loaded tables."""
        store = SchemaStore()
        tables = load_tables_from_json(schema_path)
        store._tables = tables
        return store

    def test_load_tables_from_json(self, schema_path):
        """Test loading tables from JSON file."""
        tables = load_tables_from_json(schema_path)
        assert len(tables) == 15  # 15 tables in doris_schema_enhanced.json

        table_names = [t.table_name for t in tables]
        assert "dim_date" in table_names
        assert "dim_region" in table_names
        assert "dwd_order_detail" in table_names
        assert "dws_sales_daily" in table_names

    def test_index_tables_builds_vector_index(self, store):
        """Test index_tables triggers vector index build."""
        mock_service = MagicMock()

        with patch("src.services.schema_embedding.SchemaEmbeddingService", return_value=mock_service):
            store._embedding_service = None
            store.index_tables(store._tables)
            mock_service.build_index.assert_called_once_with(store._tables)

    def test_index_tables_raises_on_build_failure(self, store):
        """Test index_tables raises RuntimeError when vector index build fails."""
        mock_service = MagicMock()
        mock_service.build_index.side_effect = ConnectionError("Qdrant down")

        with patch("src.services.schema_embedding.SchemaEmbeddingService", return_value=mock_service):
            store._embedding_service = None
            with pytest.raises(RuntimeError, match="Failed to build vector index"):
                store.index_tables(store._tables)

    def test_search_uses_vector_search(self, store):
        """Test search delegates to vector search."""
        mock_service = MagicMock()
        mock_service.search.return_value = store._tables[:2]

        with patch("src.services.schema_embedding.SchemaEmbeddingService", return_value=mock_service):
            store._embedding_service = None
            results = store.search("上个月销售额", top_k=2)

        mock_service.search.assert_called_once_with("上个月销售额", 2)
        assert len(results) == 2

    def test_search_raises_when_vector_search_fails(self, store):
        """Test search raises RuntimeError when vector search fails."""
        mock_service = MagicMock()
        mock_service.search.side_effect = ConnectionError("Qdrant unreachable")

        with patch("src.services.schema_embedding.SchemaEmbeddingService", return_value=mock_service):
            store._embedding_service = None
            with pytest.raises(RuntimeError, match="Vector search failed"):
                store.search("上个月销售额")

    def test_retrieve_delegates_to_search(self, store):
        """Test retrieve delegates to search."""
        mock_service = MagicMock()
        mock_service.search.return_value = store._tables[:1]

        with patch("src.services.schema_embedding.SchemaEmbeddingService", return_value=mock_service):
            store._embedding_service = None
            results = store.retrieve("订单金额", top_k=1)

        mock_service.search.assert_called_once_with("订单金额", 1)
        assert len(results) == 1

    def test_get_schema_store_singleton(self):
        """Test get_schema_store returns singleton."""
        store1 = get_schema_store()
        store2 = get_schema_store()
        assert store1 is store2
