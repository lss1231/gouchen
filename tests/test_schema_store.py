"""Tests for schema store."""
import pytest
from pathlib import Path

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
        store = SchemaStore(use_vector_search=False)  # Disable vector search for unit tests
        tables = load_tables_from_json(schema_path)
        store.index_tables(tables)
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

    def test_extract_keywords_time_terms(self, store):
        """Test keyword extraction for time-related terms."""
        query = "上个月销售额"
        keywords = store._extract_keywords(query)

        assert "上个月" in keywords
        assert "date" in keywords
        assert "month" in keywords

    def test_extract_keywords_metric_terms(self, store):
        """Test keyword extraction for metric terms."""
        query = "销售额和订单"
        keywords = store._extract_keywords(query)

        assert "销售额" in keywords
        assert "订单" in keywords
        assert "amount" in keywords
        assert "order" in keywords

    def test_extract_keywords_dimension_terms(self, store):
        """Test keyword extraction for dimension terms."""
        query = "华东地区"
        keywords = store._extract_keywords(query)

        assert "华东" in keywords
        assert "region" in keywords

    def test_score_table_table_name_match(self, store):
        """Test that table name matches score higher."""
        tables = store._tables
        dwd_order = next(t for t in tables if t.table_name == "dwd_order_detail")

        # Table name match should score 3.0
        score = store._score_table(dwd_order, ["order"])
        assert score >= 3.0

        # Field match should score 1.0
        score = store._score_table(dwd_order, ["pay"])
        assert score >= 1.0

    def test_search_returns_order_detail_for_sales_query(self, store):
        """Test '上个月销售额' returns dwd_order_detail table."""
        results = store.search("上个月销售额")

        table_names = [t.table_name for t in results]
        assert "dwd_order_detail" in table_names or "dws_sales_daily" in table_names

    def test_search_returns_dim_region_for_region_query(self, store):
        """Test '地区维度' returns dim_region table."""
        results = store.search("地区维度省份城市")

        table_names = [t.table_name for t in results]
        assert "dim_region" in table_names

    def test_search_returns_empty_for_no_match(self, store):
        """Test search returns empty list when no tables match."""
        results = store.search("完全不相关的查询xyz123")
        assert results == []

    def test_search_respects_top_k(self, store):
        """Test search respects top_k parameter."""
        results = store.search("订单销售额", top_k=2)
        assert len(results) <= 2

    def test_get_schema_store_singleton(self):
        """Test get_schema_store returns singleton."""
        store1 = get_schema_store()
        store2 = get_schema_store()
        assert store1 is store2

    def test_search_comprehensive_query(self, store):
        """Test search with comprehensive query."""
        results = store.search("近7天各省份的订单金额")

        table_names = [t.table_name for t in results]
        # Should include dws_sales_daily for province aggregation
        assert "dws_sales_daily" in table_names or "dwd_order_detail" in table_names

    def test_score_table_with_multiple_keywords(self, store):
        """Test scoring with multiple keywords."""
        tables = store._tables
        dwd_order = next(t for t in tables if t.table_name == "dwd_order_detail")

        # Multiple matches should accumulate
        score = store._score_table(dwd_order, ["order", "pay", "user"])
        # order = 3.0 (table name), pay = 1.0, user = 1.0
        assert score >= 5.0
