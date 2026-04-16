"""Tests for success case store."""

from unittest.mock import MagicMock, patch

import pytest

from src.services.success_case_store import SuccessCaseStore, get_success_case_store


class TestSuccessCaseStore:
    """Test SuccessCaseStore functionality."""

    @pytest.fixture
    def store(self):
        """Create a fresh SuccessCaseStore."""
        return SuccessCaseStore()

    def test_add_success_case_with_positive_row_count(self, store):
        """Test adding a success case when row_count > 0."""
        with patch.object(store, "_get_embedding_model") as mock_get_model:
            mock_model = MagicMock()
            mock_model.encode.return_value = MagicMock(tolist=lambda: [0.1, 0.2, 0.3])
            mock_get_model.return_value = mock_model

            store.add_success_case(
                query="上个月销售额",
                intent={"resolved_metrics": ["gmv"], "analysis_type": "single"},
                schema_tables=["dws_sales_monthly"],
                sql="SELECT SUM(gmv) FROM dws_sales_monthly",
                explanation="Test",
                row_count=5,
            )

        assert len(store._cases) == 1
        assert store._cases[0].query == "上个月销售额"

    def test_add_success_case_ignored_when_zero_rows(self, store):
        """Test that cases with row_count <= 0 are ignored."""
        store.add_success_case(
            query="test",
            intent={},
            schema_tables=[],
            sql="SELECT 1",
            explanation="Test",
            row_count=0,
        )
        assert len(store._cases) == 0

    def test_search_returns_empty_when_no_cases(self, store):
        """Test search returns empty list when no cases exist."""
        results = store.search("任意查询", top_k=2)
        assert results == []

    def test_format_for_prompt_with_cases(self, store):
        """Test formatting cases for prompt."""
        case = MagicMock()
        case.to_prompt_text.return_value = "查询: 测试\nSQL: SELECT 1"
        store._cases = [case]

        formatted = store.format_for_prompt(store._cases)
        assert "成功案例 1" in formatted
        assert "SELECT 1" in formatted

    def test_format_for_prompt_empty(self, store):
        """Test formatting when no cases provided."""
        formatted = store.format_for_prompt([])
        assert "无历史成功案例" in formatted

    def test_get_success_case_store_singleton(self):
        """Test get_success_case_store returns singleton."""
        store1 = get_success_case_store()
        store2 = get_success_case_store()
        assert store1 is store2
