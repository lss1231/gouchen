"""Tests for SQL generator node."""

from unittest.mock import MagicMock, patch

import pytest

from src.graph.nodes.sql_generator import sql_generator_node


class MockSQLResult:
    """Mock structured output for SQL generation."""

    def __init__(self, sql, explanation):
        self.sql = sql
        self.explanation = explanation


@pytest.fixture
def mock_llm():
    """Mock ChatOpenAI for SQL generation tests."""
    with patch("src.graph.nodes.sql_generator.ChatOpenAI") as mock_chat:
        mock_instance = MagicMock()
        mock_chat.return_value = mock_instance

        mock_structured = MagicMock()
        mock_instance.with_structured_output.return_value = mock_structured

        # Capture the prompt passed to invoke
        def capture_invoke(prompt):
            mock_structured.last_prompt = prompt
            return MockSQLResult(
                sql="SELECT 1",
                explanation="Test SQL",
            )

        mock_structured.invoke.side_effect = capture_invoke
        yield mock_structured


@pytest.fixture
def sample_schema_tables():
    """Sample relevant tables for prompt testing."""
    return [
        {
            "table_name": "dws_sales_daily",
            "table_cn_name": "日销售汇总表",
            "description": "日级销售汇总",
            "fields": [
                {"field_name": "stat_date", "field_cn_name": "统计日期", "data_type": "DATE"},
                {"field_name": "gmv", "field_cn_name": "GMV", "data_type": "DECIMAL(18,2)"},
            ],
        }
    ]


class TestSQLGeneratorNode:
    """Test SQL generator node with mocked LLM."""

    def test_basic_sql_generation(self, mock_llm, sample_schema_tables):
        state = {
            "query": "昨天销售额",
            "intent": {
                "metrics": ["销售额"],
                "resolved_metrics": ["gmv"],
                "dimensions": ["date"],
                "filters": [],
                "time_range": {"type": "yesterday"},
                "aggregation": "sum",
                "limit": 1000,
                "analysis_type": "single",
                "compare_periods": [],
            },
            "relevant_tables": sample_schema_tables,
        }

        result = sql_generator_node(state)

        assert result["generated_sql"] == "SELECT 1"
        assert result["sql_explanation"] == "Test SQL"
        assert result["datasource"] == "doris"

        # Verify prompt contains key elements
        prompt = mock_llm.last_prompt
        assert "gmv" in prompt
        assert "日销售汇总表" in prompt
        assert "参考案例" in prompt

    def test_prompt_includes_metric_definitions(self, mock_llm, sample_schema_tables):
        state = {
            "query": "昨天留存率",
            "intent": {
                "metrics": ["留存率"],
                "resolved_metrics": ["retention_rate"],
                "dimensions": [],
                "filters": [],
                "time_range": {"type": "yesterday"},
                "aggregation": "sum",
                "limit": 1000,
                "analysis_type": "single",
                "compare_periods": [],
            },
            "relevant_tables": sample_schema_tables,
        }

        sql_generator_node(state)
        prompt = mock_llm.last_prompt

        # Should include metric definition for retention_rate
        assert "retention_rate" in prompt
        assert "指标定义参考" in prompt

    def test_prompt_includes_few_shot_examples(self, mock_llm, sample_schema_tables):
        state = {
            "query": "近7天每日销售额趋势",
            "intent": {
                "metrics": ["销售额"],
                "resolved_metrics": ["gmv"],
                "dimensions": ["date"],
                "filters": [],
                "time_range": {"type": "last_7_days", "days": 7},
                "aggregation": "sum",
                "limit": 1000,
                "analysis_type": "single",
                "compare_periods": [],
            },
            "relevant_tables": sample_schema_tables,
        }

        sql_generator_node(state)
        prompt = mock_llm.last_prompt

        # Should include few-shot section
        assert "参考案例" in prompt
        assert "案例" in prompt

    def test_no_relevant_tables(self, mock_llm):
        state = {
            "query": "测试",
            "intent": {"metrics": ["gmv"]},
            "relevant_tables": [],
        }

        result = sql_generator_node(state)

        assert result["generated_sql"] is None
        assert "No relevant tables" in result["sql_explanation"]

    def test_mom_analysis_prompt(self, mock_llm, sample_schema_tables):
        state = {
            "query": "本月销售额环比",
            "intent": {
                "metrics": ["销售额", "环比增长率"],
                "resolved_metrics": ["gmv", "mom_growth"],
                "dimensions": [],
                "filters": [],
                "time_range": {"type": "current_month"},
                "aggregation": "sum",
                "limit": 1000,
                "analysis_type": "mom",
                "compare_periods": [],
            },
            "relevant_tables": sample_schema_tables,
        }

        sql_generator_node(state)
        prompt = mock_llm.last_prompt

        assert "mom" in prompt
        assert "环比分析" in prompt
        assert "参考案例" in prompt

    def test_prompt_includes_success_cases(self, mock_llm, sample_schema_tables):
        state = {
            "query": "昨天销售额",
            "intent": {
                "metrics": ["销售额"],
                "resolved_metrics": ["gmv"],
                "dimensions": ["date"],
                "filters": [],
                "time_range": {"type": "yesterday"},
                "aggregation": "sum",
                "limit": 1000,
                "analysis_type": "single",
                "compare_periods": [],
            },
            "relevant_tables": sample_schema_tables,
        }

        sql_generator_node(state)
        prompt = mock_llm.last_prompt

        assert "历史成功案例" in prompt
        assert "成功案例" in prompt or "无历史成功案例" in prompt
