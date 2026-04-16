"""Tests for metric knowledge service."""

import pytest

from src.services.metric_knowledge import MetricKnowledgeService


@pytest.fixture
def metric_service(tmp_path):
    """Create a metric service with test YAML files."""
    knowledge_dir = tmp_path / "metrics"
    knowledge_dir.mkdir()

    yaml_content = """
domain: "test_domain"
version: "1.0"
metrics:
  - name: "gmv"
    display_name: "商品交易总额"
    aliases: ["销售额", "GMV", "交易总额"]
    formula: "SUM(gmv)"
    applicable_tables: ["dws_sales_daily"]
    granularity: ["day"]
    description: "GMV描述"
    unit: "元"
    data_type: "decimal"
    keywords: ["gmv", "销售额"]
  - name: "retention_rate"
    display_name: "留存率"
    aliases: ["留存率", "次日留存", "7日留存"]
    formula: "retained_user_count / new_user_count"
    applicable_tables: ["ads_user_retention"]
    granularity: ["day"]
    description: "留存率描述"
    unit: "%"
    data_type: "decimal"
    keywords: ["留存", "retention"]
"""
    (knowledge_dir / "test_domain.yaml").write_text(yaml_content, encoding="utf-8")
    return MetricKnowledgeService(knowledge_dir)


class TestMetricKnowledgeService:
    def test_load_metrics(self, metric_service):
        metrics = metric_service.get_all_metrics()
        assert len(metrics) == 2
        assert metric_service.get_by_name("gmv") is not None
        assert metric_service.get_by_name("retention_rate") is not None

    def test_resolve_by_query(self, metric_service):
        assert "gmv" in metric_service.resolve("近7天销售额趋势")
        assert "retention_rate" in metric_service.resolve("次日留存率是多少")
        assert "gmv" in metric_service.resolve("GMV环比变化")

    def test_resolve_multiple_metrics(self, metric_service):
        resolved = metric_service.resolve("销售额和留存率对比")
        assert "gmv" in resolved
        assert "retention_rate" in resolved

    def test_resolve_from_list(self, metric_service):
        assert metric_service.resolve_from_list(["销售额"]) == ["gmv"]
        assert metric_service.resolve_from_list(["GMV"]) == ["gmv"]
        assert metric_service.resolve_from_list(["留存率", "次日留存"]) == ["retention_rate"]

    def test_format_metrics_for_prompt(self, metric_service):
        prompt = metric_service.format_metrics_for_prompt(["gmv"])
        assert "gmv" in prompt
        assert "商品交易总额" in prompt
        assert "SUM(gmv)" in prompt
        assert "dws_sales_daily" in prompt

    def test_hot_reload(self, metric_service, tmp_path):
        knowledge_dir = tmp_path / "metrics"
        yaml_file = knowledge_dir / "test_domain.yaml"
        content = yaml_file.read_text(encoding="utf-8")
        # Add a new metric
        new_metric = """
  - name: "new_metric"
    display_name: "新指标"
    aliases: ["新指标"]
    formula: "SUM(x)"
    applicable_tables: ["t1"]
    description: "新指标描述"
"""
        yaml_file.write_text(content.replace("metrics:", "metrics:" + new_metric), encoding="utf-8")

        # Service should detect the change
        metrics = metric_service.get_all_metrics()
        assert len(metrics) == 3
        assert metric_service.get_by_name("new_metric") is not None
