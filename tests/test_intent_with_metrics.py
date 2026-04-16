"""Tests for intent node metric resolution."""

import pytest

from src.services.metric_knowledge import get_metric_knowledge_service


class TestIntentMetricResolution:
    """Test metric resolution logic that intent_node relies on."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.service = get_metric_knowledge_service()

    def test_resolve_ecommerce_metrics(self):
        # GMV aliases
        assert "gmv" in self.service.resolve("昨天销售额是多少")
        assert "gmv" in self.service.resolve("近7天GMV趋势")
        assert "gmv" in self.service.resolve("本月交易总额")

    def test_resolve_retention_metrics(self):
        assert "retention_rate" in self.service.resolve("次日留存率")
        assert "retention_rate" in self.service.resolve("7日留存情况")
        assert "retention_rate" in self.service.resolve("用户留存分析")

    def test_resolve_user_metrics(self):
        assert "active_users" in self.service.resolve("昨天活跃用户数")
        assert "dau" in self.service.resolve("昨天DAU是多少")
        assert "new_users" in self.service.resolve("本月新增用户数")
        assert "paying_users" in self.service.resolve("付费用户数")

    def test_resolve_from_list_fallback(self):
        # Simulate intent_node passing raw metrics list
        assert self.service.resolve_from_list(["销售额"]) == ["gmv"]
        assert self.service.resolve_from_list(["利润"]) == ["profit_amount"]
        assert self.service.resolve_from_list(["订单数"]) == ["order_count"]

    def test_resolve_saas_metrics(self):
        # These metrics exist in the YAML even if tables don't exist yet
        assert "mrr" in self.service.resolve("本月MRR")
        assert "churn_rate" in self.service.resolve("用户流失率")
        assert "ltv" in self.service.resolve("LTV分析")

    def test_format_for_prompt(self):
        prompt = self.service.format_metrics_for_prompt(["gmv", "retention_rate"])
        assert "gmv" in prompt
        assert "retention_rate" in prompt
        assert "商品交易总额" in prompt
        assert "留存率" in prompt
