"""Datasource router for multi-database query routing."""

from typing import Any, Dict, List, Optional, Set

from ..models import DatasourceType, QueryIntent


class DatasourceRouter:
    """Router to determine the appropriate datasource for a query.

    Routing logic:
    - Queries involving large fact tables (e.g., historical order data) -> Doris
    - Queries involving BITMAP/approximate calculations -> Doris
    - Simple dimension queries -> MySQL
    """

    # Tables that should always route to Doris (large fact tables)
    DORIS_TABLES: Set[str] = {
        "fact_order",
        "fact_order_item",
        "fact_user_behavior",
        "fact_transaction",
    }

    # Metrics that benefit from Doris BITMAP/approximate functions
    DORIS_OPTIMIZED_METRICS: Set[str] = {
        "uv",  # Unique visitors
        "unique_user_count",
        "distinct_user_count",
        "bitmap_count",
    }

    def __init__(
        self,
        doris_tables: Optional[Set[str]] = None,
        doris_metrics: Optional[Set[str]] = None,
    ):
        """Initialize router with optional custom configuration.

        Args:
            doris_tables: Custom set of table names that should route to Doris
            doris_metrics: Custom set of metrics that benefit from Doris functions
        """
        self.doris_tables = doris_tables or self.DORIS_TABLES.copy()
        self.doris_metrics = doris_metrics or self.DORIS_OPTIMIZED_METRICS.copy()

    def route(
        self,
        intent: QueryIntent,
        tables: List[Dict[str, Any]],
    ) -> DatasourceType:
        """Determine the appropriate datasource for the query.

        Args:
            intent: Parsed query intent with metrics, dimensions, filters
            tables: List of relevant table metadata

        Returns:
            DatasourceType.MYSQL or DatasourceType.DORIS
        """
        # Check if any table is designated as Doris table
        table_names = {t.get("table_name", "") for t in tables}

        # If any table is in the Doris table list, use Doris
        if table_names & self.doris_tables:
            return DatasourceType.DORIS

        # Check if any table has datasource explicitly set to doris
        for table in tables:
            ds = table.get("datasource", "mysql")
            if isinstance(ds, str) and ds.lower() == "doris":
                return DatasourceType.DORIS
            if hasattr(ds, "value") and ds.value == "doris":
                return DatasourceType.DORIS

        # Check if metrics suggest Doris (BITMAP/approximate calculations)
        metrics = set(intent.metrics or [])
        if metrics & self.doris_metrics:
            return DatasourceType.DORIS

        # Check for approximate calculation keywords in intent
        if self._needs_approximate_calculation(intent):
            return DatasourceType.DORIS

        # Default to MySQL for simple dimension queries
        return DatasourceType.MYSQL

    def _needs_approximate_calculation(self, intent: QueryIntent) -> bool:
        """Check if the query needs approximate/BITMAP calculations.

        Args:
            intent: Query intent to analyze

        Returns:
            True if approximate calculation is beneficial
        """
        # Check aggregation type
        aggregation = (intent.aggregation or "").lower()

        # Approximate distinct count is beneficial for large datasets
        approximate_keywords = ["approx", "estimate", "unique", "distinct"]
        if any(kw in aggregation for kw in approximate_keywords):
            return True

        # Check if query involves counting unique users/entities
        metrics_str = " ".join(intent.metrics or []).lower()
        unique_patterns = [
            "unique",
            "distinct",
            "uv",
            "dau",
            "mau",
            "去重",
            "独立",
        ]
        if any(pattern in metrics_str for pattern in unique_patterns):
            return True

        return False

    def get_doris_hint(self, intent: QueryIntent) -> Optional[str]:
        """Generate Doris-specific SQL hints based on intent.

        Args:
            intent: Query intent

        Returns:
            SQL hint string or None
        """
        hints = []

        # Add ENABLE_PIPELINE_ENGINE hint for better performance
        hints.append("ENABLE_PIPELINE_ENGINE=true")

        # Add hints for approximate calculations
        metrics_str = " ".join(intent.metrics or []).lower()
        if "approx" in metrics_str or "estimate" in metrics_str:
            hints.append("ENABLE_APPROXIMATE_QUERY=true")

        if hints:
            return f"SET {', '.join(hints)};"

        return None


def get_default_router() -> DatasourceRouter:
    """Get the default datasource router instance."""
    return DatasourceRouter()
