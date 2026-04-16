"""Metric knowledge service with hot-reload support."""

import os
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from ..config import get_settings
from ..models import MetricDefinition


class MetricKnowledgeService:
    """Service for loading, indexing, and resolving metrics from YAML files."""

    def __init__(self, knowledge_dir: Optional[Path] = None):
        """Initialize metric knowledge service.

        Args:
            knowledge_dir: Directory containing metric YAML files.
                          Defaults to PROJECT_ROOT/workspace/data/knowledge/metrics
        """
        if knowledge_dir is None:
            settings = get_settings()
            knowledge_dir = Path(settings.knowledge_dir) / "metrics"
        self._knowledge_dir = Path(knowledge_dir)
        self._metrics: Dict[str, MetricDefinition] = {}
        self._alias_index: Dict[str, str] = {}  # alias -> metric_name
        self._domain_files: Dict[str, Path] = {}
        self._last_mtime = 0.0
        self._load_metrics()

    def _load_metrics(self) -> None:
        """Load all metric YAML files from knowledge directory."""
        self._metrics.clear()
        self._alias_index.clear()
        self._domain_files.clear()

        if not self._knowledge_dir.exists():
            return

        max_mtime = 0.0
        for yaml_file in sorted(self._knowledge_dir.glob("*.yaml")):
            if yaml_file.name.startswith("_"):
                continue
            try:
                with open(yaml_file, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f)

                mtime = os.path.getmtime(yaml_file)
                max_mtime = max(max_mtime, mtime)

                domain = data.get("domain", yaml_file.stem)
                self._domain_files[domain] = yaml_file

                for metric_data in data.get("metrics", []):
                    metric = MetricDefinition(**metric_data)
                    self._metrics[metric.name] = metric
                    # Build alias index
                    for alias in metric.aliases:
                        self._alias_index[alias] = metric.name
                    # Also index display_name and name itself
                    self._alias_index[metric.display_name] = metric.name
                    self._alias_index[metric.name] = metric.name
            except Exception as e:
                print(f"Failed to load metric file {yaml_file}: {e}")

        self._last_mtime = max_mtime

    def _check_reload(self) -> None:
        """Check if any metric file has changed and reload if necessary."""
        if not self._knowledge_dir.exists():
            return

        max_mtime = 0.0
        for yaml_file in self._knowledge_dir.glob("*.yaml"):
            if yaml_file.name.startswith("_"):
                continue
            try:
                mtime = os.path.getmtime(yaml_file)
                max_mtime = max(max_mtime, mtime)
            except OSError:
                continue

        if max_mtime > self._last_mtime:
            self._load_metrics()

    def get_all_metrics(self) -> List[MetricDefinition]:
        """Get all loaded metric definitions."""
        self._check_reload()
        return list(self._metrics.values())

    def get_by_name(self, name: str) -> Optional[MetricDefinition]:
        """Get metric definition by standard name."""
        self._check_reload()
        return self._metrics.get(name)

    def resolve(self, query: str) -> List[str]:
        """Resolve metric mentions in a natural language query to standard names.

        Uses exact alias matching first, then keyword containment fallback.

        Args:
            query: Natural language query string.

        Returns:
            List of matched standard metric names (deduplicated).
        """
        self._check_reload()
        matched = set()
        query_lower = query.lower()

        # 1. Exact alias matching (longest first to avoid partial match issues)
        sorted_aliases = sorted(self._alias_index.keys(), key=len, reverse=True)
        for alias in sorted_aliases:
            if alias.lower() in query_lower:
                matched.add(self._alias_index[alias])

        return list(matched)

    def resolve_from_list(self, metric_names: List[str]) -> List[str]:
        """Resolve a list of possibly colloquial metric names to standard names.

        Args:
            metric_names: List of metric expressions from intent parsing.

        Returns:
            List of standard metric names.
        """
        self._check_reload()
        resolved = set()
        for name in metric_names:
            name_lower = name.lower().strip()
            # Direct name match
            if name_lower in self._metrics:
                resolved.add(name_lower)
                continue
            # Alias match
            if name_lower in self._alias_index:
                resolved.add(self._alias_index[name_lower])
                continue
            # Fuzzy: try each alias
            for alias, std_name in self._alias_index.items():
                if alias.lower() == name_lower:
                    resolved.add(std_name)
                    break
        return list(resolved)

    def format_metrics_for_prompt(self, metric_names: List[str]) -> str:
        """Format metric definitions for injection into LLM prompt.

        Args:
            metric_names: List of standard metric names to include.

        Returns:
            Formatted string describing the metrics.
        """
        self._check_reload()
        lines = []
        for name in metric_names:
            metric = self._metrics.get(name)
            if not metric:
                continue
            lines.append(f"- {metric.name} ({metric.display_name})")
            if metric.aliases:
                lines.append(f"  别名: {', '.join(metric.aliases)}")
            if metric.formula:
                lines.append(f"  公式: {metric.formula}")
            if metric.applicable_tables:
                lines.append(f"  适用表: {', '.join(metric.applicable_tables)}")
            if metric.description:
                lines.append(f"  说明: {metric.description}")
        return "\n".join(lines) if lines else "（无可用指标定义）"

    def get_metrics_catalog(self) -> str:
        """Get a concise catalog of all available metrics for intent prompt."""
        self._check_reload()
        lines = []
        for metric in self._metrics.values():
            alias_str = f" | 别名: {', '.join(metric.aliases[:5])}" if metric.aliases else ""
            lines.append(f"- {metric.name}: {metric.display_name}{alias_str}")
        return "\n".join(lines)


# Singleton instance
_metric_knowledge_service: Optional[MetricKnowledgeService] = None


def get_metric_knowledge_service() -> MetricKnowledgeService:
    """Get or create the singleton metric knowledge service."""
    global _metric_knowledge_service
    if _metric_knowledge_service is None:
        _metric_knowledge_service = MetricKnowledgeService()
    return _metric_knowledge_service
