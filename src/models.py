from enum import Enum
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field


class DatasourceType(str, Enum):
    """单一数据源类型 - 只保留 Doris"""
    DORIS = "doris"


# Literal type for datasource fields
DatasourceTypeLiteral = Literal["doris"]


class QueryIntent(BaseModel):
    """解析后的查询意图"""
    metrics: List[str] = Field(default=[], description="指标字段（原始口语化表达）")
    resolved_metrics: List[str] = Field(default=[], description="解析后的标准指标名称")
    dimensions: List[str] = Field(default=[], description="维度字段")
    filters: List[Dict[str, Any]] = Field(default=[], description="过滤条件")
    time_range: Optional[Dict[str, Any]] = Field(default=None, description="时间范围")
    aggregation: str = Field(default="sum", description="聚合方式")
    sort_by: Optional[str] = Field(default=None, description="排序字段")
    sort_order: str = Field(default="desc", description="排序方向")
    limit: int = Field(default=1000, description="返回条数限制")
    analysis_type: str = Field(default="single", description="分析类型：single/mom/yoy/comparison")
    compare_periods: List[Dict[str, Any]] = Field(default_factory=list, description="对比时间段")


class TableMetadata(BaseModel):
    """表元数据"""
    table_name: str
    table_cn_name: str
    description: str
    datasource: DatasourceType = DatasourceType.DORIS
    fields: List[Dict[str, Any]]
    keywords: List[str] = Field(default_factory=list, description="表关键词，用于向量检索")


class GeneratedSQL(BaseModel):
    """生成的 SQL"""
    sql: str
    datasource: DatasourceType = DatasourceType.DORIS
    tables: List[str]
    explanation: str


class MetricDefinition(BaseModel):
    """指标定义"""
    name: str = Field(description="指标英文名/标准标识符")
    display_name: str = Field(description="指标中文显示名")
    aliases: List[str] = Field(default_factory=list, description="指标别名列表，用于口语化匹配")
    formula: str = Field(default="", description="指标计算公式或聚合方式")
    applicable_tables: List[str] = Field(default_factory=list, description="该指标适用的表列表")
    granularity: List[str] = Field(default_factory=list, description="指标支持的粒度，如 day, month, province")
    description: str = Field(default="", description="指标业务口径说明")
    unit: str = Field(default="", description="指标单位，如 元, %, 人")
    data_type: str = Field(default="decimal", description="指标数据类型")
    keywords: List[str] = Field(default_factory=list, description="用于召回匹配的关键词")


class QueryResult(BaseModel):
    """查询结果"""
    sql: str
    execution_time_ms: int
    row_count: int
    columns: List[Dict[str, str]]
    rows: List[Dict[str, Any]]
