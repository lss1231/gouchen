from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union
from pydantic import BaseModel, Field


class DatasourceType(str, Enum):
    MYSQL = "mysql"
    DORIS = "doris"


# Literal type for datasource fields
DatasourceTypeLiteral = Literal["mysql", "doris"]


class QueryIntent(BaseModel):
    """解析后的查询意图"""
    metrics: List[str] = Field(default=[], description="指标字段")
    dimensions: List[str] = Field(default=[], description="维度字段")
    filters: List[Dict[str, Any]] = Field(default=[], description="过滤条件")
    time_range: Optional[Dict[str, str]] = Field(default=None, description="时间范围")
    aggregation: str = Field(default="sum", description="聚合方式")
    sort_by: Optional[str] = Field(default=None, description="排序字段")
    sort_order: str = Field(default="desc", description="排序方向")
    limit: int = Field(default=1000, description="返回条数限制")


class TableMetadata(BaseModel):
    """表元数据"""
    table_name: str
    table_cn_name: str
    description: str
    datasource: Union[DatasourceType, DatasourceTypeLiteral] = DatasourceType.MYSQL
    fields: List[Dict[str, Any]]


class GeneratedSQL(BaseModel):
    """生成的 SQL"""
    sql: str
    datasource: Union[DatasourceType, DatasourceTypeLiteral] = DatasourceType.MYSQL
    tables: List[str]
    explanation: str


class QueryResult(BaseModel):
    """查询结果"""
    sql: str
    execution_time_ms: int
    row_count: int
    columns: List[Dict[str, str]]
    rows: List[Dict[str, Any]]
