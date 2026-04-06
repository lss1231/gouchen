#!/usr/bin/env python
"""Index schema metadata to vector store using FAISS and OpenAI Embeddings."""
import json
import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pydantic import BaseModel
from typing import List, Dict, Any
from enum import Enum


class DatasourceType(str, Enum):
    MYSQL = "mysql"
    DORIS = "doris"


class TableMetadata(BaseModel):
    """表元数据"""
    table_name: str
    table_cn_name: str
    description: str
    datasource: DatasourceType
    fields: List[Dict[str, Any]]


def load_tables_from_json(path: Path) -> List[TableMetadata]:
    """Load table metadata from JSON file."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    tables = []
    for table_data in data.get("tables", []):
        tables.append(TableMetadata(**table_data))
    return tables


def main():
    print("Loading schema metadata...")
    schema_path = Path("data/schema/ecommerce_schema.json")
    if not schema_path.exists():
        print(f"Error: Schema file not found at {schema_path}")
        sys.exit(1)

    tables = load_tables_from_json(schema_path)
    print(f"Loaded {len(tables)} tables")

    print("Building vector index with FAISS and OpenAI Embeddings...")

    # Import here after setting up the path
    from src.services.schema_embedding import SchemaEmbeddingService

    # Initialize and build index
    service = SchemaEmbeddingService()
    service.build_index(tables)

    print(f"Indexed {len(tables)} tables to vector store")

    print("\nTesting vector retrieval...")
    test_queries = [
        ("上个月销售额", ["fact_order", "dim_date"]),
        ("各品类订单量", ["fact_order_item", "dim_category"]),
        ("华东地区销售情况", ["fact_order", "dim_region"]),
        ("用户购买金额", ["fact_order"]),  # 语义匹配测试
        ("销售金额", ["fact_order"]),  # 语义匹配测试
        ("商品分类信息", ["dim_category"]),  # 语义匹配测试
    ]

    for query, expected_tables in test_queries:
        results = service.search(query, top_k=3)

        print(f"\nQuery: '{query}'")
        print(f"  Expected: {', '.join(expected_tables)}")
        print(f"  Results:")
        for table in results:
            table_name = table.table_name
            table_cn_name = table.table_cn_name
            match_indicator = "*" if table_name in expected_tables else " "
            print(f"    [{match_indicator}] {table_cn_name} ({table_name})")

    print("\nIndexing complete!")


if __name__ == "__main__":
    main()
