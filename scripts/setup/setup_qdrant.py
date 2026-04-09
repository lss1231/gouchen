#!/usr/bin/env python3
"""
将 Schema 数据导入 Qdrant 向量数据库

使用方法:
    python scripts/setup_qdrant.py [--recreate]

选项:
    --recreate: 如果设置，删除并重新创建集合
"""

import os
import sys
import json
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from sentence_transformers import SentenceTransformer
from src.services.qdrant_client import QdrantSchemaClient
from src.models import TableMetadata, DatasourceType


def load_schema_tables(schema_path: str):
    """从 schema JSON 文件加载表元数据."""
    with open(schema_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    tables = []
    for table_data in data.get('tables', []):
        # 确定数据源类型
        ds_value = table_data.get('datasource', 'mysql')
        try:
            datasource = DatasourceType(ds_value)
        except ValueError:
            datasource = DatasourceType.MYSQL

        table = TableMetadata(
            table_name=table_data['table_name'],
            table_cn_name=table_data.get('table_cn_name', table_data['table_name']),
            description=table_data.get('description', ''),
            datasource=datasource,
            fields=table_data.get('fields', []),
            keywords=table_data.get('keywords', [])
        )
        tables.append(table)

    return tables


def table_to_text(table: TableMetadata) -> str:
    """将表元数据转换为可搜索文本."""
    parts = [
        f"表名: {table.table_name}",
        f"中文名: {table.table_cn_name}",
        f"描述: {table.description}",
    ]

    # Add keywords for better retrieval
    if table.keywords:
        parts.append(f"关键词: {', '.join(table.keywords)}")

    parts.append("字段:")
    for field in table.fields:
        field_name = field.get('field_name', '')
        field_cn = field.get('field_cn_name', field_name)
        desc = field.get('description', '')
        parts.append(f"  - {field_name} ({field_cn}): {desc}")

    return "\n".join(parts)


def main():
    parser = argparse.ArgumentParser(description='Setup Qdrant with schema embeddings')
    parser.add_argument('--recreate', action='store_true', help='Recreate collection')
    args = parser.parse_args()

    print("=" * 70)
    print("  Qdrant Schema 数据导入工具")
    print("=" * 70)

    # 加载 schema 文件
    schema_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'schema', 'doris_schema_enhanced.json')
    print(f"\n[1/4] 加载 schema 文件: {schema_path}")
    tables = load_schema_tables(schema_path)
    print(f"      [OK] 加载了 {len(tables)} 个表")

    # 显示表列表
    print("\n      表列表:")
    doris_tables = [t for t in tables if t.datasource == DatasourceType.DORIS]
    for t in doris_tables:
        print(f"        · {t.table_name} ({t.table_cn_name})")

    # 加载模型
    print("\n[2/4] 加载 embedding 模型 (BAAI/bge-small-zh)...")
    model = SentenceTransformer('BAAI/bge-small-zh')
    print("      [OK] 模型加载完成")

    # 连接 Qdrant
    print("\n[3/4] 连接 Qdrant...")
    client = QdrantSchemaClient()
    if args.recreate:
        print("      删除现有集合并重新创建...")
        client.create_collection(recreate=True)
    else:
        # 检查是否已有数据
        if client.is_collection_ready():
            count = client.get_table_count()
            print(f"      [WARN] 集合已存在，包含 {count} 个表")
            response = input("      是否重新导入? (y/n): ").strip().lower()
            if response == 'y':
                client.create_collection(recreate=True)
            else:
                print("      跳过导入")
                return
        else:
            client.create_collection()
            print("      [OK] 集合创建完成")

    # 生成 embeddings
    print(f"\n[4/4] 生成向量嵌入 ({len(tables)} 个表)...")
    texts = [table_to_text(t) for t in tables]
    embeddings = model.encode(texts, normalize_embeddings=True, show_progress_bar=True)
    embeddings_list = embeddings.tolist()

    # 导入 Qdrant
    print("\n      导入 Qdrant...")
    client.upsert_tables(tables, embeddings_list)

    # 验证
    count = client.get_table_count()
    print(f"\n      [OK] 导入完成! Qdrant 中共有 {count} 个表")

    print("\n" + "=" * 70)
    print("  完成! 可以运行 python scripts/test_qdrant_retrieval.py 测试召回")
    print("=" * 70)


if __name__ == '__main__':
    main()
