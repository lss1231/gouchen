#!/usr/bin/env python3
"""Rebuild Qdrant schema_embeddings index from workspace/dbgen/schema.json."""

import os
import sys
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from sentence_transformers import SentenceTransformer
from src.services.qdrant_client import QdrantSchemaClient
from src.models import TableMetadata, DatasourceType
from src.config import get_settings

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'schema.json')


def load_schema_tables(schema_path: str):
    """Load all table metadata from schema JSON."""
    with open(schema_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    tables = []
    for table_data in data.get('tables', []):
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
    """Convert table metadata to searchable text."""
    parts = [
        f"表名: {table.table_name}",
        f"中文名: {table.table_cn_name}",
        f"描述: {table.description}",
    ]

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
    settings = get_settings()
    print("=" * 60)
    print("  Qdrant Schema Embeddings Rebuild")
    print("=" * 60)

    # 1. Load schema
    print(f"\n[1/4] Loading schema from {SCHEMA_PATH}...")
    tables = load_schema_tables(SCHEMA_PATH)
    print(f"      Loaded {len(tables)} tables")

    # 2. Load embedding model
    print(f"\n[2/4] Loading embedding model ({settings.embedding_model})...")
    model = SentenceTransformer(settings.embedding_model)
    print("      Model loaded")

    # 3. Recreate collection
    print(f"\n[3/4] Recreating Qdrant collection ({settings.qdrant_collection})...")
    client = QdrantSchemaClient(
        host=settings.qdrant_host,
        port=settings.qdrant_port,
        collection_name=settings.qdrant_collection,
    )
    client.create_collection(recreate=True)
    print("      Collection recreated")

    # 4. Generate embeddings and upsert
    print(f"\n[4/4] Generating embeddings and upserting {len(tables)} tables...")
    texts = [table_to_text(t) for t in tables]
    embeddings = model.encode(texts, normalize_embeddings=True, show_progress_bar=True)
    embeddings_list = embeddings.tolist()

    client.upsert_tables(tables, embeddings_list)

    # Verify
    count = client.get_table_count()
    print(f"\n      [OK] Collection now has {count} tables")

    print("\n" + "=" * 60)
    print("  Done!")
    print("=" * 60)


if __name__ == '__main__':
    main()
