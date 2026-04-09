"""Test script for Qdrant vector search (standalone)."""
import sys
import os
import json

# Add src parent to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient


def load_schema():
    """Load Doris schema from JSON file."""
    schema_path = os.path.join(
        os.path.dirname(__file__), '..', 'data', 'schema', 'doris_schema_enhanced.json'
    )
    with open(schema_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def table_to_text(table: dict) -> str:
    """Convert table metadata to searchable text."""
    fields = table.get('fields', [])
    table_name = table.get('table_name', '')
    table_cn_name = table.get('table_cn_name', '')
    description = table.get('description', '')

    parts = [
        f"表名: {table_name}",
        f"中文名: {table_cn_name}",
        f"描述: {description}",
    ]

    # Add keywords if available
    keywords = table.get('keywords', [])
    if keywords:
        parts.append(f"关键词: {', '.join(keywords)}")

    parts.append("字段:")
    for field in fields:
        field_name = field.get('field_name', '')
        field_cn_name = field.get('field_cn_name', '')
        field_desc = field.get('description', '')
        field_str = f"  - {field_name} ({field_cn_name}): {field_desc}"
        parts.append(field_str)

    return "\n".join(parts)


def test_search():
    """Test vector search with sample queries."""
    # Load model and connect to Qdrant
    print("Loading embedding model...")
    model = SentenceTransformer('BAAI/bge-small-zh')

    print("Connecting to Qdrant...")
    client = QdrantClient(host="localhost", port=6333)
    collection_name = "schema_embeddings"

    # Check collection
    collections = client.get_collections().collections
    exists = any(c.name == collection_name for c in collections)
    if not exists:
        print(f"Error: Collection '{collection_name}' not found")
        return

    # Load schema for display
    schema = load_schema()
    tables_by_name = {t['table_name']: t for t in schema['tables']}

    # Test queries
    test_queries = [
        ("指标查询-销售额", "统计上个月的销售额和订单量"),
        ("指标查询-GMV", "销售额GMV订单量"),
        ("指标查询-省份", "查看各省份的销售业绩"),
        ("指标查询-类目", "统计每个类目的GMV"),
        ("环比分析", "销售额环比分析"),
        ("同比分析", "对比今年和去年的销售额"),
        ("明细查询", "查看订单明细"),
        ("用户相关", "用户画像分析"),
    ]

    print("\n" + "=" * 70)
    print("Qdrant Vector Search Test (top_k=10)")
    print("=" * 70)

    for test_name, query in test_queries:
        print(f"\n【{test_name}】查询: {query}")
        print("-" * 50)

        # Encode query
        query_embedding = model.encode(query, normalize_embeddings=True)

        # Search Qdrant
        from qdrant_client.models import SearchRequest
        search_request = SearchRequest(
            vector=query_embedding.tolist(),
            limit=10,
            with_payload=True,
        )
        response = client.http.search_api.search_points(
            collection_name=collection_name,
            search_request=search_request,
        )

        if not response.result:
            print("  未召回任何表")
            continue

        print(f"  召回 {len(response.result)} 个表:")
        for i, result in enumerate(response.result, 1):
            payload = result.payload
            table_name = payload['table_name']
            table_cn_name = payload['table_cn_name']
            score = result.score

            # Get keywords from payload or original schema
            keywords = payload.get('keywords', [])
            if not keywords and table_name in tables_by_name:
                keywords = tables_by_name[table_name].get('keywords', [])

            keywords_str = ""
            if keywords:
                keywords_str = f" [关键词: {', '.join(keywords[:3])}]"

            print(f"  {i}. {table_name}: {table_cn_name}")
            print(f"     相似度: {score:.4f}{keywords_str}")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    test_search()
