#!/usr/bin/env python
"""Index schema metadata to Chroma vector store."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.services.schema_store import SchemaStore, load_tables_from_json

def main():
    print("Loading schema metadata...")
    schema_path = Path("data/schema/ecommerce_schema.json")
    tables = load_tables_from_json(schema_path)
    print(f"Loaded {len(tables)} tables")

    print("Indexing to Chroma...")
    store = SchemaStore()
    store.index_tables(tables)

    print("Testing retrieval...")
    test_queries = [
        "上个月销售额",
        "各品类订单量",
        "华东地区销售情况",
    ]

    for query in test_queries:
        results = store.retrieve(query, top_k=2)
        print(f"\nQuery: '{query}'")
        for r in results:
            print(f"  -> {r.table_cn_name} ({r.table_name})")

    print("\nIndexing complete!")

if __name__ == "__main__":
    main()
