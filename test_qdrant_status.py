"""Test script to check Qdrant availability and schema retrieval path."""

from pathlib import Path

from src.services.schema_store import get_schema_store, load_tables_from_json
from src.services.schema_embedding import SchemaEmbeddingService


def test_qdrant_connection():
    """Check if Qdrant server is reachable."""
    print("=== Qdrant Connection Test ===")
    try:
        from qdrant_client import QdrantClient
        from src.config import get_settings

        settings = get_settings()
        client = QdrantClient(host=settings.qdrant_host, port=settings.qdrant_port)
        collections = client.get_collections()
        print(f"  Qdrant reachable at {settings.qdrant_host}:{settings.qdrant_port}")
        print(f"  Collections: {[c.name for c in collections.collections]}")
        return True
    except Exception as e:
        print(f"  Qdrant NOT reachable: {e}")
        return False


def test_embedding_service():
    """Check if embedding service can build index and search."""
    print("\n=== Embedding Service Test ===")
    schema_path = Path("data/schema/doris_schema_enhanced.json")
    if not schema_path.exists():
        print(f"  Schema file not found: {schema_path}")
        return False

    tables = load_tables_from_json(schema_path)
    print(f"  Loaded {len(tables)} tables from JSON")

    service = SchemaEmbeddingService()
    try:
        service.build_index(tables)
        print(f"  Vector index built successfully (indexed={service.is_indexed()})")
    except Exception as e:
        print(f"  Vector index build FAILED: {e}")
        return False

    try:
        results = service.search("上个月销售额", top_k=3)
        print(f"  Vector search returned {len(results)} results")
        for r in results:
            print(f"    - {r.table_name} ({r.table_cn_name})")
        return True
    except Exception as e:
        print(f"  Vector search FAILED: {e}")
        return False


def test_schema_store_retrieval_path():
    """Check which path SchemaStore actually uses."""
    print("\n=== SchemaStore Retrieval Path Test ===")
    # schema_path = Path("data/schema/doris_schema_enhanced.json")
    # tables = load_tables_from_json(schema_path)
    #
    store = get_schema_store()
    # store.index_tables(tables)
    #
    # print(f"  use_vector_search flag = {store._use_vector_search}")

    query = "上个月各省份销售额"
    try:
        results = store.search(query, top_k=3)
        print(f"  Search returned {len(results)} tables for query: '{query}'")
        for r in results:
            print(f"    - {r.table_name} ({r.table_cn_name})")

        # Determine which path was actually used by inspecting embedding service state
        if store._use_vector_search:
            try:
                service = store._get_embedding_service()
                if service.is_indexed():
                    print("  -> Retrieval path: VECTOR SEARCH (Qdrant)")
                else:
                    print("  -> Retrieval path: KEYWORD MATCH (vector index not ready)")
            except Exception as e:
                print(f"  -> Retrieval path: KEYWORD MATCH (embedding service error: {e})")
        else:
            print("  -> Retrieval path: KEYWORD MATCH (vector search disabled)")

        return True
    except Exception as e:
        print(f"  Search FAILED: {e}")
        return False


if __name__ == "__main__":
    qdrant_ok = test_qdrant_connection()
    embedding_ok = test_embedding_service()
    retrieval_ok = test_schema_store_retrieval_path()

    print("\n=== Summary ===")
    print(f"  Qdrant reachable: {qdrant_ok}")
    print(f"  Embedding service works: {embedding_ok}")
    print(f"  Schema retrieval works: {retrieval_ok}")
