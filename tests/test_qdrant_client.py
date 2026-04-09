"""Tests for QdrantSchemaClient."""
import pytest
from src.services.qdrant_client import QdrantSchemaClient
from src.models import TableMetadata, DatasourceType


def test_qdrant_client_init():
    """Test Qdrant client initialization."""
    client = QdrantSchemaClient()
    assert client.host == "localhost"
    assert client.port == 6333


def test_create_collection():
    """Test collection creation."""
    client = QdrantSchemaClient()
    client.create_collection(recreate=True)
    assert client.is_collection_ready() is False  # Empty but exists


def test_upsert_and_search():
    """Test upsert and search flow."""
    client = QdrantSchemaClient()
    client.create_collection(recreate=True)

    # Create test tables
    tables = [
        TableMetadata(
            table_name="users",
            table_cn_name="用户表",
            description="用户信息",
            datasource=DatasourceType.MYSQL,
            fields=[{"field_name": "id", "field_cn_name": "ID", "data_type": "INT"}],
        ),
        TableMetadata(
            table_name="orders",
            table_cn_name="订单表",
            description="订单信息",
            datasource=DatasourceType.MYSQL,
            fields=[{"field_name": "order_id", "field_cn_name": "订单ID", "data_type": "BIGINT"}],
        ),
    ]

    # Mock embeddings (512 dim)
    embeddings = [[0.1] * 512, [0.2] * 512]

    # Upsert
    client.upsert_tables(tables, embeddings)
    assert client.get_table_count() == 2

    # Search
    results = client.search([0.1] * 512, top_k=2)
    assert len(results) == 2
    assert results[0].table_name == "users"


def test_datasource_filter():
    """Test datasource filter."""
    client = QdrantSchemaClient()
    client.create_collection(recreate=True)

    tables = [
        TableMetadata(
            table_name="mysql_table",
            table_cn_name="MySQL表",
            description="",
            datasource=DatasourceType.MYSQL,
            fields=[],
        ),
        TableMetadata(
            table_name="doris_table",
            table_cn_name="Doris表",
            description="",
            datasource=DatasourceType.DORIS,
            fields=[],
        ),
    ]
    embeddings = [[0.1] * 512, [0.2] * 512]
    client.upsert_tables(tables, embeddings)

    # Search with filter
    results = client.search([0.1] * 512, top_k=10, datasource_filter="mysql")
    assert len(results) == 1
    assert results[0].table_name == "mysql_table"
