"""Basic tests for NL2SQL tools."""
import pytest
import json

from src.tools.sql_executor import _validate_sql_safety as validate_sql


def test_validate_sql_safe():
    """Test SQL validation with safe query."""
    sql = "SELECT * FROM orders LIMIT 10"
    is_valid = validate_sql(sql)
    assert is_valid is True


def test_validate_sql_forbidden():
    """Test SQL validation blocks forbidden operations."""
    sql = "DELETE FROM orders"
    is_valid = validate_sql(sql)
    assert is_valid is False


def test_validate_sql_select_only():
    """Test SQL validation only allows SELECT statements."""
    sql = "SELECT * FROM orders"
    is_valid = validate_sql(sql)
    assert is_valid is True


def test_validate_sql_blocks_drop():
    """Test SQL validation blocks DROP."""
    sql = "DROP TABLE orders"
    is_valid = validate_sql(sql)
    assert is_valid is False


def test_validate_sql_blocks_update():
    """Test SQL validation blocks UPDATE."""
    sql = "UPDATE orders SET status = 'done'"
    is_valid = validate_sql(sql)
    assert is_valid is False


def test_validate_sql_blocks_insert():
    """Test SQL validation blocks INSERT."""
    sql = "INSERT INTO orders (id, status) VALUES (1, 'pending')"
    is_valid = validate_sql(sql)
    assert is_valid is False


def test_validate_sql_blocks_alter():
    """Test SQL validation blocks ALTER."""
    sql = "ALTER TABLE orders ADD COLUMN new_col VARCHAR(255)"
    is_valid = validate_sql(sql)
    assert is_valid is False


def test_validate_sql_blocks_create():
    """Test SQL validation blocks CREATE."""
    sql = "CREATE TABLE new_orders (id INT)"
    is_valid = validate_sql(sql)
    assert is_valid is False


def test_validate_sql_blocks_truncate():
    """Test SQL validation blocks TRUNCATE."""
    sql = "TRUNCATE TABLE orders"
    is_valid = validate_sql(sql)
    assert is_valid is False


def test_validate_sql_blocks_union():
    """Test SQL validation blocks UNION (potential injection risk)."""
    sql = "SELECT * FROM orders UNION SELECT * FROM users"
    is_valid = validate_sql(sql)
    assert is_valid is False


def test_validate_sql_empty():
    """Test SQL validation rejects empty SQL."""
    sql = ""
    is_valid = validate_sql(sql)
    assert is_valid is False


def test_validate_sql_none():
    """Test SQL validation rejects None."""
    sql = None
    is_valid = validate_sql(sql)
    assert is_valid is False
