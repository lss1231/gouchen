"""Tests for permission service."""
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from services.permission import PermissionService, get_permission_service


def test_admin_can_access_any_table():
    """Test that admin role can access any table."""
    service = PermissionService()

    # Admin should be able to access any table
    assert service.can_query_table("admin", "fact_order") is True
    assert service.can_query_table("admin", "dim_user") is True
    assert service.can_query_table("admin", "any_table") is True
    assert service.can_query_table("admin", "sensitive_data") is True

    # Admin allowed tables should be wildcard
    allowed = service.get_allowed_tables("admin")
    assert "*" in allowed

    # Admin should have no denied fields per config
    denied = service.get_denied_fields("admin")
    assert denied == []

    print("[PASS] Admin can access any table")


def test_sales_limited_table_access():
    """Test that sales role can only access allowed tables."""
    service = PermissionService()

    # Sales should be able to access allowed tables
    assert service.can_query_table("sales", "fact_order") is True
    assert service.can_query_table("sales", "dim_region") is True
    assert service.can_query_table("sales", "dim_product") is True

    # Sales should NOT be able to access other tables
    assert service.can_query_table("sales", "dim_user") is False
    assert service.can_query_table("sales", "fact_inventory") is False
    assert service.can_query_table("sales", "any_other_table") is False

    # Check allowed tables list
    allowed = service.get_allowed_tables("sales")
    assert allowed == ["fact_order", "dim_region", "dim_product"]

    # Check denied fields
    denied = service.get_denied_fields("sales")
    assert "cost_price" in denied
    assert "profit_margin" in denied

    print("[PASS] Sales can only access allowed tables")


def test_analyst_can_access_all_tables_with_field_restrictions():
    """Test that analyst role can access all tables but has field restrictions."""
    service = PermissionService()

    # Analyst should be able to access any table
    assert service.can_query_table("analyst", "fact_order") is True
    assert service.can_query_table("analyst", "dim_user") is True
    assert service.can_query_table("analyst", "any_table") is True

    # Analyst allowed tables should be wildcard
    allowed = service.get_allowed_tables("analyst")
    assert "*" in allowed

    # Check denied fields
    denied = service.get_denied_fields("analyst")
    assert "user_phone" in denied
    assert "user_email" in denied
    assert "id_card" in denied

    print("[PASS] Analyst can access all tables with field restrictions")


def test_filter_tables():
    """Test filtering tables by role permissions."""
    service = PermissionService()

    all_tables = ["fact_order", "dim_region", "dim_product", "dim_user", "fact_inventory"]

    # Admin should get all tables
    admin_tables = service.filter_tables("admin", all_tables)
    assert set(admin_tables) == set(all_tables)

    # Sales should only get allowed tables
    sales_tables = service.filter_tables("sales", all_tables)
    assert sales_tables == ["fact_order", "dim_region", "dim_product"]

    # Analyst should get all tables
    analyst_tables = service.filter_tables("analyst", all_tables)
    assert set(analyst_tables) == set(all_tables)

    print("[PASS] Filter tables works correctly")


def test_unknown_role_returns_false():
    """Test that unknown role returns False for all permissions."""
    service = PermissionService()

    # Unknown role should not be able to query any table
    assert service.can_query_table("unknown_role", "fact_order") is False
    assert service.can_query_table("unknown_role", "any_table") is False

    # Unknown role should have empty allowed tables
    allowed = service.get_allowed_tables("unknown_role")
    assert allowed == []

    # Unknown role should have empty denied fields
    denied = service.get_denied_fields("unknown_role")
    assert denied == []

    # Unknown role should get empty list when filtering
    tables = ["fact_order", "dim_user"]
    filtered = service.filter_tables("unknown_role", tables)
    assert filtered == []

    print("[PASS] Unknown role returns False/empty")


def test_singleton():
    """Test that get_permission_service returns a singleton."""
    service1 = get_permission_service()
    service2 = get_permission_service()

    assert service1 is service2

    print("[PASS] Singleton works correctly")


if __name__ == "__main__":
    test_admin_can_access_any_table()
    test_sales_limited_table_access()
    test_analyst_can_access_all_tables_with_field_restrictions()
    test_filter_tables()
    test_unknown_role_returns_false()
    test_singleton()

    print("\n[ALL PASSED] All permission tests passed!")
