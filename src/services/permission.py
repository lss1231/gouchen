"""Permission service for role-based access control."""
import json
from pathlib import Path
from typing import List, Optional, Dict, Any


class PermissionService:
    """Service for managing role-based permissions."""

    def __init__(self, roles_path: Optional[Path] = None):
        """Initialize permission service with roles configuration.

        Args:
            roles_path: Path to roles.json file. If None, uses default path.
        """
        if roles_path is None:
            # Default to data/permissions/roles.json relative to project root
            roles_path = Path(__file__).parent.parent.parent / "data" / "permissions" / "roles.json"

        self._roles_path = roles_path
        self._roles: Dict[str, Dict[str, Any]] = {}
        self._load_roles()

    def _load_roles(self) -> None:
        """Load roles from JSON file."""
        if not self._roles_path.exists():
            self._roles = {}
            return

        with open(self._roles_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            self._roles = data.get("roles", {})

    def can_query_table(self, role_id: str, table_name: str) -> bool:
        """Check if role can query a specific table.

        Args:
            role_id: The role identifier
            table_name: The table name to check

        Returns:
            True if the role can query the table, False otherwise
        """
        role = self._roles.get(role_id)
        if role is None:
            return False

        allowed_tables = role.get("allowed_tables", [])

        # Check for wildcard
        if "*" in allowed_tables:
            return True

        return table_name in allowed_tables

    def filter_tables(self, role_id: str, tables: List[str]) -> List[str]:
        """Filter table list by role permissions.

        Args:
            role_id: The role identifier
            tables: List of table names to filter

        Returns:
            List of table names the role is allowed to access
        """
        role = self._roles.get(role_id)
        if role is None:
            return []

        allowed_tables = role.get("allowed_tables", [])

        # Check for wildcard - allow all tables
        if "*" in allowed_tables:
            return tables

        return [table for table in tables if table in allowed_tables]

    def get_allowed_tables(self, role_id: str) -> List[str]:
        """Get all allowed tables for a role.

        Args:
            role_id: The role identifier

        Returns:
            List of allowed table names. Returns empty list for unknown roles.
        """
        role = self._roles.get(role_id)
        if role is None:
            return []

        return role.get("allowed_tables", [])

    def get_denied_fields(self, role_id: str) -> List[str]:
        """Get all denied fields for a role.

        Args:
            role_id: The role identifier

        Returns:
            List of denied field names. Returns empty list for unknown roles.
        """
        role = self._roles.get(role_id)
        if role is None:
            return []

        return role.get("denied_fields", [])


# Singleton instance
_permission_service: Optional[PermissionService] = None


def get_permission_service(roles_path: Optional[Path] = None) -> PermissionService:
    """Get or create permission service singleton.

    Args:
        roles_path: Path to roles.json file. Only used on first call.

    Returns:
        PermissionService instance
    """
    global _permission_service
    if _permission_service is None:
        _permission_service = PermissionService(roles_path)
    return _permission_service
