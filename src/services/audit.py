"""Audit logging service for query tracking."""
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List


class AuditService:
    """Service for logging and querying audit records."""

    def __init__(self, log_dir: Optional[Path] = None):
        """Initialize audit service with log directory.

        Args:
            log_dir: Directory to store audit log files. If None, uses default path.
        """
        if log_dir is None:
            # Default to data/audit relative to project root
            log_dir = Path(__file__).parent.parent.parent / "data" / "audit"

        self._log_dir = log_dir
        # Create log directory if not exists
        self._log_dir.mkdir(parents=True, exist_ok=True)

    def log_query(
        self,
        query: str,
        user_role: str,
        intent: Optional[Dict[str, Any]] = None,
        generated_sql: Optional[str] = None,
        approval_decision: Optional[str] = None,
        execution_result: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        start_time: Optional[float] = None,
    ) -> str:
        """Log an audit entry for a query.

        Args:
            query: The natural language query
            user_role: The user's role identifier
            intent: Parsed intent information
            generated_sql: The generated SQL query
            approval_decision: Approval decision (approved/rejected)
            execution_result: SQL execution result
            error: Error message if any
            start_time: Start timestamp for duration calculation

        Returns:
            The log_id of the created audit entry
        """
        # Generate unique log_id
        log_id = str(uuid.uuid4())

        # Calculate duration if start_time provided
        duration_ms = None
        if start_time is not None:
            duration_ms = int((datetime.now().timestamp() - start_time) * 1000)

        # Build audit entry
        entry = {
            "log_id": log_id,
            "timestamp": datetime.now().isoformat(),
            "query": query,
            "user_role": user_role,
            "intent": intent,
            "generated_sql": generated_sql,
            "approval_decision": approval_decision,
            "execution_result": execution_result,
            "error": error,
            "duration_ms": duration_ms,
        }

        # Write to daily log file
        log_file = self._get_log_file()
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

        return log_id

    def _get_log_file(self) -> Path:
        """Get the log file path for today."""
        date_str = datetime.now().strftime("%Y-%m-%d")
        return self._log_dir / f"audit_{date_str}.jsonl"

    def query_logs(
        self,
        date_str: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Query historical audit logs.

        Args:
            date_str: Date string in YYYY-MM-DD format. If None, uses today.
            limit: Maximum number of logs to return

        Returns:
            List of audit log entries, most recent first
        """
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")

        log_file = self._log_dir / f"audit_{date_str}.jsonl"

        if not log_file.exists():
            return []

        logs = []
        with open(log_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        entry = json.loads(line)
                        logs.append(entry)
                    except json.JSONDecodeError:
                        # Skip malformed lines
                        continue

        # Return most recent first, limited by limit
        return logs[-limit:][::-1]


# Singleton instance
_audit_service: Optional[AuditService] = None


def get_audit_service(log_dir: Optional[Path] = None) -> AuditService:
    """Get or create audit service singleton.

    Args:
        log_dir: Directory to store audit log files. Only used on first call.

    Returns:
        AuditService instance
    """
    global _audit_service
    if _audit_service is None:
        _audit_service = AuditService(log_dir)
    return _audit_service
