import sqlite3
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

@dataclass
class RunResult:
    site_name: str
    timestamp: str
    records_count: int
    status: str  # 'success', 'failed', 'partial'
    error_count: int
    last_error: Optional[str] = None

class RunHistoryStore:
    """
    Persistent store for run history using a lightweight SQLite database.
    """
    def __init__(self, db_path: str = "./output/cwsf_meta.db"):
        self.db_path = Path(db_path)
        self._ensure_db_exists()

    def _ensure_db_exists(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path))
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS run_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    site_name TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    records_count INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    error_count INTEGER NOT NULL,
                    last_error TEXT
                )
            """)
            conn.commit()
        finally:
            conn.close()

    def record_run(self, result: RunResult):
        conn = sqlite3.connect(str(self.db_path))
        try:
            conn.execute("""
                INSERT INTO run_history (site_name, timestamp, records_count, status, error_count, last_error)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                result.site_name,
                result.timestamp,
                result.records_count,
                result.status,
                result.error_count,
                result.last_error
            ))
            conn.commit()
        finally:
            conn.close()

    def get_last_runs(self) -> List[RunResult]:
        """Returns the latest run result for each site."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute("""
                SELECT h1.* FROM run_history h1
                JOIN (
                    SELECT site_name, MAX(timestamp) as max_ts
                    FROM run_history
                    GROUP BY site_name
                ) h2 ON h1.site_name = h2.site_name AND h1.timestamp = h2.max_ts
                ORDER BY h1.site_name ASC
            """)
            rows = cursor.fetchall()
            return [RunResult(
                site_name=row["site_name"],
                timestamp=row["timestamp"],
                records_count=row["records_count"],
                status=row["status"],
                error_count=row["error_count"],
                last_error=row["last_error"]
            ) for row in rows]
        finally:
            conn.close()

    def get_site_history(self, site_name: str, limit: int = 5) -> List[RunResult]:
        """Returns the history for a specific site."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute("""
                SELECT * FROM run_history
                WHERE site_name = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """, (site_name, limit))
            rows = cursor.fetchall()
            return [RunResult(
                site_name=row["site_name"],
                timestamp=row["timestamp"],
                records_count=row["records_count"],
                status=row["status"],
                error_count=row["error_count"],
                last_error=row["last_error"]
            ) for row in rows]
        finally:
            conn.close()
