import sqlite3
import re
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from cwsf.output.base import BaseWriter


class WriterClosedError(Exception):
    """Raised when attempting to write to a closed writer."""
    pass


class SqliteWriter(BaseWriter):
    """
    SQLite output writer for CWSF.
    
    Automatically creates the database and table based on configuration.
    Supports schema evolution by adding new columns as they appear in the config.
    """

    def __init__(self):
        self.conn: Optional[sqlite3.Connection] = None
        self.table_name: Optional[str] = None
        self.db_path: Optional[Path] = None
        self.mode: str = "append"
        self._closed = False

    def _sanitize_table_name(self, name: str) -> str:
        """
        Sanitizes the site name to be a valid SQLite identifier.
        
        Args:
            name: The site name to sanitize.
            
        Returns:
            str: The sanitized table name.
            
        Raises:
            ValueError: If the name cannot be sanitized.
        """
        # Allow only alphanumeric and underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        # Remove leading underscores if they were created by sanitization and the original didn't have them,
        # but SQLite allows starting with underscore.
        # The real issue is if it's empty or only underscores when it shouldn't be.
        # Let's be stricter: must contain at least one alphanumeric character.
        if not re.search(r'[a-zA-Z0-9]', sanitized):
            raise ValueError(f"Invalid site name for SQLite table: {name}")
        return sanitized

    def open(self, config: Dict[str, Any]) -> None:
        """
        Initialize the SQLite writer, creating the database and table if needed.
        
        Args:
            config: The site configuration dictionary.
        """
        site_name = config.get("site_name")
        if not site_name:
            raise ValueError("site_name is required in config")

        self.table_name = self._sanitize_table_name(site_name)
        
        output_config = config.get("output", {})
        destination = output_config.get("destination", "./output/")
        self.mode = output_config.get("mode", "append")
        
        dest_path = Path(destination)
        if dest_path.suffix == ".db":
            self.db_path = dest_path
        else:
            self.db_path = dest_path / f"{site_name}.db"

        # Ensure directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._closed = False

        self._create_or_update_table(config)

    def _create_or_update_table(self, config: Dict[str, Any]) -> None:
        """
        Creates the table if it doesn't exist, or adds missing columns.
        """
        if not self.conn or not self.table_name:
            return

        fields = config.get("selectors", {}).get("fields", {})
        field_names = list(fields.keys())

        # Base columns
        columns = [
            "id INTEGER PRIMARY KEY AUTOINCREMENT",
            "site_name TEXT NOT NULL",
            "source_url TEXT NOT NULL",
            "scrape_timestamp TEXT NOT NULL"
        ]

        # Create table if not exists
        create_stmt = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({', '.join(columns)})"
        self.conn.execute(create_stmt)

        # Schema evolution: add missing columns
        cursor = self.conn.execute(f"PRAGMA table_info({self.table_name})")
        existing_columns = {row["name"] for row in cursor.fetchall()}

        for field in field_names:
            if field not in existing_columns:
                # SQLite ALTER TABLE ADD COLUMN only supports one column at a time
                self.conn.execute(f"ALTER TABLE {self.table_name} ADD COLUMN {field} TEXT")
        
        self.conn.commit()

    def write_records(self, records: List[Dict[str, Any]]) -> int:
        """
        Write a batch of scraped records into the site's table within a single transaction.
        
        Args:
            records: A list of dictionaries containing the scraped data.
            
        Returns:
            int: The count of records successfully written.
            
        Raises:
            WriterClosedError: If the writer is closed.
            sqlite3.Error: If a database error occurs during insertion.
        """
        if self._closed or not self.conn or not self.table_name:
            raise WriterClosedError("Cannot write to a closed SqliteWriter")

        if not records:
            return 0

        # Handle overwrite mode
        if self.mode == "overwrite":
            try:
                with self.conn:
                    self.conn.execute(f"DELETE FROM {self.table_name} WHERE site_name = ?", (records[0]["site_name"],))
            except sqlite3.Error:
                raise

        # Get current table columns to handle missing/extra fields
        cursor = self.conn.execute(f"PRAGMA table_info({self.table_name})")
        table_columns = [row["name"] for row in cursor.fetchall()]
        # Remove 'id' as it's autoincrement
        if "id" in table_columns:
            table_columns.remove("id")

        # Prepare the INSERT statement
        placeholders = ", ".join(["?" for _ in table_columns])
        columns_str = ", ".join(table_columns)
        insert_stmt = f"INSERT INTO {self.table_name} ({columns_str}) VALUES ({placeholders})"

        # Prepare data for executemany
        data_to_insert = []
        for record in records:
            row = []
            for col in table_columns:
                # Get value from record, default to None (NULL in SQLite)
                # Fields in record but not in table_columns are implicitly ignored
                row.append(record.get(col))
            data_to_insert.append(tuple(row))

        try:
            # Use a transaction for atomicity
            with self.conn:
                self.conn.executemany(insert_stmt, data_to_insert)
            return len(records)
        except sqlite3.Error:
            # Transaction is automatically rolled back by the 'with self.conn' context manager on exception
            raise

    def close(self) -> None:
        """
        Commits pending transactions and closes the database connection.
        """
        if self.conn:
            try:
                self.conn.commit()
            except sqlite3.ProgrammingError:
                # Connection might already be closed
                pass
            self.conn.close()
            self.conn = None
        self._closed = True
