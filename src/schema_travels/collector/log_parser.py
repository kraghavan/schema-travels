"""Log parser module for extracting queries from database log files."""

import re
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Iterator

from schema_travels.collector.models import QueryLog


class LogParser(ABC):
    """Abstract base class for database log parsers."""

    def __init__(self, logs_dir: Path | str):
        """Initialize parser with logs directory."""
        self.logs_dir = Path(logs_dir)
        if not self.logs_dir.exists():
            raise FileNotFoundError(f"Logs directory not found: {self.logs_dir}")

    @abstractmethod
    def parse(self) -> list[QueryLog]:
        """Parse all log files and return query logs."""
        pass

    @abstractmethod
    def parse_file(self, file_path: Path) -> Iterator[QueryLog]:
        """Parse a single log file."""
        pass

    def get_log_files(self) -> list[Path]:
        """Get all log files in the directory."""
        patterns = self._get_file_patterns()
        files = []
        for pattern in patterns:
            files.extend(self.logs_dir.glob(pattern))
        return sorted(files)

    @abstractmethod
    def _get_file_patterns(self) -> list[str]:
        """Get glob patterns for log files."""
        pass


class PostgresLogParser(LogParser):
    """Parser for PostgreSQL query logs."""

    # PostgreSQL log line patterns
    # Format: 2024-01-15 10:30:45.123 UTC [12345] user@database LOG:  statement: SELECT ...
    LOG_LINE_PATTERN = re.compile(
        r"^(?P<timestamp>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}[\.\d]*)\s+"
        r"(?P<timezone>\w+)?\s*"
        r"\[(?P<pid>\d+)\]\s+"
        r"(?:(?P<user>\w+)@(?P<database>\w+)\s+)?"
        r"(?P<level>\w+):\s+"
        r"(?P<message>.*)"
    )

    # Duration pattern: duration: 123.456 ms
    DURATION_PATTERN = re.compile(r"duration:\s+(?P<duration>[\d.]+)\s+ms")

    # Statement pattern
    STATEMENT_PATTERN = re.compile(r"(?:statement|execute\s+\w+):\s+(?P<sql>.*)", re.IGNORECASE)

    def _get_file_patterns(self) -> list[str]:
        """PostgreSQL log file patterns."""
        return ["*.log", "postgresql-*.log", "postgresql*.log"]

    def parse(self) -> list[QueryLog]:
        """Parse all PostgreSQL log files."""
        queries = []
        for log_file in self.get_log_files():
            queries.extend(self.parse_file(log_file))
        return queries

    def parse_file(self, file_path: Path) -> Iterator[QueryLog]:
        """Parse a single PostgreSQL log file."""
        current_entry: dict = {}
        continuation_buffer: list[str] = []

        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.rstrip()

                # Try to match a new log entry
                match = self.LOG_LINE_PATTERN.match(line)

                if match:
                    # Process previous entry if exists
                    if current_entry and continuation_buffer:
                        query = self._build_query_log(current_entry, continuation_buffer)
                        if query:
                            yield query

                    # Start new entry
                    current_entry = match.groupdict()
                    continuation_buffer = [current_entry.get("message", "")]
                elif current_entry:
                    # Continuation of previous entry
                    continuation_buffer.append(line)

            # Process last entry
            if current_entry and continuation_buffer:
                query = self._build_query_log(current_entry, continuation_buffer)
                if query:
                    yield query

    def _build_query_log(self, entry: dict, message_lines: list[str]) -> QueryLog | None:
        """Build QueryLog from parsed entry."""
        full_message = " ".join(line.strip() for line in message_lines if line.strip())

        # Check for statement
        stmt_match = self.STATEMENT_PATTERN.search(full_message)
        if not stmt_match:
            return None

        sql = stmt_match.group("sql").strip()
        if not sql:
            return None

        # Parse timestamp
        timestamp = None
        if entry.get("timestamp"):
            try:
                # Handle various PostgreSQL timestamp formats
                ts_str = entry["timestamp"]
                for fmt in ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]:
                    try:
                        timestamp = datetime.strptime(ts_str.split()[0] + " " + ts_str.split()[1][:15], fmt)
                        break
                    except ValueError:
                        continue
            except (ValueError, IndexError):
                pass

        # Parse duration
        duration_ms = None
        dur_match = self.DURATION_PATTERN.search(full_message)
        if dur_match:
            try:
                duration_ms = float(dur_match.group("duration"))
            except ValueError:
                pass

        return QueryLog(
            sql=sql,
            timestamp=timestamp,
            duration_ms=duration_ms,
            user=entry.get("user"),
            database=entry.get("database"),
        )


class MySQLLogParser(LogParser):
    """Parser for MySQL slow query logs and general logs."""

    # MySQL slow query log patterns
    # # Time: 2024-01-15T10:30:45.123456Z
    # # User@Host: user[user] @ localhost []  Id:    12
    # # Query_time: 0.000123  Lock_time: 0.000000 Rows_sent: 1  Rows_examined: 1
    # SET timestamp=1705314645;
    # SELECT ...

    TIME_PATTERN = re.compile(r"^#\s*Time:\s*(?P<timestamp>[\dT:.Z-]+)")
    USER_PATTERN = re.compile(r"^#\s*User@Host:\s*(?P<user>\w+)\[.*?\]\s*@\s*(?P<host>\S+)")
    QUERY_TIME_PATTERN = re.compile(
        r"^#\s*Query_time:\s*(?P<query_time>[\d.]+)\s+"
        r"Lock_time:\s*(?P<lock_time>[\d.]+)\s+"
        r"Rows_sent:\s*(?P<rows_sent>\d+)\s+"
        r"Rows_examined:\s*(?P<rows_examined>\d+)"
    )
    SET_TIMESTAMP_PATTERN = re.compile(r"^SET\s+timestamp=\d+;", re.IGNORECASE)

    def _get_file_patterns(self) -> list[str]:
        """MySQL log file patterns."""
        return ["*.log", "slow*.log", "mysql-slow*.log", "general*.log"]

    def parse(self) -> list[QueryLog]:
        """Parse all MySQL log files."""
        queries = []
        for log_file in self.get_log_files():
            queries.extend(self.parse_file(log_file))
        return queries

    def parse_file(self, file_path: Path) -> Iterator[QueryLog]:
        """Parse a single MySQL log file."""
        current_entry: dict = {}
        sql_buffer: list[str] = []

        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.rstrip()

                # Check for time marker (new entry)
                time_match = self.TIME_PATTERN.match(line)
                if time_match:
                    # Process previous entry
                    if current_entry and sql_buffer:
                        query = self._build_query_log(current_entry, sql_buffer)
                        if query:
                            yield query

                    current_entry = {"timestamp": time_match.group("timestamp")}
                    sql_buffer = []
                    continue

                # Check for user info
                user_match = self.USER_PATTERN.match(line)
                if user_match:
                    current_entry["user"] = user_match.group("user")
                    current_entry["host"] = user_match.group("host")
                    continue

                # Check for query time info
                qt_match = self.QUERY_TIME_PATTERN.match(line)
                if qt_match:
                    current_entry["query_time"] = qt_match.group("query_time")
                    current_entry["rows_sent"] = qt_match.group("rows_sent")
                    current_entry["rows_examined"] = qt_match.group("rows_examined")
                    continue

                # Skip SET timestamp lines
                if self.SET_TIMESTAMP_PATTERN.match(line):
                    continue

                # Skip comment lines
                if line.startswith("#"):
                    continue

                # Accumulate SQL
                if line:
                    sql_buffer.append(line)

            # Process last entry
            if current_entry and sql_buffer:
                query = self._build_query_log(current_entry, sql_buffer)
                if query:
                    yield query

    def _build_query_log(self, entry: dict, sql_lines: list[str]) -> QueryLog | None:
        """Build QueryLog from parsed entry."""
        sql = " ".join(sql_lines).strip()

        # Remove trailing semicolon for consistency
        sql = sql.rstrip(";")

        if not sql:
            return None

        # Skip administrative commands
        if sql.upper() in ("COMMIT", "BEGIN", "ROLLBACK", "START TRANSACTION"):
            return None

        # Parse timestamp
        timestamp = None
        if entry.get("timestamp"):
            try:
                ts_str = entry["timestamp"].replace("T", " ").replace("Z", "")
                timestamp = datetime.fromisoformat(ts_str)
            except ValueError:
                pass

        # Parse duration (MySQL reports in seconds)
        duration_ms = None
        if entry.get("query_time"):
            try:
                duration_ms = float(entry["query_time"]) * 1000
            except ValueError:
                pass

        # Parse rows affected
        rows_affected = None
        if entry.get("rows_sent"):
            try:
                rows_affected = int(entry["rows_sent"])
            except ValueError:
                pass

        return QueryLog(
            sql=sql,
            timestamp=timestamp,
            duration_ms=duration_ms,
            rows_affected=rows_affected,
            user=entry.get("user"),
        )


def get_parser(db_type: str, logs_dir: Path | str) -> LogParser:
    """Factory function to get appropriate log parser."""
    parsers = {
        "postgres": PostgresLogParser,
        "postgresql": PostgresLogParser,
        "mysql": MySQLLogParser,
    }

    parser_class = parsers.get(db_type.lower())
    if not parser_class:
        raise ValueError(f"Unsupported database type: {db_type}. Supported: {list(parsers.keys())}")

    return parser_class(logs_dir)
