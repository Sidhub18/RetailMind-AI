"""Exact, cross-chunk duplicate detection and removal."""

import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from pathlib import Path
from tempfile import TemporaryDirectory

import orjson
import pandas as pd
from etl.ingestion.schema_validator import TableSchema


class DuplicateStrategy(StrEnum):
    """Supported responses to duplicate primary keys."""

    DROP = "drop"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class DuplicateResult:
    """Result of applying primary-key deduplication to one chunk."""

    frame: pd.DataFrame
    duplicate_rows: int


class DuplicateError(ValueError):
    """Raised when duplicate keys are forbidden by policy."""


class SQLiteDuplicateKeyStore:
    """Disk-backed exact key index used across ingestion chunks."""

    def __init__(self, database_path: Path | None = None) -> None:
        """Create an exact temporary or explicitly located key index."""
        self._temporary_directory: TemporaryDirectory[str] | None = None
        if database_path is None:
            self._temporary_directory = TemporaryDirectory(prefix="retailmind-dedup-")
            database_path = (
                Path(self._temporary_directory.name) / "duplicate-keys.sqlite3"
            )
        else:
            database_path.parent.mkdir(parents=True, exist_ok=True)

        self._connection = sqlite3.connect(database_path)
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=NORMAL")
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS seen_keys (
                table_name TEXT NOT NULL,
                key_value TEXT NOT NULL,
                PRIMARY KEY (table_name, key_value)
            ) WITHOUT ROWID
            """
        )
        self._connection.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS incoming_keys (
                key_value TEXT PRIMARY KEY
            ) WITHOUT ROWID
            """
        )

    def reset(self, table_name: str) -> None:
        """Remove keys retained for a prior processing attempt."""
        with self._connection:
            self._connection.execute(
                "DELETE FROM seen_keys WHERE table_name = ?",
                (table_name,),
            )

    def check_and_add(
        self,
        table_name: str,
        unique_keys: Sequence[str],
    ) -> frozenset[str]:
        """Return previously seen keys and atomically retain new keys."""
        if not unique_keys:
            return frozenset()

        with self._connection:
            self._connection.execute("DELETE FROM incoming_keys")
            self._connection.executemany(
                "INSERT INTO incoming_keys (key_value) VALUES (?)",
                ((key,) for key in unique_keys),
            )
            existing = frozenset(
                row[0]
                for row in self._connection.execute(
                    """
                    SELECT incoming.key_value
                    FROM incoming_keys AS incoming
                    INNER JOIN seen_keys AS seen
                        ON seen.key_value = incoming.key_value
                    WHERE seen.table_name = ?
                    """,
                    (table_name,),
                )
            )
            self._connection.execute(
                """
                INSERT OR IGNORE INTO seen_keys (table_name, key_value)
                SELECT ?, key_value FROM incoming_keys
                """,
                (table_name,),
            )
        return existing

    def close(self) -> None:
        """Close the database and remove temporary storage when applicable."""
        self._connection.close()
        if self._temporary_directory is not None:
            self._temporary_directory.cleanup()
            self._temporary_directory = None


class DuplicateHandler:
    """Remove or reject exact primary-key duplicates across all chunks."""

    def __init__(
        self,
        key_store: SQLiteDuplicateKeyStore,
        strategy: DuplicateStrategy = DuplicateStrategy.DROP,
    ) -> None:
        """Initialize the handler with an exact key store and policy."""
        self._key_store = key_store
        self._strategy = strategy

    def reset(self, table_name: str) -> None:
        """Reset duplicate state before starting a table."""
        self._key_store.reset(table_name)

    def handle(
        self,
        frame: pd.DataFrame,
        schema: TableSchema,
    ) -> DuplicateResult:
        """Apply exact deduplication using the table's logical primary key."""
        serialized_keys = [
            self._serialize_key(values)
            for values in frame.loc[:, list(schema.primary_key)].itertuples(
                index=False,
                name=None,
            )
        ]
        key_series = pd.Series(serialized_keys, index=frame.index, dtype="string")
        within_chunk = key_series.duplicated(keep="first")
        unique_keys = key_series.loc[~within_chunk].tolist()
        previously_seen = self._key_store.check_and_add(
            schema.name,
            unique_keys,
        )
        cross_chunk = key_series.isin(previously_seen)
        duplicate_mask = within_chunk | cross_chunk
        duplicate_rows = int(duplicate_mask.sum())

        if duplicate_rows and self._strategy is DuplicateStrategy.ERROR:
            message = f"Duplicate primary keys in {schema.name}: count={duplicate_rows}"
            raise DuplicateError(message)

        if duplicate_rows == 0:
            return DuplicateResult(frame=frame, duplicate_rows=0)
        return DuplicateResult(
            frame=frame.loc[~duplicate_mask].copy(),
            duplicate_rows=duplicate_rows,
        )

    def close(self) -> None:
        """Release the underlying exact key store."""
        self._key_store.close()

    @staticmethod
    def _serialize_key(values: tuple[object, ...]) -> str:
        normalized = [DuplicateHandler._normalize_key_value(value) for value in values]
        return orjson.dumps(normalized).decode("utf-8")

    @staticmethod
    def _normalize_key_value(value: object) -> str | int | float | bool | None:
        if value is None or value is pd.NA:
            return None
        if isinstance(value, pd.Timestamp):
            return str(value.isoformat())
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, (str, int, float, bool)):
            return value
        return str(value)
