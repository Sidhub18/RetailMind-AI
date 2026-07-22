"""Incremental checkpoints and raw-layer persistence abstractions."""

import re
import shutil
from collections.abc import Callable
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Protocol
from uuid import uuid4

import orjson
import pandas as pd
from config.settings import RetrySettings
from etl.ingestion.csv_reader import SourceMetadata
from etl.ingestion.schema_validator import TableSchema
from pandas.api.types import is_datetime64_any_dtype
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)


class LoadMode(StrEnum):
    """Supported source ingestion modes."""

    FULL = "full"
    INCREMENTAL = "incremental"


@dataclass(frozen=True, slots=True)
class LoadCheckpoint:
    """Durable state committed after a successful table ingestion."""

    table_name: str
    source_path: str
    source_size_bytes: int
    source_modified_time_ns: int
    watermark_value: str | None
    completed_at_utc: str


@dataclass(frozen=True, slots=True)
class IncrementalDecision:
    """Decision describing whether and from where a table should load."""

    should_process: bool
    reason: str
    lower_watermark: str | None


@dataclass(slots=True)
class RawWriteSession:
    """Transport-neutral state for one atomic raw-layer write."""

    table_name: str
    run_id: str
    staging_location: str
    final_location: str
    next_part_number: int = 0
    written_rows: int = 0


class IncrementalStateError(OSError):
    """Raised when checkpoint state cannot be read or committed."""


class RawDataSink(Protocol):
    """Port implemented by local and future Amazon S3 raw-layer sinks."""

    def begin(self, table_name: str, run_id: str) -> RawWriteSession:
        """Begin an isolated table write transaction."""
        ...

    def write(self, session: RawWriteSession, frame: pd.DataFrame) -> None:
        """Persist one validated chunk within the transaction."""
        ...

    def commit(self, session: RawWriteSession) -> str:
        """Publish all staged parts atomically and return their location."""
        ...

    def abort(self, session: RawWriteSession) -> None:
        """Remove staged output for a failed transaction."""
        ...


class JsonCheckpointStore:
    """Atomic local JSON checkpoint store for development ingestion."""

    _SAFE_TABLE_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")

    def __init__(self, root: Path) -> None:
        """Initialize the configured checkpoint root."""
        self._root = root.expanduser().resolve(strict=False)

    def load(self, table_name: str) -> LoadCheckpoint | None:
        """Load the most recently committed checkpoint for a table."""
        path = self._checkpoint_path(table_name)
        if not path.is_file():
            return None
        try:
            payload = orjson.loads(path.read_bytes())
            return LoadCheckpoint(
                table_name=str(payload["table_name"]),
                source_path=str(payload["source_path"]),
                source_size_bytes=int(payload["source_size_bytes"]),
                source_modified_time_ns=int(payload["source_modified_time_ns"]),
                watermark_value=(
                    None
                    if payload["watermark_value"] is None
                    else str(payload["watermark_value"])
                ),
                completed_at_utc=str(payload["completed_at_utc"]),
            )
        except (KeyError, TypeError, ValueError, orjson.JSONDecodeError) as error:
            message = f"Invalid checkpoint state: {path}"
            raise IncrementalStateError(message) from error

    def save(self, checkpoint: LoadCheckpoint) -> None:
        """Atomically commit a successful table checkpoint."""
        path = self._checkpoint_path(checkpoint.table_name)
        temporary_path = path.with_suffix(".json.tmp")
        try:
            self._root.mkdir(parents=True, exist_ok=True)
            temporary_path.write_bytes(
                orjson.dumps(asdict(checkpoint), option=orjson.OPT_INDENT_2)
            )
            temporary_path.replace(path)
        except OSError as error:
            message = f"Unable to commit checkpoint: {path}"
            raise IncrementalStateError(message) from error

    def _checkpoint_path(self, table_name: str) -> Path:
        if self._SAFE_TABLE_NAME.fullmatch(table_name) is None:
            message = f"Unsafe checkpoint table name: {table_name}"
            raise IncrementalStateError(message)
        return self._root / f"{table_name}.json"


class IncrementalLoader:
    """Apply high-watermark loading and commit state after successful writes."""

    def __init__(self, checkpoint_store: JsonCheckpointStore) -> None:
        """Initialize the loader with a durable state-store adapter."""
        self._checkpoint_store = checkpoint_store

    def decide(
        self,
        schema: TableSchema,
        metadata: SourceMetadata,
        mode: LoadMode,
    ) -> IncrementalDecision:
        """Determine whether a source changed and which rows are new."""
        checkpoint = self._checkpoint_store.load(schema.name)
        if checkpoint is None:
            return IncrementalDecision(
                should_process=True,
                reason="no_checkpoint",
                lower_watermark=None,
            )
        if mode is LoadMode.FULL:
            return IncrementalDecision(
                should_process=True,
                reason="full_reload",
                lower_watermark=None,
            )
        unchanged = (
            checkpoint.source_path == str(metadata.path)
            and checkpoint.source_size_bytes == metadata.size_bytes
            and checkpoint.source_modified_time_ns == metadata.modified_time_ns
        )
        if unchanged:
            return IncrementalDecision(
                should_process=False,
                reason="source_unchanged",
                lower_watermark=None,
            )
        if schema.watermark_column is None:
            return IncrementalDecision(
                should_process=True,
                reason="changed_snapshot",
                lower_watermark=None,
            )
        return IncrementalDecision(
            should_process=True,
            reason="watermark_increment",
            lower_watermark=checkpoint.watermark_value,
        )

    def filter_new_rows(
        self,
        frame: pd.DataFrame,
        schema: TableSchema,
        decision: IncrementalDecision,
    ) -> pd.DataFrame:
        """Return rows strictly newer than the committed watermark."""
        watermark_column = schema.watermark_column
        if watermark_column is None or decision.lower_watermark is None:
            return frame

        series = frame[watermark_column]
        if is_datetime64_any_dtype(series.dtype):
            lower_bound: object = pd.Timestamp(decision.lower_watermark)
        else:
            lower_bound = decision.lower_watermark
        return frame.loc[series.gt(lower_bound)].copy()

    def maximum_watermark(
        self,
        frame: pd.DataFrame,
        schema: TableSchema,
    ) -> str | None:
        """Return the normalized maximum watermark in a nonempty chunk."""
        watermark_column = schema.watermark_column
        if watermark_column is None or frame.empty:
            return None
        maximum = frame[watermark_column].max()
        if isinstance(maximum, pd.Timestamp):
            return str(maximum.strftime("%Y-%m-%d"))
        return str(maximum)

    def commit(
        self,
        schema: TableSchema,
        metadata: SourceMetadata,
        watermark_value: str | None,
    ) -> None:
        """Commit source identity and watermark after successful publication."""
        self._checkpoint_store.save(
            LoadCheckpoint(
                table_name=schema.name,
                source_path=str(metadata.path),
                source_size_bytes=metadata.size_bytes,
                source_modified_time_ns=metadata.modified_time_ns,
                watermark_value=watermark_value,
                completed_at_utc=datetime.now(UTC).isoformat(),
            )
        )


class LocalRawDataSink:
    """Write validated raw CSV parts through an atomic local transaction."""

    def __init__(self, raw_root: Path, retry_settings: RetrySettings) -> None:
        """Initialize the local raw root and transient-I/O retry policy."""
        self._raw_root = raw_root.expanduser().resolve(strict=False)
        self._staging_root = self._raw_root / "_staging"
        self._retry_settings = retry_settings

    def begin(self, table_name: str, run_id: str) -> RawWriteSession:
        """Create an isolated staging directory for a table."""
        ingestion_date = datetime.now(UTC).date().isoformat()
        staging_directory = self._staging_root / run_id / table_name
        final_directory = (
            self._raw_root
            / f"table={table_name}"
            / f"ingestion_date={ingestion_date}"
            / f"run_id={run_id}"
        )

        def create() -> None:
            staging_directory.mkdir(parents=True, exist_ok=False)

        self._run_with_retry(create)
        return RawWriteSession(
            table_name=table_name,
            run_id=run_id,
            staging_location=str(staging_directory),
            final_location=str(final_directory),
        )

    def write(self, session: RawWriteSession, frame: pd.DataFrame) -> None:
        """Write one chunk as an atomic CSV part inside staging."""
        if frame.empty:
            return
        part_name = f"part-{session.next_part_number:05d}.csv"
        staging_directory = Path(session.staging_location)
        final_part = staging_directory / part_name
        temporary_part = final_part.with_suffix(".csv.tmp")

        def write_part() -> None:
            frame.to_csv(
                temporary_part,
                index=False,
                encoding="utf-8",
                date_format="%Y-%m-%d",
                lineterminator="\n",
            )
            temporary_part.replace(final_part)

        self._run_with_retry(write_part)
        session.next_part_number += 1
        session.written_rows += len(frame)

    def commit(self, session: RawWriteSession) -> str:
        """Atomically move staged parts into the published raw partition."""
        if session.written_rows == 0:
            message = f"Cannot commit an empty raw session: {session.table_name}"
            raise ValueError(message)

        staging_directory = Path(session.staging_location)
        final_directory = Path(session.final_location)

        def publish() -> None:
            final_directory.parent.mkdir(parents=True, exist_ok=True)
            staging_directory.replace(final_directory)

        self._run_with_retry(publish)
        run_staging_directory = self._staging_root / session.run_id
        if run_staging_directory.is_dir():
            with suppress(OSError):
                run_staging_directory.rmdir()
        return str(final_directory)

    def abort(self, session: RawWriteSession) -> None:
        """Remove only the transaction's validated staging directory."""
        staging_directory = Path(session.staging_location).resolve(strict=False)
        staging_root = self._staging_root.resolve(strict=False)
        if not staging_directory.is_relative_to(staging_root):
            message = f"Refusing to remove path outside staging: {staging_directory}"
            raise ValueError(message)
        if staging_directory.is_dir():
            shutil.rmtree(staging_directory)

    def _run_with_retry[T](self, operation: Callable[[], T]) -> T:
        retrying = Retrying(
            stop=stop_after_attempt(self._retry_settings.max_attempts),
            wait=(
                wait_exponential(
                    multiplier=self._retry_settings.initial_delay_seconds,
                    max=self._retry_settings.max_delay_seconds,
                    exp_base=self._retry_settings.backoff_multiplier,
                )
                + wait_random(0, self._retry_settings.jitter_seconds)
            ),
            retry=retry_if_exception_type(OSError),
            reraise=True,
        )
        return retrying(operation)


def new_run_id() -> str:
    """Return a collision-resistant identifier for one ingestion run."""
    return uuid4().hex
