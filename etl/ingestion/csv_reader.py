"""Chunked, retry-aware CSV source reader for retail ingestion."""

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from config.settings import RetrySettings
from etl.ingestion.schema_validator import RetailSchemaRegistry
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)


@dataclass(frozen=True, slots=True)
class CSVReadOptions:
    """Configuration for streaming CSV parsing."""

    chunk_size: int
    encoding: str = "utf-8-sig"
    delimiter: str = ","
    quote_character: str = '"'


@dataclass(frozen=True, slots=True)
class CSVSource:
    """Resolved local CSV source for one logical table."""

    table_name: str
    path: Path


@dataclass(frozen=True, slots=True)
class SourceMetadata:
    """Stable source identity used by incremental loading."""

    path: Path
    size_bytes: int
    modified_time_ns: int


class CSVReadError(OSError):
    """Raised when a CSV source cannot be safely inspected or streamed."""


class CSVReader:
    """Read multiple CSV files in bounded-memory chunks."""

    def __init__(
        self,
        options: CSVReadOptions,
        retry_settings: RetrySettings,
    ) -> None:
        """Initialize parsing and transient-I/O retry policies."""
        if options.chunk_size < 1:
            message = "CSV chunk size must be at least one row"
            raise ValueError(message)
        self._options = options
        self._retry_settings = retry_settings

    def discover_sources(
        self,
        source_root: Path,
        registry: RetailSchemaRegistry,
        file_overrides: Mapping[str, Path] | None = None,
    ) -> tuple[CSVSource, ...]:
        """Resolve and validate configured paths for all supported tables."""
        overrides = dict(file_overrides or {})
        supported = {schema.name for schema in registry.tables()}
        unknown = tuple(sorted(set(overrides).difference(supported)))
        if unknown:
            message = f"File overrides contain unsupported tables: {unknown}"
            raise CSVReadError(message)

        sources: list[CSVSource] = []
        for schema in registry.tables():
            configured_path = overrides.get(
                schema.name,
                source_root / schema.file_name,
            )
            resolved_path = configured_path.expanduser().resolve(strict=False)
            if not resolved_path.is_file():
                message = (
                    f"Configured CSV source is missing for {schema.name}: "
                    f"{resolved_path}"
                )
                raise CSVReadError(message)
            sources.append(CSVSource(schema.name, resolved_path))
        return tuple(sources)

    def metadata(self, source: CSVSource) -> SourceMetadata:
        """Read retry-protected source metadata without changing the file."""

        def inspect() -> SourceMetadata:
            stat = source.path.stat()
            return SourceMetadata(
                path=source.path,
                size_bytes=stat.st_size,
                modified_time_ns=stat.st_mtime_ns,
            )

        try:
            return self._run_with_retry(inspect)
        except OSError as error:
            message = f"Unable to inspect CSV source: {source.path}"
            raise CSVReadError(message) from error

    def read_header(self, source: CSVSource) -> tuple[str, ...]:
        """Read the CSV header with retry for transient file access errors."""

        def read() -> tuple[str, ...]:
            frame = pd.read_csv(
                source.path,
                nrows=0,
                encoding=self._options.encoding,
                encoding_errors="strict",
                sep=self._options.delimiter,
                quotechar=self._options.quote_character,
                on_bad_lines="error",
            )
            return tuple(str(column) for column in frame.columns)

        try:
            return self._run_with_retry(read)
        except (OSError, UnicodeError, pd.errors.ParserError) as error:
            message = f"Unable to read CSV header: {source.path}"
            raise CSVReadError(message) from error

    def read_chunks(self, source: CSVSource) -> Iterator[pd.DataFrame]:
        """Yield source rows as string-preserving, bounded-memory chunks."""
        self._preflight(source.path)
        try:
            reader = pd.read_csv(
                source.path,
                dtype="string",
                keep_default_na=False,
                chunksize=self._options.chunk_size,
                encoding=self._options.encoding,
                encoding_errors="strict",
                sep=self._options.delimiter,
                quotechar=self._options.quote_character,
                on_bad_lines="error",
            )
            yield from reader
        except (OSError, UnicodeError, pd.errors.ParserError) as error:
            message = f"Unable to stream CSV source: {source.path}"
            raise CSVReadError(message) from error

    def _preflight(self, path: Path) -> None:
        def open_source() -> None:
            with path.open("rb") as handle:
                handle.read(1)

        try:
            self._run_with_retry(open_source)
        except OSError as error:
            message = f"Unable to open CSV source: {path}"
            raise CSVReadError(message) from error

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
