"""Application service orchestrating the enterprise CSV ingestion layer."""

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Final, Self

from config.config import get_settings
from config.env import EnvironmentSettings
from config.logger import get_logger
from config.paths import resolve_project_path
from etl.ingestion.csv_reader import (
    CSVReader,
    CSVReadError,
    CSVReadOptions,
    CSVSource,
)
from etl.ingestion.data_quality import DataQualityError, DataQualityValidator
from etl.ingestion.duplicate_handler import (
    DuplicateError,
    DuplicateHandler,
    DuplicateStrategy,
    SQLiteDuplicateKeyStore,
)
from etl.ingestion.incremental_loader import (
    IncrementalLoader,
    IncrementalStateError,
    JsonCheckpointStore,
    LoadMode,
    LocalRawDataSink,
    RawDataSink,
    new_run_id,
)
from etl.ingestion.missing_value_handler import (
    MissingValueError,
    MissingValueHandler,
)
from etl.ingestion.schema_validator import (
    RetailSchemaRegistry,
    SchemaValidationError,
    SchemaValidator,
)
from pydantic import Field, field_validator, model_validator
from structlog.stdlib import BoundLogger

DEFAULT_CHUNK_SIZE: Final = 100_000
DEFAULT_CSV_ENCODING: Final = "utf-8-sig"
DEFAULT_CSV_DELIMITER: Final = ","
RAW_DIRECTORY_NAME: Final = "raw"
CHECKPOINT_DIRECTORY_NAME: Final = "_state"


class TableIngestionStatus(StrEnum):
    """Terminal state for one table in a pipeline run."""

    SUCCEEDED = "succeeded"
    SKIPPED = "skipped"
    FAILED = "failed"


class IngestionRuntimeSettings(EnvironmentSettings):
    """Environment-backed settings specific to CSV ingestion."""

    source_root: Path = Field(validation_alias="INGESTION_SOURCE_ROOT")
    raw_root: Path | None = Field(
        default=None,
        validation_alias="INGESTION_RAW_ROOT",
    )
    checkpoint_root: Path | None = Field(
        default=None,
        validation_alias="INGESTION_CHECKPOINT_ROOT",
    )
    file_overrides: dict[str, Path] = Field(
        default_factory=dict,
        validation_alias="INGESTION_FILE_OVERRIDES",
    )
    chunk_size: int = Field(
        default=DEFAULT_CHUNK_SIZE,
        ge=1,
        validation_alias="INGESTION_CHUNK_SIZE",
    )
    load_mode: LoadMode = Field(
        default=LoadMode.INCREMENTAL,
        validation_alias="INGESTION_LOAD_MODE",
    )
    fail_fast: bool = Field(
        default=True,
        validation_alias="INGESTION_FAIL_FAST",
    )
    duplicate_strategy: DuplicateStrategy = Field(
        default=DuplicateStrategy.DROP,
        validation_alias="INGESTION_DUPLICATE_STRATEGY",
    )
    csv_encoding: str = Field(
        default=DEFAULT_CSV_ENCODING,
        min_length=1,
        validation_alias="INGESTION_CSV_ENCODING",
    )
    csv_delimiter: str = Field(
        default=DEFAULT_CSV_DELIMITER,
        min_length=1,
        max_length=1,
        validation_alias="INGESTION_CSV_DELIMITER",
    )

    @field_validator("source_root", "raw_root", "checkpoint_root", mode="before")
    @classmethod
    def normalize_root_path(cls, value: object) -> object:
        """Resolve configured roots relative to the project when necessary."""
        if value is None:
            return None
        if not isinstance(value, (str, Path)):
            message = "Ingestion roots must be strings or pathlib.Path values"
            raise TypeError(message)
        return resolve_project_path(value)

    @field_validator("file_overrides", mode="after")
    @classmethod
    def normalize_file_overrides(
        cls,
        value: dict[str, Path],
    ) -> dict[str, Path]:
        """Resolve configured table-specific source paths."""
        return {
            table_name: resolve_project_path(path) for table_name, path in value.items()
        }

    @model_validator(mode="after")
    def validate_roots(self) -> Self:
        """Prevent local output and checkpoints from targeting source files."""
        source_root = self.source_root.resolve(strict=False)
        for configured_root in (self.raw_root, self.checkpoint_root):
            if configured_root is None:
                continue
            resolved_root = configured_root.resolve(strict=False)
            roots_overlap = (
                resolved_root == source_root
                or resolved_root.is_relative_to(source_root)
                or source_root.is_relative_to(resolved_root)
            )
            if roots_overlap:
                message = "Ingestion output roots must be disjoint from source_root"
                raise ValueError(message)
        return self


@dataclass(frozen=True, slots=True)
class TableIngestionResult:
    """Operational result for one source table."""

    table_name: str
    status: TableIngestionStatus
    source_path: str
    rows_read: int
    rows_written: int
    duplicate_rows_removed: int
    missing_rows_dropped: int
    output_location: str | None
    reason: str


@dataclass(frozen=True, slots=True)
class PipelineRunResult:
    """Complete immutable result for an ingestion run."""

    run_id: str
    started_at_utc: str
    completed_at_utc: str
    tables: tuple[TableIngestionResult, ...]

    @property
    def succeeded(self) -> bool:
        """Return whether no table ended in a failed state."""
        return all(
            table.status is not TableIngestionStatus.FAILED for table in self.tables
        )


class IngestionPipelineError(RuntimeError):
    """Raised when fail-fast orchestration cannot complete a table."""


@dataclass(frozen=True, slots=True)
class IngestionDependencies:
    """Replaceable adapters and services required by the orchestrator."""

    registry: RetailSchemaRegistry
    reader: CSVReader
    schema_validator: SchemaValidator
    missing_value_handler: MissingValueHandler
    data_quality_validator: DataQualityValidator
    duplicate_handler: DuplicateHandler
    incremental_loader: IncrementalLoader
    raw_sink: RawDataSink


class IngestionPipeline:
    """Coordinate source reading, validation, cleaning, and raw persistence."""

    _EXPECTED_ERRORS = (
        CSVReadError,
        SchemaValidationError,
        MissingValueError,
        DuplicateError,
        DataQualityError,
        IncrementalStateError,
        OSError,
    )

    def __init__(
        self,
        *,
        runtime_settings: IngestionRuntimeSettings,
        dependencies: IngestionDependencies,
        logger: BoundLogger,
    ) -> None:
        """Initialize the service with explicit, replaceable dependencies."""
        self._runtime_settings = runtime_settings
        self._registry = dependencies.registry
        self._reader = dependencies.reader
        self._schema_validator = dependencies.schema_validator
        self._missing_value_handler = dependencies.missing_value_handler
        self._data_quality_validator = dependencies.data_quality_validator
        self._duplicate_handler = dependencies.duplicate_handler
        self._incremental_loader = dependencies.incremental_loader
        self._raw_sink = dependencies.raw_sink
        self._logger = logger

    def run(self) -> PipelineRunResult:
        """Ingest every configured retail table and return an audit summary."""
        run_id = new_run_id()
        started = datetime.now(UTC)
        sources = self._reader.discover_sources(
            self._runtime_settings.source_root,
            self._registry,
            self._runtime_settings.file_overrides,
        )
        self._logger.info(
            "ingestion_run_started",
            run_id=run_id,
            table_count=len(sources),
            load_mode=self._runtime_settings.load_mode.value,
        )
        results: list[TableIngestionResult] = []

        for source in sources:
            try:
                results.append(self._ingest_table(source, run_id))
            except self._EXPECTED_ERRORS as error:
                self._logger.exception(
                    "ingestion_table_failed",
                    run_id=run_id,
                    table=source.table_name,
                    error_type=type(error).__name__,
                )
                if self._runtime_settings.fail_fast:
                    message = f"Ingestion failed for {source.table_name}"
                    raise IngestionPipelineError(message) from error
                results.append(
                    TableIngestionResult(
                        table_name=source.table_name,
                        status=TableIngestionStatus.FAILED,
                        source_path=str(source.path),
                        rows_read=0,
                        rows_written=0,
                        duplicate_rows_removed=0,
                        missing_rows_dropped=0,
                        output_location=None,
                        reason=type(error).__name__,
                    )
                )

        completed = datetime.now(UTC)
        pipeline_result = PipelineRunResult(
            run_id=run_id,
            started_at_utc=started.isoformat(),
            completed_at_utc=completed.isoformat(),
            tables=tuple(results),
        )
        self._logger.info(
            "ingestion_run_completed",
            run_id=run_id,
            succeeded=pipeline_result.succeeded,
            duration_seconds=(completed - started).total_seconds(),
            rows_written=sum(table.rows_written for table in results),
        )
        return pipeline_result

    def close(self) -> None:
        """Release temporary duplicate-index resources."""
        self._duplicate_handler.close()

    def _ingest_table(
        self,
        source: CSVSource,
        run_id: str,
    ) -> TableIngestionResult:
        schema = self._registry.get(source.table_name)
        self._schema_validator.validate_header(
            self._reader.read_header(source),
            schema,
        )
        metadata = self._reader.metadata(source)
        decision = self._incremental_loader.decide(
            schema,
            metadata,
            self._runtime_settings.load_mode,
        )
        if not decision.should_process:
            self._logger.info(
                "ingestion_table_skipped",
                run_id=run_id,
                table=schema.name,
                reason=decision.reason,
            )
            return TableIngestionResult(
                table_name=schema.name,
                status=TableIngestionStatus.SKIPPED,
                source_path=str(source.path),
                rows_read=0,
                rows_written=0,
                duplicate_rows_removed=0,
                missing_rows_dropped=0,
                output_location=None,
                reason=decision.reason,
            )

        self._duplicate_handler.reset(schema.name)
        session = self._raw_sink.begin(schema.name, run_id)
        rows_read = 0
        rows_written = 0
        duplicate_rows = 0
        missing_rows = 0
        maximum_watermark = decision.lower_watermark
        transaction_closed = False

        self._logger.info(
            "ingestion_table_started",
            run_id=run_id,
            table=schema.name,
            source_size_bytes=metadata.size_bytes,
            reason=decision.reason,
        )
        try:
            for chunk_number, source_chunk in enumerate(
                self._reader.read_chunks(source)
            ):
                rows_read += len(source_chunk)
                missing_result = self._missing_value_handler.handle(
                    source_chunk,
                    schema,
                )
                missing_rows += missing_result.dropped_rows
                typed_chunk = self._schema_validator.validate_and_cast(
                    missing_result.frame,
                    schema,
                )
                new_rows = self._incremental_loader.filter_new_rows(
                    typed_chunk,
                    schema,
                    decision,
                )
                if new_rows.empty:
                    continue

                quality_report = self._data_quality_validator.validate(
                    new_rows,
                    schema,
                )
                quality_report.raise_for_errors()
                chunk_watermark = self._incremental_loader.maximum_watermark(
                    new_rows,
                    schema,
                )
                maximum_watermark = self._newer_watermark(
                    maximum_watermark,
                    chunk_watermark,
                )

                duplicate_result = self._duplicate_handler.handle(new_rows, schema)
                duplicate_rows += duplicate_result.duplicate_rows
                self._raw_sink.write(session, duplicate_result.frame)
                rows_written += len(duplicate_result.frame)
                self._logger.debug(
                    "ingestion_chunk_completed",
                    run_id=run_id,
                    table=schema.name,
                    chunk_number=chunk_number,
                    rows_read=len(source_chunk),
                    rows_written=len(duplicate_result.frame),
                )

            completed_metadata = self._reader.metadata(source)
            if completed_metadata != metadata:
                message = f"Source changed during ingestion: {source.path}"
                raise CSVReadError(message)

            output_location: str | None = None
            if rows_written:
                output_location = self._raw_sink.commit(session)
                transaction_closed = True
            else:
                self._raw_sink.abort(session)
                transaction_closed = True

            self._incremental_loader.commit(
                schema,
                metadata,
                maximum_watermark,
            )
        finally:
            if not transaction_closed:
                self._raw_sink.abort(session)

        self._logger.info(
            "ingestion_table_completed",
            run_id=run_id,
            table=schema.name,
            rows_read=rows_read,
            rows_written=rows_written,
            duplicates_removed=duplicate_rows,
            missing_rows_dropped=missing_rows,
            output_location=output_location,
        )
        return TableIngestionResult(
            table_name=schema.name,
            status=TableIngestionStatus.SUCCEEDED,
            source_path=str(source.path),
            rows_read=rows_read,
            rows_written=rows_written,
            duplicate_rows_removed=duplicate_rows,
            missing_rows_dropped=missing_rows,
            output_location=output_location,
            reason=decision.reason,
        )

    @staticmethod
    def _newer_watermark(
        current: str | None,
        candidate: str | None,
    ) -> str | None:
        if candidate is None:
            return current
        if current is None:
            return candidate
        return max(current, candidate)


def build_ingestion_pipeline(
    runtime_settings: IngestionRuntimeSettings | None = None,
) -> IngestionPipeline:
    """Build the default local pipeline from validated application settings."""
    application_settings = get_settings()
    runtime = runtime_settings or IngestionRuntimeSettings()
    raw_root = (
        runtime.raw_root
        if runtime.raw_root is not None
        else application_settings.paths.data_root / RAW_DIRECTORY_NAME
    )
    checkpoint_root = (
        runtime.checkpoint_root
        if runtime.checkpoint_root is not None
        else raw_root / CHECKPOINT_DIRECTORY_NAME
    )
    _validate_resolved_roots(runtime, raw_root, checkpoint_root)
    registry = RetailSchemaRegistry()
    reader = CSVReader(
        CSVReadOptions(
            chunk_size=runtime.chunk_size,
            encoding=runtime.csv_encoding,
            delimiter=runtime.csv_delimiter,
        ),
        application_settings.retry,
    )
    duplicate_store = SQLiteDuplicateKeyStore()
    return IngestionPipeline(
        runtime_settings=runtime,
        dependencies=IngestionDependencies(
            registry=registry,
            reader=reader,
            schema_validator=SchemaValidator(),
            missing_value_handler=MissingValueHandler(),
            data_quality_validator=DataQualityValidator(),
            duplicate_handler=DuplicateHandler(
                duplicate_store,
                runtime.duplicate_strategy,
            ),
            incremental_loader=IncrementalLoader(JsonCheckpointStore(checkpoint_root)),
            raw_sink=LocalRawDataSink(raw_root, application_settings.retry),
        ),
        logger=get_logger(__name__, component="retail_csv_ingestion"),
    )


def _validate_resolved_roots(
    runtime: IngestionRuntimeSettings,
    raw_root: Path,
    checkpoint_root: Path,
) -> None:
    """Reject source/output overlap after applying configured defaults."""
    output_roots = (
        raw_root.resolve(strict=False),
        checkpoint_root.resolve(strict=False),
    )
    source_locations = (
        runtime.source_root.resolve(strict=False),
        *(path.resolve(strict=False) for path in runtime.file_overrides.values()),
    )
    for source_location in source_locations:
        for output_root in output_roots:
            if (
                output_root == source_location
                or output_root.is_relative_to(source_location)
                or source_location.is_relative_to(output_root)
            ):
                message = (
                    "Ingestion source locations and resolved output roots "
                    "must be disjoint"
                )
                raise ValueError(message)
