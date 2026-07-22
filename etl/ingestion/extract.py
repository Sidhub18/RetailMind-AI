"""PySpark extraction adapter for the validated Phase 5 raw layer."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Final

from config.settings import RetrySettings
from etl.ingestion.schema_validator import (
    LogicalType,
    RetailSchemaRegistry,
    TableSchema,
)
from py4j.protocol import Py4JError
from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from structlog.stdlib import BoundLogger
from tenacity import (
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)

RAW_PART_GLOB: Final = "part-*.csv"
TRANSIENT_STORAGE_ERROR_MARKERS: Final = (
    "connection reset",
    "connection timed out",
    "connection timeout",
    "request timeout",
    "service unavailable",
    "slow down",
    "temporarily unavailable",
    "throttl",
)


class RawExtractionError(RuntimeError):
    """Raised when a validated raw table cannot be extracted."""


def is_transient_storage_error(error: BaseException) -> bool:
    """Identify retryable local or remote object-storage failures."""
    if isinstance(error, OSError):
        return True
    normalized_message = str(error).casefold()
    return any(
        marker in normalized_message for marker in TRANSIENT_STORAGE_ERROR_MARKERS
    )


@dataclass(frozen=True, slots=True)
class RawDatasetBundle:
    """Immutable collection of extracted Spark DataFrames by table name."""

    frames: Mapping[str, DataFrame]

    def get(self, table_name: str) -> DataFrame:
        """Return an extracted table or fail with a clear contract error."""
        try:
            return self.frames[table_name]
        except KeyError as error:
            message = f"Raw dataset bundle does not contain {table_name}"
            raise RawExtractionError(message) from error


@dataclass(frozen=True, slots=True)
class RawExtractionConfig:
    """Infrastructure configuration for raw-layer extraction."""

    raw_uri: str
    retry_settings: RetrySettings


def join_data_uri(root: str, *parts: str) -> str:
    """Join local paths and Spark-compatible object-storage URIs safely."""
    if "://" in root:
        normalized_parts = [root.rstrip("/")]
        normalized_parts.extend(part.strip("/") for part in parts)
        return "/".join(normalized_parts)
    return str(Path(root).expanduser().joinpath(*parts).resolve(strict=False))


class SparkSchemaFactory:
    """Translate the shared Phase 4 contracts into explicit Spark schemas."""

    _TYPE_MAP: Final[Mapping[LogicalType, DataType]] = MappingProxyType(
        {
            LogicalType.STRING: StringType(),
            LogicalType.INTEGER: LongType(),
            LogicalType.DECIMAL: DoubleType(),
            LogicalType.DATE: DateType(),
            LogicalType.BOOLEAN: BooleanType(),
        }
    )

    def build(self, table_schema: TableSchema) -> StructType:
        """Build a Spark ``StructType`` without schema inference."""
        return StructType(
            [
                StructField(
                    column.name,
                    self._TYPE_MAP[column.logical_type],
                    nullable=not column.required,
                )
                for column in table_schema.columns
            ]
        )


class RawDatasetExtractor:
    """Read all validated raw tables through Spark's distributed CSV reader."""

    def __init__(
        self,
        *,
        spark: SparkSession,
        config: RawExtractionConfig,
        registry: RetailSchemaRegistry,
        schema_factory: SparkSchemaFactory,
        logger: BoundLogger,
    ) -> None:
        """Initialize the extraction adapter and its infrastructure policies."""
        self._spark = spark
        self._raw_uri = config.raw_uri
        self._registry = registry
        self._schema_factory = schema_factory
        self._retry_settings = config.retry_settings
        self._logger = logger

    def extract_all(self) -> RawDatasetBundle:
        """Extract every registered retail table from immutable raw parts."""
        frames: dict[str, DataFrame] = {}
        for table_schema in self._registry.tables():
            frame = self._read_table(table_schema)
            frames[table_schema.name] = frame
            self._logger.info(
                "pyspark_raw_table_extracted",
                table=table_schema.name,
                source_file_count=len(frame.inputFiles()),
            )
        return RawDatasetBundle(frames=MappingProxyType(frames))

    def _read_table(self, table_schema: TableSchema) -> DataFrame:
        table_uri = join_data_uri(
            self._raw_uri,
            f"table={table_schema.name}",
        )

        def read() -> DataFrame:
            frame = (
                self._spark.read.schema(self._schema_factory.build(table_schema))
                .option("header", value=True)
                .option("mode", value="FAILFAST")
                .option("enforceSchema", value=False)
                .option("recursiveFileLookup", value=True)
                .option("pathGlobFilter", value=RAW_PART_GLOB)
                .csv(table_uri)
            )
            if not frame.inputFiles():
                message = f"No raw CSV parts found for {table_schema.name}: {table_uri}"
                raise RawExtractionError(message)
            return frame.withColumn("_source_file", F.input_file_name())

        try:
            return self._run_with_retry(read)
        except RawExtractionError:
            raise
        except (OSError, PySparkException, Py4JError) as error:
            message = f"Unable to extract raw table {table_schema.name}: {table_uri}"
            raise RawExtractionError(message) from error

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
            retry=retry_if_exception(is_transient_storage_error),
            reraise=True,
        )
        return retrying(operation)
