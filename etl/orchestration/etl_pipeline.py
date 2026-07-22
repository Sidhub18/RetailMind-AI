"""Application service orchestrating raw-to-curated enterprise PySpark ETL."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Final, Self

from config.config import initialize_configuration
from config.env import DeploymentEnvironment, EnvironmentSettings
from config.logger import get_logger
from config.paths import resolve_project_path
from etl.curation.load import (
    CuratedDataLoader,
    CuratedLoadError,
    CuratedLoadResult,
    CuratedWriteConfig,
    CuratedWriteMode,
    ParquetCompression,
)
from etl.ingestion.extract import (
    RawDatasetExtractor,
    RawExtractionConfig,
    RawExtractionError,
    SparkSchemaFactory,
)
from etl.ingestion.incremental_loader import new_run_id
from etl.ingestion.schema_validator import RetailSchemaRegistry
from etl.transformation.feature_engineering import (
    FeatureEngineeringConfig,
    FeatureEngineeringError,
    RetailFeatureEngineer,
)
from etl.transformation.transform import RetailTransformer, TransformationError
from py4j.protocol import Py4JError
from pydantic import Field, field_validator, model_validator
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession
from structlog.stdlib import BoundLogger

DEFAULT_SPARK_APPLICATION_NAME: Final = "RetailMind-AI-Enterprise-ETL"
DEFAULT_SHUFFLE_PARTITIONS: Final = 200
DEFAULT_LAG_DAYS: Final = (1, 7, 14, 28)
DEFAULT_ROLLING_WINDOWS: Final = (7, 28)
DEFAULT_MINIMUM_HISTORY_PERIODS: Final = 7
DEFAULT_SEGMENTATION_LOOKBACK_DAYS: Final = 90
DEFAULT_ABC_A_THRESHOLD: Final = 0.80
DEFAULT_ABC_B_THRESHOLD: Final = 0.95
DEFAULT_XYZ_X_THRESHOLD: Final = 0.50
DEFAULT_XYZ_Y_THRESHOLD: Final = 1.00
DEFAULT_VOLATILITY_LOW_THRESHOLD: Final = 0.50
DEFAULT_VOLATILITY_HIGH_THRESHOLD: Final = 1.00
DEFAULT_PARQUET_COMPRESSION: Final = ParquetCompression.SNAPPY
RAW_DIRECTORY_NAME: Final = "raw"
CURATED_DIRECTORY_NAME: Final = "curated"
LOCAL_SPARK_MASTER: Final = "local[*]"

type PositiveInteger = Annotated[int, Field(ge=1)]


class SparkLogLevel(StrEnum):
    """Log levels accepted by ``SparkContext.setLogLevel``."""

    ALL = "ALL"
    DEBUG = "DEBUG"
    ERROR = "ERROR"
    FATAL = "FATAL"
    INFO = "INFO"
    OFF = "OFF"
    TRACE = "TRACE"
    WARN = "WARN"


class PySparkEtlRuntimeSettings(EnvironmentSettings):
    """Environment-backed operational and feature settings for Phase 6."""

    raw_uri: str | None = Field(
        default=None,
        validation_alias="PYSPARK_ETL_RAW_URI",
    )
    curated_uri: str | None = Field(
        default=None,
        validation_alias="PYSPARK_ETL_CURATED_URI",
    )
    spark_application_name: str = Field(
        default=DEFAULT_SPARK_APPLICATION_NAME,
        min_length=1,
        validation_alias="PYSPARK_ETL_APPLICATION_NAME",
    )
    spark_master: str | None = Field(
        default=None,
        validation_alias="PYSPARK_ETL_MASTER",
    )
    spark_log_level: SparkLogLevel = Field(
        default=SparkLogLevel.WARN,
        validation_alias="PYSPARK_ETL_LOG_LEVEL",
    )
    shuffle_partitions: int = Field(
        default=DEFAULT_SHUFFLE_PARTITIONS,
        ge=1,
        validation_alias="PYSPARK_ETL_SHUFFLE_PARTITIONS",
    )
    target_output_partitions: int | None = Field(
        default=None,
        ge=1,
        validation_alias="PYSPARK_ETL_TARGET_OUTPUT_PARTITIONS",
    )
    adaptive_query_execution: bool = Field(
        default=True,
        validation_alias="PYSPARK_ETL_ADAPTIVE_QUERY_EXECUTION",
    )
    parquet_compression: ParquetCompression = Field(
        default=DEFAULT_PARQUET_COMPRESSION,
        validation_alias="PYSPARK_ETL_PARQUET_COMPRESSION",
    )
    write_mode: CuratedWriteMode = Field(
        default=CuratedWriteMode.OVERWRITE,
        validation_alias="PYSPARK_ETL_WRITE_MODE",
    )
    lag_days: tuple[PositiveInteger, ...] = Field(
        default=DEFAULT_LAG_DAYS,
        validation_alias="PYSPARK_ETL_LAG_DAYS",
    )
    rolling_windows: tuple[PositiveInteger, ...] = Field(
        default=DEFAULT_ROLLING_WINDOWS,
        validation_alias="PYSPARK_ETL_ROLLING_WINDOWS",
    )
    minimum_history_periods: int = Field(
        default=DEFAULT_MINIMUM_HISTORY_PERIODS,
        ge=2,
        validation_alias="PYSPARK_ETL_MINIMUM_HISTORY_PERIODS",
    )
    abc_lookback_days: int = Field(
        default=DEFAULT_SEGMENTATION_LOOKBACK_DAYS,
        ge=2,
        validation_alias="PYSPARK_ETL_ABC_LOOKBACK_DAYS",
    )
    abc_a_threshold: float = Field(
        default=DEFAULT_ABC_A_THRESHOLD,
        gt=0,
        lt=1,
        validation_alias="PYSPARK_ETL_ABC_A_THRESHOLD",
    )
    abc_b_threshold: float = Field(
        default=DEFAULT_ABC_B_THRESHOLD,
        gt=0,
        lt=1,
        validation_alias="PYSPARK_ETL_ABC_B_THRESHOLD",
    )
    xyz_lookback_days: int = Field(
        default=DEFAULT_SEGMENTATION_LOOKBACK_DAYS,
        ge=2,
        validation_alias="PYSPARK_ETL_XYZ_LOOKBACK_DAYS",
    )
    xyz_x_threshold: float = Field(
        default=DEFAULT_XYZ_X_THRESHOLD,
        gt=0,
        validation_alias="PYSPARK_ETL_XYZ_X_THRESHOLD",
    )
    xyz_y_threshold: float = Field(
        default=DEFAULT_XYZ_Y_THRESHOLD,
        gt=0,
        validation_alias="PYSPARK_ETL_XYZ_Y_THRESHOLD",
    )
    volatility_low_threshold: float = Field(
        default=DEFAULT_VOLATILITY_LOW_THRESHOLD,
        gt=0,
        validation_alias="PYSPARK_ETL_VOLATILITY_LOW_THRESHOLD",
    )
    volatility_high_threshold: float = Field(
        default=DEFAULT_VOLATILITY_HIGH_THRESHOLD,
        gt=0,
        validation_alias="PYSPARK_ETL_VOLATILITY_HIGH_THRESHOLD",
    )

    @field_validator("raw_uri", "curated_uri", mode="before")
    @classmethod
    def normalize_layer_uri(cls, value: object) -> object:
        """Normalize local layer paths while preserving Spark storage URIs."""
        if value is None:
            return None
        if not isinstance(value, (str, Path)):
            message = "ETL locations must be strings or pathlib.Path values"
            raise TypeError(message)
        normalized = str(value).strip()
        if not normalized:
            return None
        if "://" in normalized:
            return normalized.rstrip("/")
        return str(resolve_project_path(normalized))

    @field_validator("spark_master", mode="before")
    @classmethod
    def normalize_spark_master(cls, value: object) -> object:
        """Normalize an optional Spark master without treating it as a path."""
        if value is None:
            return None
        if not isinstance(value, str):
            message = "Spark master must be a string"
            raise TypeError(message)
        normalized = value.strip()
        return normalized or None

    @field_validator("lag_days", "rolling_windows", mode="after")
    @classmethod
    def normalize_windows(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        """Return unique feature windows in deterministic ascending order."""
        if not value:
            message = "Feature window configuration cannot be empty"
            raise ValueError(message)
        return tuple(sorted(set(value)))

    @model_validator(mode="after")
    def validate_thresholds(self) -> Self:
        """Validate ordered segmentation thresholds and history coverage."""
        if self.abc_a_threshold >= self.abc_b_threshold:
            message = "ABC A threshold must be below the B threshold"
            raise ValueError(message)
        if self.xyz_x_threshold >= self.xyz_y_threshold:
            message = "XYZ X threshold must be below the Y threshold"
            raise ValueError(message)
        if self.volatility_low_threshold >= self.volatility_high_threshold:
            message = "Volatility low threshold must be below the high threshold"
            raise ValueError(message)
        if self.abc_lookback_days < self.minimum_history_periods:
            message = "ABC lookback must cover minimum history"
            raise ValueError(message)
        if self.xyz_lookback_days < self.minimum_history_periods:
            message = "XYZ lookback must cover minimum history"
            raise ValueError(message)
        return self


class SparkSessionFactory:
    """Create a consistently configured Spark session for local or AWS use."""

    def __init__(
        self,
        *,
        runtime: PySparkEtlRuntimeSettings,
        environment: DeploymentEnvironment,
    ) -> None:
        """Initialize the factory from validated runtime configuration."""
        self._runtime = runtime
        self._environment = environment

    def create(self) -> SparkSession:
        """Create or reuse a Spark session with enterprise SQL safeguards."""
        builder = SparkSession.builder.appName(self._runtime.spark_application_name)
        if self._runtime.spark_master is not None:
            builder = builder.master(self._runtime.spark_master)
        elif not self._environment.is_deployed:
            builder = builder.master(LOCAL_SPARK_MASTER)

        spark = (
            builder.config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.ansi.enabled", value=True)
            .config("spark.sql.caseSensitive", value=True)
            .config(
                "spark.sql.shuffle.partitions",
                value=self._runtime.shuffle_partitions,
            )
            .config(
                "spark.sql.adaptive.enabled",
                value=self._runtime.adaptive_query_execution,
            )
            .config(
                "spark.sql.adaptive.coalescePartitions.enabled",
                value=self._runtime.adaptive_query_execution,
            )
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel(self._runtime.spark_log_level.value)
        return spark


@dataclass(frozen=True, slots=True)
class PySparkEtlDependencies:
    """Replaceable services required by the ETL application service."""

    extractor: RawDatasetExtractor
    transformer: RetailTransformer
    feature_engineer: RetailFeatureEngineer
    loader: CuratedDataLoader


@dataclass(frozen=True, slots=True)
class PySparkEtlRunResult:
    """Operational result returned by a successful curated ETL run."""

    run_id: str
    started_at_utc: str
    completed_at_utc: str
    curated_location: str
    dataset_locations: Mapping[str, str]


class PySparkEtlPipelineError(RuntimeError):
    """Raised when the enterprise PySpark ETL run cannot complete."""


class EnterprisePySparkEtlPipeline:
    """Orchestrate extraction, transformation, features, and curated loading."""

    _EXPECTED_ERRORS = (
        RawExtractionError,
        TransformationError,
        FeatureEngineeringError,
        CuratedLoadError,
        PySparkException,
        Py4JError,
        OSError,
    )

    def __init__(
        self,
        *,
        spark: SparkSession,
        dependencies: PySparkEtlDependencies,
        logger: BoundLogger,
        owns_spark_session: bool,
    ) -> None:
        """Initialize the application service with explicit dependencies."""
        self._spark = spark
        self._extractor = dependencies.extractor
        self._transformer = dependencies.transformer
        self._feature_engineer = dependencies.feature_engineer
        self._loader = dependencies.loader
        self._logger = logger
        self._owns_spark_session = owns_spark_session

    def run(self) -> PySparkEtlRunResult:
        """Execute one complete raw-to-curated Spark lineage."""
        run_id = new_run_id()
        started = datetime.now(UTC)
        self._logger.info("pyspark_etl_run_started", run_id=run_id)
        try:
            raw = self._extractor.extract_all()
            transformed = self._transformer.transform(raw)
            curated = self._feature_engineer.build(transformed)
            load_result = self._loader.load(curated, run_id)
        except self._EXPECTED_ERRORS as error:
            self._logger.exception(
                "pyspark_etl_run_failed",
                run_id=run_id,
                error_type=type(error).__name__,
            )
            message = f"Enterprise PySpark ETL failed for run {run_id}"
            raise PySparkEtlPipelineError(message) from error

        completed = datetime.now(UTC)
        self._logger.info(
            "pyspark_etl_run_completed",
            run_id=run_id,
            duration_seconds=(completed - started).total_seconds(),
            curated_location=load_result.run_location,
        )
        return self._build_result(run_id, started, completed, load_result)

    def close(self) -> None:
        """Stop the Spark session when it is owned by this pipeline."""
        if self._owns_spark_session:
            self._spark.stop()

    @staticmethod
    def _build_result(
        run_id: str,
        started: datetime,
        completed: datetime,
        load_result: CuratedLoadResult,
    ) -> PySparkEtlRunResult:
        return PySparkEtlRunResult(
            run_id=run_id,
            started_at_utc=started.isoformat(),
            completed_at_utc=completed.isoformat(),
            curated_location=load_result.run_location,
            dataset_locations=MappingProxyType(dict(load_result.dataset_locations)),
        )


def build_etl_pipeline(
    runtime_settings: PySparkEtlRuntimeSettings | None = None,
    spark: SparkSession | None = None,
) -> EnterprisePySparkEtlPipeline:
    """Build the Phase 6 pipeline from existing application configuration."""
    application_settings = initialize_configuration()
    runtime = runtime_settings or PySparkEtlRuntimeSettings()
    raw_uri = runtime.raw_uri or str(
        application_settings.paths.data_root / RAW_DIRECTORY_NAME
    )
    curated_uri = runtime.curated_uri or str(
        application_settings.paths.data_root / CURATED_DIRECTORY_NAME
    )
    _validate_layer_uris(raw_uri, curated_uri)
    feature_config = FeatureEngineeringConfig(
        lag_days=runtime.lag_days,
        rolling_windows=runtime.rolling_windows,
        minimum_history_periods=runtime.minimum_history_periods,
        abc_lookback_days=runtime.abc_lookback_days,
        abc_a_threshold=runtime.abc_a_threshold,
        abc_b_threshold=runtime.abc_b_threshold,
        xyz_lookback_days=runtime.xyz_lookback_days,
        xyz_x_threshold=runtime.xyz_x_threshold,
        xyz_y_threshold=runtime.xyz_y_threshold,
        volatility_low_threshold=runtime.volatility_low_threshold,
        volatility_high_threshold=runtime.volatility_high_threshold,
    )
    owns_spark_session = spark is None
    active_spark = (
        spark
        or SparkSessionFactory(
            runtime=runtime,
            environment=application_settings.application.environment,
        ).create()
    )
    logger = get_logger(__name__, component="enterprise_pyspark_etl")
    registry = RetailSchemaRegistry()
    return EnterprisePySparkEtlPipeline(
        spark=active_spark,
        dependencies=PySparkEtlDependencies(
            extractor=RawDatasetExtractor(
                spark=active_spark,
                config=RawExtractionConfig(
                    raw_uri=raw_uri,
                    retry_settings=application_settings.retry,
                ),
                registry=registry,
                schema_factory=SparkSchemaFactory(),
                logger=logger,
            ),
            transformer=RetailTransformer(registry=registry, logger=logger),
            feature_engineer=RetailFeatureEngineer(
                config=feature_config,
                logger=logger,
            ),
            loader=CuratedDataLoader(
                curated_uri=curated_uri,
                config=CuratedWriteConfig(
                    mode=runtime.write_mode,
                    compression=runtime.parquet_compression,
                    target_partitions=runtime.target_output_partitions,
                ),
                retry_settings=application_settings.retry,
                logger=logger,
            ),
        ),
        logger=logger,
        owns_spark_session=owns_spark_session,
    )


def _validate_layer_uris(raw_uri: str, curated_uri: str) -> None:
    """Prevent raw and curated layers from nesting inside one another."""
    if ("://" in raw_uri) != ("://" in curated_uri):
        return
    if "://" in raw_uri:
        normalized_raw = raw_uri.rstrip("/")
        normalized_curated = curated_uri.rstrip("/")
        overlap = (
            normalized_raw == normalized_curated
            or normalized_raw.startswith(f"{normalized_curated}/")
            or normalized_curated.startswith(f"{normalized_raw}/")
        )
    else:
        raw_path = Path(raw_uri).resolve(strict=False)
        curated_path = Path(curated_uri).resolve(strict=False)
        overlap = (
            raw_path == curated_path
            or raw_path.is_relative_to(curated_path)
            or curated_path.is_relative_to(raw_path)
        )
    if overlap:
        message = "Raw and curated layer locations must be disjoint"
        raise ValueError(message)
