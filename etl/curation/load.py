"""Spark-native persistence adapter for the local or Amazon S3 curated layer."""

import shutil
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Final

from config.settings import RetrySettings
from etl.ingestion.extract import (
    is_transient_storage_error,
    join_data_uri,
)
from etl.transformation.feature_engineering import CuratedDatasetBundle
from py4j.protocol import Py4JError
from pyspark.errors import PySparkException
from pyspark.sql import DataFrame
from pyspark.sql import functions as F  # noqa: N812
from structlog.stdlib import BoundLogger
from tenacity import (
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)

REMOTE_URI_SEPARATOR: Final = "://"


class CuratedWriteMode(StrEnum):
    """Supported Spark output modes for immutable curated run paths."""

    ERROR_IF_EXISTS = "errorifexists"
    OVERWRITE = "overwrite"


class ParquetCompression(StrEnum):
    """Parquet compression codecs supported by the curated writer."""

    GZIP = "gzip"
    LZ4 = "lz4"
    NONE = "none"
    SNAPPY = "snappy"
    ZSTD = "zstd"


@dataclass(frozen=True, slots=True)
class CuratedWriteConfig:
    """Immutable physical-write configuration for curated datasets."""

    mode: CuratedWriteMode
    compression: ParquetCompression
    target_partitions: int | None

    def __post_init__(self) -> None:
        """Validate physical output settings before executing Spark jobs."""
        if self.target_partitions is not None and self.target_partitions < 1:
            message = "Curated target_partitions must be positive"
            raise ValueError(message)


@dataclass(frozen=True, slots=True)
class CuratedDatasetSpec:
    """Physical partition contract for one named curated dataset."""

    name: str
    partition_columns: tuple[str, ...] = ()
    apply_target_partitions: bool = False


CURATED_DATASET_SPECS: Final[Mapping[str, CuratedDatasetSpec]] = MappingProxyType(
    {
        "demand_features": CuratedDatasetSpec(
            name="demand_features",
            partition_columns=("year", "month"),
            apply_target_partitions=True,
        ),
        "purchase_order_features": CuratedDatasetSpec(
            name="purchase_order_features",
            partition_columns=("order_year", "order_month"),
            apply_target_partitions=True,
        ),
        "dim_product": CuratedDatasetSpec(name="dim_product"),
        "dim_store": CuratedDatasetSpec(name="dim_store"),
        "dim_supplier": CuratedDatasetSpec(name="dim_supplier"),
        "dim_warehouse": CuratedDatasetSpec(name="dim_warehouse"),
        "dim_date": CuratedDatasetSpec(name="dim_date"),
        "weather": CuratedDatasetSpec(name="weather"),
        "promotions": CuratedDatasetSpec(name="promotions"),
        "holidays": CuratedDatasetSpec(name="holidays"),
    }
)


class CuratedLoadError(RuntimeError):
    """Raised when a curated run cannot be safely persisted."""


@dataclass(frozen=True, slots=True)
class CuratedLoadResult:
    """Published locations for one successful curated-layer run."""

    run_id: str
    processing_date: str
    run_location: str
    dataset_locations: Mapping[str, str]


class CuratedDataLoader:
    """Write typed Parquet datasets with local atomic-run publication."""

    def __init__(
        self,
        *,
        curated_uri: str,
        config: CuratedWriteConfig,
        retry_settings: RetrySettings,
        logger: BoundLogger,
    ) -> None:
        """Initialize curated persistence and transient retry policies."""
        self._curated_uri = curated_uri
        self._config = config
        self._retry_settings = retry_settings
        self._logger = logger

    def load(
        self,
        bundle: CuratedDatasetBundle,
        run_id: str,
    ) -> CuratedLoadResult:
        """Persist all curated datasets and publish the completed run."""
        self._validate_bundle(bundle)
        processing_date = datetime.now(UTC).date().isoformat()
        final_run_uri = join_data_uri(
            self._curated_uri,
            f"processing_date={processing_date}",
            f"run_id={run_id}",
        )
        is_local = self._is_local_uri(self._curated_uri)
        write_run_uri = (
            join_data_uri(self._curated_uri, "_staging", f"run_id={run_id}")
            if is_local
            else final_run_uri
        )
        locations: dict[str, str] = {}

        try:
            for dataset_name, spec in CURATED_DATASET_SPECS.items():
                dataset_uri = join_data_uri(
                    write_run_uri,
                    f"dataset={dataset_name}",
                )
                self._write_dataset(bundle.get(dataset_name), spec, dataset_uri)
                locations[dataset_name] = join_data_uri(
                    final_run_uri,
                    f"dataset={dataset_name}",
                )
                self._logger.info(
                    "pyspark_curated_dataset_written",
                    dataset=dataset_name,
                    run_id=run_id,
                    partition_columns=spec.partition_columns,
                )
            if is_local:
                self._publish_local_run(write_run_uri, final_run_uri)
        except CuratedLoadError:
            if is_local:
                self._abort_local_run(write_run_uri)
            raise
        except (OSError, PySparkException, Py4JError, ValueError) as error:
            if is_local:
                self._abort_local_run(write_run_uri)
            message = f"Unable to publish curated run {run_id}"
            raise CuratedLoadError(message) from error

        return CuratedLoadResult(
            run_id=run_id,
            processing_date=processing_date,
            run_location=final_run_uri,
            dataset_locations=MappingProxyType(locations),
        )

    def _validate_bundle(self, bundle: CuratedDatasetBundle) -> None:
        expected = set(CURATED_DATASET_SPECS)
        actual = set(bundle.datasets)
        if actual == expected:
            return
        missing = tuple(sorted(expected.difference(actual)))
        unexpected = tuple(sorted(actual.difference(expected)))
        message = (
            "Curated dataset contract mismatch: "
            f"missing={missing}, unexpected={unexpected}"
        )
        raise CuratedLoadError(message)

    def _write_dataset(
        self,
        frame: DataFrame,
        spec: CuratedDatasetSpec,
        destination_uri: str,
    ) -> None:
        missing_partitions = tuple(
            column for column in spec.partition_columns if column not in frame.columns
        )
        if missing_partitions:
            message = (
                f"Dataset {spec.name} is missing partition columns: "
                f"{missing_partitions}"
            )
            raise CuratedLoadError(message)

        prepared = frame
        if spec.apply_target_partitions and self._config.target_partitions:
            partition_columns = [F.col(name) for name in spec.partition_columns]
            prepared = prepared.repartition(
                self._config.target_partitions,
                *partition_columns,
            )

        def write() -> None:
            writer = (
                prepared.write.mode(self._config.mode.value)
                .format("parquet")
                .option("compression", self._config.compression.value)
            )
            if spec.partition_columns:
                writer = writer.partitionBy(*spec.partition_columns)
            writer.save(destination_uri)

        self._run_with_retry(write)

    def _publish_local_run(
        self,
        staging_uri: str,
        final_uri: str,
    ) -> None:
        staging_path = Path(staging_uri).resolve(strict=True)
        curated_root = Path(self._curated_uri).expanduser().resolve(strict=False)
        staging_root = (curated_root / "_staging").resolve(strict=False)
        final_path = Path(final_uri).resolve(strict=False)
        if not staging_path.is_relative_to(staging_root):
            message = f"Unsafe curated staging path: {staging_path}"
            raise CuratedLoadError(message)
        if final_path.exists():
            message = f"Curated run already exists: {final_path}"
            raise CuratedLoadError(message)

        def publish() -> None:
            final_path.parent.mkdir(parents=True, exist_ok=True)
            staging_path.replace(final_path)

        self._run_with_retry(publish)
        staging_parent = staging_root
        if staging_parent.is_dir() and not any(staging_parent.iterdir()):
            with suppress(OSError):
                staging_parent.rmdir()

    def _abort_local_run(self, staging_uri: str) -> None:
        staging_path = Path(staging_uri).resolve(strict=False)
        curated_root = Path(self._curated_uri).expanduser().resolve(strict=False)
        staging_root = (curated_root / "_staging").resolve(strict=False)
        if not staging_path.is_relative_to(staging_root):
            message = f"Refusing to remove path outside staging: {staging_path}"
            raise CuratedLoadError(message)
        if staging_path.is_dir():
            try:
                shutil.rmtree(staging_path)
            except OSError as error:
                self._logger.warning(
                    "pyspark_curated_staging_cleanup_failed",
                    staging_path=str(staging_path),
                    error_type=type(error).__name__,
                )

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

    @staticmethod
    def _is_local_uri(uri: str) -> bool:
        return REMOTE_URI_SEPARATOR not in uri
