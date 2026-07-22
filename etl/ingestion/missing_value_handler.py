"""Policy-driven missing-value handling for ingestion chunks."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum

import pandas as pd
from etl.ingestion.schema_validator import TableSchema


class MissingValueStrategy(StrEnum):
    """Supported handling strategies for optional missing values."""

    PRESERVE = "preserve"
    DROP_ROW = "drop_row"
    FILL = "fill"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class MissingValuePolicy:
    """Column-level missing-value policies and explicitly approved fills."""

    strategies: Mapping[str, MissingValueStrategy] = field(default_factory=dict)
    fill_values: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MissingValueResult:
    """Result of applying missing-value policies to one chunk."""

    frame: pd.DataFrame
    dropped_rows: int
    filled_values: int
    preserved_values: int


class MissingValueError(ValueError):
    """Raised when required data is absent or a policy is incomplete."""


class MissingValueHandler:
    """Handle missing values without silently fabricating retail data."""

    def __init__(
        self,
        policies: Mapping[str, MissingValuePolicy] | None = None,
    ) -> None:
        """Initialize optional table-specific policies."""
        self._policies = dict(policies or {})

    def handle(
        self,
        frame: pd.DataFrame,
        schema: TableSchema,
    ) -> MissingValueResult:
        """Apply required-field and optional-field policies to a chunk."""
        policy = self._policies.get(schema.name, MissingValuePolicy())
        working = frame.copy()
        drop_mask = pd.Series(data=False, index=working.index)
        filled_values = 0
        preserved_values = 0

        for column in schema.columns:
            missing = self._missing_mask(working[column.name])
            missing_count = int(missing.sum())
            if missing_count == 0:
                continue
            if column.required:
                message = (
                    f"Required values missing in {schema.name}.{column.name}: "
                    f"count={missing_count}"
                )
                raise MissingValueError(message)

            strategy = policy.strategies.get(
                column.name,
                MissingValueStrategy.PRESERVE,
            )
            if strategy is MissingValueStrategy.ERROR:
                message = (
                    f"Optional values rejected by policy in "
                    f"{schema.name}.{column.name}: count={missing_count}"
                )
                raise MissingValueError(message)
            if strategy is MissingValueStrategy.DROP_ROW:
                drop_mask |= missing
                continue
            if strategy is MissingValueStrategy.FILL:
                if column.name not in policy.fill_values:
                    message = f"Missing fill value for {schema.name}.{column.name}"
                    raise MissingValueError(message)
                working.loc[missing, column.name] = policy.fill_values[column.name]
                filled_values += missing_count
                continue
            preserved_values += missing_count

        dropped_rows = int(drop_mask.sum())
        if dropped_rows:
            working = working.loc[~drop_mask].copy()

        return MissingValueResult(
            frame=working,
            dropped_rows=dropped_rows,
            filled_values=filled_values,
            preserved_values=preserved_values,
        )

    @staticmethod
    def _missing_mask(series: pd.Series) -> pd.Series:
        """Treat nulls and blank source text as missing for every logical type."""
        missing = series.isna()
        missing |= series.astype("string").str.strip().eq("").fillna(value=True)
        return missing
