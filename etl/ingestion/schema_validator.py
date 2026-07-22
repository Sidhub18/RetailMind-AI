"""Retail dataset contracts and schema validation for CSV ingestion."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Final

import numpy as np
import pandas as pd


class LogicalType(StrEnum):
    """Logical data types supported by the ingestion contracts."""

    STRING = "string"
    INTEGER = "integer"
    DECIMAL = "decimal"
    DATE = "date"
    BOOLEAN = "boolean"


@dataclass(frozen=True, slots=True)
class ColumnSchema:
    """Immutable contract for one source column."""

    name: str
    logical_type: LogicalType
    required: bool = True
    minimum: float | None = None
    maximum: float | None = None
    allowed_values: frozenset[str] = frozenset()
    pattern: str | None = None


@dataclass(frozen=True, slots=True)
class TableSchema:
    """Immutable schema and grain definition for one retail table."""

    name: str
    file_name: str
    columns: tuple[ColumnSchema, ...]
    primary_key: tuple[str, ...]
    watermark_column: str | None = None

    @property
    def column_names(self) -> tuple[str, ...]:
        """Return expected source columns in their required order."""
        return tuple(column.name for column in self.columns)

    @property
    def column_map(self) -> Mapping[str, ColumnSchema]:
        """Return column contracts indexed by column name."""
        return MappingProxyType({column.name: column for column in self.columns})


class SchemaValidationError(ValueError):
    """Raised when source structure or values violate a table contract."""


def _string(
    name: str,
    *,
    required: bool = True,
    allowed_values: frozenset[str] = frozenset(),
    pattern: str | None = None,
) -> ColumnSchema:
    return ColumnSchema(
        name=name,
        logical_type=LogicalType.STRING,
        required=required,
        allowed_values=allowed_values,
        pattern=pattern,
    )


def _integer(
    name: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> ColumnSchema:
    return ColumnSchema(
        name=name,
        logical_type=LogicalType.INTEGER,
        minimum=minimum,
        maximum=maximum,
    )


def _decimal(
    name: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> ColumnSchema:
    return ColumnSchema(
        name=name,
        logical_type=LogicalType.DECIMAL,
        minimum=minimum,
        maximum=maximum,
    )


def _date(name: str) -> ColumnSchema:
    return ColumnSchema(name=name, logical_type=LogicalType.DATE)


def _boolean(name: str) -> ColumnSchema:
    return ColumnSchema(
        name=name,
        logical_type=LogicalType.BOOLEAN,
        allowed_values=frozenset({"True", "False"}),
    )


def _build_retail_schemas() -> Mapping[str, TableSchema]:
    schemas = (
        TableSchema(
            name="Fact_Sales",
            file_name="Fact_Sales.csv",
            columns=(
                _date("Date"),
                _string("Store_ID", pattern=r"S\d{3}"),
                _string("Product_ID", pattern=r"P\d{5}"),
                _integer("Inventory_Level", minimum=0),
                _integer("Units_Sold", minimum=0),
                _integer("Units_Ordered", minimum=0),
                _decimal("Price", minimum=0),
                _decimal("Discount", minimum=0, maximum=1),
                _string("Promotion", required=False),
                _string("Holiday", required=False),
                _string(
                    "Weather",
                    allowed_values=frozenset(
                        {
                            "Cloudy",
                            "Cold Wave",
                            "Fog",
                            "Heat Wave",
                            "Rainy",
                            "Storm",
                            "Sunny",
                        }
                    ),
                ),
                _decimal("Competitor_Price", minimum=0),
                _decimal("Temperature"),
                _decimal("Humidity", minimum=0, maximum=100),
                _decimal("Rainfall", minimum=0),
                _decimal("Revenue", minimum=0),
            ),
            primary_key=("Date", "Store_ID", "Product_ID"),
            watermark_column="Date",
        ),
        TableSchema(
            name="Fact_Inventory",
            file_name="Fact_Inventory.csv",
            columns=(
                _date("Date"),
                _string("Store_ID", pattern=r"S\d{3}"),
                _string("Product_ID", pattern=r"P\d{5}"),
                _integer("Opening_Stock", minimum=0),
                _integer("Received_Stock", minimum=0),
                _integer("Sold", minimum=0),
                _integer("Damaged", minimum=0),
                _integer("Closing_Stock", minimum=0),
            ),
            primary_key=("Date", "Store_ID", "Product_ID"),
            watermark_column="Date",
        ),
        TableSchema(
            name="Fact_PurchaseOrders",
            file_name="Fact_PurchaseOrders.csv",
            columns=(
                _string("PO_ID", pattern=r"PO\d{7}"),
                _string("Supplier_ID", pattern=r"SUP\d{3}"),
                _string("Store_ID", pattern=r"S\d{3}"),
                _string("Product_ID", pattern=r"P\d{5}"),
                _date("Order_Date"),
                _date("Expected_Delivery_Date"),
                _integer("Quantity", minimum=1),
                _string(
                    "Status",
                    allowed_values=frozenset({"Delivered", "Pending"}),
                ),
            ),
            primary_key=("PO_ID",),
            watermark_column="Order_Date",
        ),
        TableSchema(
            name="Dim_Product",
            file_name="Dim_Product.csv",
            columns=(
                _string("Product_ID", pattern=r"P\d{5}"),
                _string("Product_Name"),
                _string("Base_Product_Name"),
                _string(
                    "Category",
                    allowed_values=frozenset(
                        {
                            "Clothing",
                            "Electronics",
                            "Furniture",
                            "Groceries",
                            "Toys",
                        }
                    ),
                ),
                _string("Sub_Category"),
                _string("Brand"),
                _decimal("Unit_Cost", minimum=0),
                _decimal("Selling_Price", minimum=0),
                _integer("Safety_Stock", minimum=0),
                _integer("Reorder_Point", minimum=0),
                _integer("Shelf_Life", minimum=0),
                _string("Supplier_ID", pattern=r"SUP\d{3}"),
                _decimal("Weight", minimum=0),
                _decimal("Volume", minimum=0),
            ),
            primary_key=("Product_ID",),
        ),
        TableSchema(
            name="Dim_Store",
            file_name="Dim_Store.csv",
            columns=(
                _string("Store_ID", pattern=r"S\d{3}"),
                _string("Store_Name"),
                _string("City"),
                _string("State"),
                _string(
                    "Region",
                    allowed_values=frozenset(
                        {"Central", "East", "North", "South", "West"}
                    ),
                ),
                _string(
                    "Store_Size",
                    allowed_values=frozenset({"Large", "Medium", "Small"}),
                ),
                _string("Warehouse_ID", pattern=r"WH\d{2}"),
            ),
            primary_key=("Store_ID",),
        ),
        TableSchema(
            name="Dim_Date",
            file_name="Dim_Date.csv",
            columns=(
                _date("Date"),
                _integer("Year"),
                _integer("Month", minimum=1, maximum=12),
                _integer("Day", minimum=1, maximum=31),
                _string(
                    "Weekday",
                    allowed_values=frozenset(
                        {
                            "Friday",
                            "Monday",
                            "Saturday",
                            "Sunday",
                            "Thursday",
                            "Tuesday",
                            "Wednesday",
                        }
                    ),
                ),
                _integer("Quarter", minimum=1, maximum=4),
                _boolean("IsWeekend"),
                _string(
                    "Season",
                    allowed_values=frozenset(
                        {"Monsoon", "Post-Monsoon", "Summer", "Winter"}
                    ),
                ),
            ),
            primary_key=("Date",),
            watermark_column="Date",
        ),
        TableSchema(
            name="Dim_Supplier",
            file_name="Dim_Supplier.csv",
            columns=(
                _string("Supplier_ID", pattern=r"SUP\d{3}"),
                _string("Supplier_Name"),
                _integer("Lead_Time_Days", minimum=1),
                _integer("MOQ", minimum=1),
                _decimal("Reliability", minimum=0, maximum=1),
                _string("Supplier_City"),
                _string("State"),
                _string("Country"),
                _string("Email", pattern=r"[^@\s]+@[^@\s]+\.[^@\s]+"),
                _string("Phone", pattern=r"\+91-\d{10}"),
            ),
            primary_key=("Supplier_ID",),
        ),
        TableSchema(
            name="Dim_Warehouse",
            file_name="Dim_Warehouse.csv",
            columns=(
                _string("Warehouse_ID", pattern=r"WH\d{2}"),
                _string("Warehouse_Name"),
                _string("City"),
                _string("State"),
                _string(
                    "Region",
                    allowed_values=frozenset(
                        {"Central", "East", "North", "South", "West"}
                    ),
                ),
                _integer("Capacity_Units", minimum=1),
            ),
            primary_key=("Warehouse_ID",),
        ),
        TableSchema(
            name="Weather",
            file_name="Weather.csv",
            columns=(
                _date("Date"),
                _string("City"),
                _decimal("Temperature"),
                _decimal("Humidity", minimum=0, maximum=100),
                _decimal("Rainfall", minimum=0),
                _string(
                    "Weather",
                    allowed_values=frozenset(
                        {
                            "Cloudy",
                            "Cold Wave",
                            "Fog",
                            "Heat Wave",
                            "Rainy",
                            "Storm",
                            "Sunny",
                        }
                    ),
                ),
            ),
            primary_key=("Date", "City"),
            watermark_column="Date",
        ),
        TableSchema(
            name="Promotion",
            file_name="Promotion.csv",
            columns=(
                _string("Promotion_ID", pattern=r"PROMO\d{4}"),
                _string("Promotion_Name"),
                _date("Start_Date"),
                _date("End_Date"),
                _decimal("Discount_Pct", minimum=0, maximum=1),
            ),
            primary_key=("Promotion_ID",),
            watermark_column="End_Date",
        ),
        TableSchema(
            name="Holiday",
            file_name="Holiday.csv",
            columns=(
                _date("Date"),
                _string("Holiday_Name"),
            ),
            primary_key=("Date",),
            watermark_column="Date",
        ),
    )
    return MappingProxyType({schema.name: schema for schema in schemas})


RETAIL_TABLE_SCHEMAS: Final[Mapping[str, TableSchema]] = _build_retail_schemas()


class RetailSchemaRegistry:
    """Read-only registry for all validated enterprise retail table schemas."""

    def __init__(
        self,
        schemas: Mapping[str, TableSchema] = RETAIL_TABLE_SCHEMAS,
    ) -> None:
        """Initialize the registry from immutable table contracts."""
        self._schemas = dict(schemas)

    def get(self, table_name: str) -> TableSchema:
        """Return the schema for a table.

        Raises:
            KeyError: If the table is not supported by the ingestion layer.
        """
        if table_name not in self._schemas:
            message = f"Unsupported retail table: {table_name}"
            raise KeyError(message)
        return self._schemas[table_name]

    def tables(self) -> tuple[TableSchema, ...]:
        """Return all table schemas in deterministic contract order."""
        return tuple(self._schemas.values())


class SchemaValidator:
    """Validate CSV headers and cast source values to contracted types."""

    def validate_header(
        self,
        columns: Sequence[str],
        schema: TableSchema,
    ) -> None:
        """Validate exact column names and ordering for a table."""
        actual = tuple(columns)
        expected = schema.column_names
        if actual == expected:
            return

        missing = tuple(column for column in expected if column not in actual)
        unexpected = tuple(column for column in actual if column not in expected)
        message = (
            f"Schema mismatch for {schema.name}: expected={expected}, "
            f"actual={actual}, missing={missing}, unexpected={unexpected}"
        )
        raise SchemaValidationError(message)

    def validate_and_cast(
        self,
        frame: pd.DataFrame,
        schema: TableSchema,
    ) -> pd.DataFrame:
        """Validate a chunk and return values cast to logical contract types."""
        self.validate_header(frame.columns.tolist(), schema)
        cast_frame = frame.copy()
        for column in schema.columns:
            cast_frame[column.name] = self._cast_column(
                cast_frame[column.name],
                column,
                schema.name,
            )
        return cast_frame

    def _cast_column(
        self,
        series: pd.Series,
        column: ColumnSchema,
        table_name: str,
    ) -> pd.Series:
        if column.logical_type is LogicalType.STRING:
            return series.astype("string")
        if column.logical_type is LogicalType.INTEGER:
            return self._cast_integer(series, column.name, table_name)
        if column.logical_type is LogicalType.DECIMAL:
            return self._cast_decimal(series, column.name, table_name)
        if column.logical_type is LogicalType.DATE:
            return self._cast_date(series, column.name, table_name)
        if column.logical_type is LogicalType.BOOLEAN:
            return self._cast_boolean(series, column.name, table_name)

    @staticmethod
    def _cast_integer(
        series: pd.Series,
        column_name: str,
        table_name: str,
    ) -> pd.Series:
        numeric = pd.to_numeric(series, errors="coerce")
        finite = pd.Series(
            np.isfinite(numeric.to_numpy(dtype=float)),
            index=series.index,
        )
        invalid = numeric.isna() | ~finite | numeric.mod(1).ne(0)
        SchemaValidator._raise_if_invalid(
            invalid,
            table_name,
            column_name,
            LogicalType.INTEGER,
        )
        return numeric.astype("Int64")

    @staticmethod
    def _cast_decimal(
        series: pd.Series,
        column_name: str,
        table_name: str,
    ) -> pd.Series:
        numeric = pd.to_numeric(series, errors="coerce")
        finite = pd.Series(
            np.isfinite(numeric.to_numpy(dtype=float)),
            index=series.index,
        )
        SchemaValidator._raise_if_invalid(
            numeric.isna() | ~finite,
            table_name,
            column_name,
            LogicalType.DECIMAL,
        )
        return numeric.astype("Float64")

    @staticmethod
    def _cast_date(
        series: pd.Series,
        column_name: str,
        table_name: str,
    ) -> pd.Series:
        parsed = pd.to_datetime(series, format="%Y-%m-%d", errors="coerce")
        SchemaValidator._raise_if_invalid(
            parsed.isna(),
            table_name,
            column_name,
            LogicalType.DATE,
        )
        return parsed

    @staticmethod
    def _cast_boolean(
        series: pd.Series,
        column_name: str,
        table_name: str,
    ) -> pd.Series:
        normalized = series.astype("string")
        invalid = ~normalized.isin(("True", "False"))
        SchemaValidator._raise_if_invalid(
            invalid,
            table_name,
            column_name,
            LogicalType.BOOLEAN,
        )
        return normalized.map({"True": True, "False": False}).astype("boolean")

    @staticmethod
    def _raise_if_invalid(
        invalid: pd.Series,
        table_name: str,
        column_name: str,
        logical_type: LogicalType,
    ) -> None:
        invalid_count = int(invalid.sum())
        if invalid_count == 0:
            return
        message = (
            f"Invalid {logical_type.value} values in {table_name}.{column_name}: "
            f"count={invalid_count}"
        )
        raise SchemaValidationError(message)
