"""Deterministic PySpark cleaning and retail-domain transformations."""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final

from etl.ingestion.extract import RawDatasetBundle
from etl.ingestion.schema_validator import (
    LogicalType,
    RetailSchemaRegistry,
    TableSchema,
)
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F  # noqa: N812
from structlog.stdlib import BoundLogger

CAMEL_WORD_BOUNDARY: Final = re.compile(r"(.)([A-Z][a-z]+)")
LOWER_UPPER_BOUNDARY: Final = re.compile(r"([a-z0-9])([A-Z])")
MULTIPLE_UNDERSCORES: Final = re.compile(r"_+")


class TransformationError(RuntimeError):
    """Raised when raw datasets cannot be transformed consistently."""


@dataclass(frozen=True, slots=True)
class TransformedDatasetBundle:
    """Cleaned datasets prepared for feature engineering and curation."""

    demand_base: DataFrame
    purchase_orders: DataFrame
    reference_tables: Mapping[str, DataFrame]


class RetailTransformer:
    """Clean, standardize, and enrich the enterprise retail datasets."""

    def __init__(
        self,
        *,
        registry: RetailSchemaRegistry,
        logger: BoundLogger,
    ) -> None:
        """Initialize the transformer with shared schema contracts."""
        self._registry = registry
        self._logger = logger

    def transform(self, raw: RawDatasetBundle) -> TransformedDatasetBundle:
        """Transform all raw tables into demand, procurement, and references."""
        cleaned = {
            schema.name: self._clean_table(raw.get(schema.name), schema)
            for schema in self._registry.tables()
        }
        calendar = self._build_calendar_features(
            cleaned["Dim_Date"],
            cleaned["Holiday"],
        )
        demand_base = self._build_demand_base(cleaned, calendar)
        purchase_orders = self._build_purchase_orders(cleaned)
        references = MappingProxyType(
            {
                "dim_product": cleaned["Dim_Product"].drop("_source_file"),
                "dim_store": cleaned["Dim_Store"].drop("_source_file"),
                "dim_supplier": cleaned["Dim_Supplier"].drop("_source_file"),
                "dim_warehouse": cleaned["Dim_Warehouse"].drop("_source_file"),
                "dim_date": calendar,
                "weather": cleaned["Weather"].drop("_source_file"),
                "promotions": cleaned["Promotion"].drop("_source_file"),
                "holidays": cleaned["Holiday"].drop("_source_file"),
            }
        )
        self._logger.info(
            "pyspark_transformation_graph_built",
            reference_table_count=len(references),
        )
        return TransformedDatasetBundle(
            demand_base=demand_base,
            purchase_orders=purchase_orders,
            reference_tables=references,
        )

    def _clean_table(
        self,
        frame: DataFrame,
        schema: TableSchema,
    ) -> DataFrame:
        renamed_columns = [self._to_snake_case(column) for column in frame.columns]
        cleaned = frame.toDF(*renamed_columns)
        for column in schema.columns:
            if column.logical_type is not LogicalType.STRING:
                continue
            column_name = self._to_snake_case(column.name)
            trimmed = F.trim(F.col(column_name))
            cleaned = cleaned.withColumn(
                column_name,
                F.when(trimmed == "", F.lit(None)).otherwise(trimmed),
            )

        primary_key = [self._to_snake_case(name) for name in schema.primary_key]
        return cleaned.dropDuplicates(primary_key)

    def _build_calendar_features(
        self,
        date_dimension: DataFrame,
        holidays: DataFrame,
    ) -> DataFrame:
        holiday_lookup = holidays.select("date", "holiday_name")
        calendar = date_dimension.drop("_source_file").join(
            holiday_lookup,
            on="date",
            how="left",
        )
        ordered_dates = Window.orderBy("date")
        previous_dates = ordered_dates.rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow,
        )
        next_dates = ordered_dates.rowsBetween(
            Window.currentRow,
            Window.unboundedFollowing,
        )
        calendar = calendar.withColumn(
            "_holiday_date",
            F.when(F.col("holiday_name").isNotNull(), F.col("date")),
        )
        calendar = calendar.withColumn(
            "_previous_holiday_date",
            F.last("_holiday_date", ignorenulls=True).over(previous_dates),
        ).withColumn(
            "_next_holiday_date",
            F.first("_holiday_date", ignorenulls=True).over(next_dates),
        )
        return (
            calendar.withColumn(
                "is_holiday",
                F.col("holiday_name").isNotNull(),
            )
            .withColumn(
                "days_since_previous_holiday",
                F.datediff("date", "_previous_holiday_date"),
            )
            .withColumn(
                "days_to_next_holiday",
                F.datediff("_next_holiday_date", "date"),
            )
            .drop(
                "_holiday_date",
                "_previous_holiday_date",
                "_next_holiday_date",
            )
        )

    def _build_demand_base(
        self,
        cleaned: Mapping[str, DataFrame],
        calendar: DataFrame,
    ) -> DataFrame:
        sales = cleaned["Fact_Sales"].select(
            "date",
            "store_id",
            "product_id",
            "inventory_level",
            "units_sold",
            "units_ordered",
            "price",
            "discount",
            F.col("promotion").alias("sales_promotion"),
            F.col("holiday").alias("sales_holiday"),
            F.col("weather").alias("sales_weather"),
            "competitor_price",
            F.col("temperature").alias("sales_temperature"),
            F.col("humidity").alias("sales_humidity"),
            F.col("rainfall").alias("sales_rainfall"),
            "revenue",
        )
        inventory = cleaned["Fact_Inventory"].drop("_source_file")
        product = cleaned["Dim_Product"].drop("_source_file")
        store = (
            cleaned["Dim_Store"]
            .drop("_source_file")
            .select(
                "store_id",
                "store_name",
                F.col("city").alias("store_city"),
                F.col("state").alias("store_state"),
                F.col("region").alias("store_region"),
                "store_size",
                "warehouse_id",
            )
        )
        supplier = (
            cleaned["Dim_Supplier"]
            .drop("_source_file")
            .select(
                "supplier_id",
                "supplier_name",
                "lead_time_days",
                "moq",
                "reliability",
                F.col("supplier_city"),
                F.col("state").alias("supplier_state"),
                "country",
            )
        )
        warehouse = (
            cleaned["Dim_Warehouse"]
            .drop("_source_file")
            .select(
                "warehouse_id",
                "warehouse_name",
                F.col("city").alias("warehouse_city"),
                F.col("state").alias("warehouse_state"),
                F.col("region").alias("warehouse_region"),
                "capacity_units",
            )
        )
        weather = (
            cleaned["Weather"]
            .drop("_source_file")
            .select(
                F.col("date").alias("weather_date"),
                F.col("city").alias("weather_city"),
                F.col("temperature").alias("weather_temperature"),
                F.col("humidity").alias("weather_humidity"),
                F.col("rainfall").alias("weather_rainfall"),
                F.col("weather").alias("weather_condition"),
            )
        )
        promotions = (
            cleaned["Promotion"]
            .drop("_source_file")
            .select(
                "promotion_id",
                "promotion_name",
                "start_date",
                "end_date",
                F.col("discount_pct").alias("promotion_discount_pct"),
            )
        )

        enriched = (
            sales.join(
                inventory,
                on=["date", "store_id", "product_id"],
                how="left",
            )
            .join(product, on="product_id", how="left")
            .join(store, on="store_id", how="left")
            .join(supplier, on="supplier_id", how="left")
            .join(warehouse, on="warehouse_id", how="left")
            .join(calendar, on="date", how="left")
        )
        enriched = enriched.join(
            weather,
            on=(enriched["date"] == weather["weather_date"])
            & (enriched["store_city"] == weather["weather_city"]),
            how="left",
        ).drop("weather_date", "weather_city")
        enriched = enriched.join(
            promotions,
            on=(enriched["sales_promotion"] == promotions["promotion_name"])
            & enriched["date"].between(
                promotions["start_date"],
                promotions["end_date"],
            ),
            how="left",
        )
        promotion_match = Window.partitionBy(
            "date",
            "store_id",
            "product_id",
        ).orderBy(
            F.col("start_date").desc_nulls_last(),
            F.col("promotion_id"),
        )
        enriched = (
            enriched.withColumn(
                "_promotion_match_rank",
                F.row_number().over(promotion_match),
            )
            .filter(F.col("_promotion_match_rank") == 1)
            .drop("_promotion_match_rank")
        )
        return (
            enriched.withColumn(
                "inventory_snapshot_matches",
                self._null_safe_equal(
                    F.col("inventory_level"),
                    F.col("closing_stock"),
                ),
            )
            .withColumn(
                "holiday_label_matches",
                self._null_safe_equal(
                    F.col("sales_holiday"),
                    F.col("holiday_name"),
                ),
            )
            .withColumn(
                "weather_label_matches",
                self._null_safe_equal(
                    F.col("sales_weather"),
                    F.col("weather_condition"),
                ),
            )
            .withColumn(
                "promotion_label_matches",
                self._null_safe_equal(
                    F.col("sales_promotion"),
                    F.col("promotion_name"),
                ),
            )
        )

    def _build_purchase_orders(
        self,
        cleaned: Mapping[str, DataFrame],
    ) -> DataFrame:
        product = cleaned["Dim_Product"].select(
            "product_id",
            "product_name",
            "category",
            "sub_category",
            "brand",
            "unit_cost",
            "selling_price",
        )
        supplier = cleaned["Dim_Supplier"].select(
            "supplier_id",
            "supplier_name",
            "lead_time_days",
            "moq",
            "reliability",
        )
        store = cleaned["Dim_Store"].select(
            "store_id",
            "store_name",
            F.col("city").alias("store_city"),
            F.col("region").alias("store_region"),
            "warehouse_id",
        )
        orders = (
            cleaned["Fact_PurchaseOrders"]
            .drop("_source_file")
            .join(product, on="product_id", how="left")
            .join(supplier, on="supplier_id", how="left")
            .join(store, on="store_id", how="left")
        )
        return (
            orders.withColumn(
                "expected_lead_time_days",
                F.datediff("expected_delivery_date", "order_date"),
            )
            .withColumn(
                "lead_time_variance_days",
                F.col("expected_lead_time_days") - F.col("lead_time_days"),
            )
            .withColumn(
                "order_value_at_cost",
                F.round(F.col("quantity") * F.col("unit_cost"), 2),
            )
            .withColumn("is_pending", F.col("status") == "Pending")
            .withColumn("is_delivered", F.col("status") == "Delivered")
            .withColumn("order_year", F.year("order_date"))
            .withColumn("order_month", F.month("order_date"))
        )

    @staticmethod
    def _null_safe_equal(left: Column, right: Column) -> Column:
        return left.eqNullSafe(right)

    @staticmethod
    def _to_snake_case(value: str) -> str:
        normalized = value.replace("-", "_").replace(" ", "_")
        normalized = CAMEL_WORD_BOUNDARY.sub(r"\1_\2", normalized)
        normalized = LOWER_UPPER_BOUNDARY.sub(r"\1_\2", normalized)
        return MULTIPLE_UNDERSCORES.sub("_", normalized).casefold()
