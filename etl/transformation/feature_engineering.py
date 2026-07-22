"""Point-in-time PySpark features for demand and inventory intelligence."""

from collections.abc import Mapping
from dataclasses import dataclass
from math import tau
from types import MappingProxyType
from typing import Final

from etl.transformation.transform import TransformedDatasetBundle
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F  # noqa: N812
from structlog.stdlib import BoundLogger

FEATURE_KEY_COLUMNS: Final = ("store_id", "product_id")
LAG_VALUE_COLUMNS: Final = ("units_sold", "revenue")
EXTREME_WEATHER_CONDITIONS: Final = (
    "Cold Wave",
    "Heat Wave",
    "Storm",
)
EPOCH_DATE: Final = "1970-01-01"
MINIMUM_STATISTICAL_WINDOW: Final = 2
COMPETITOR_PARITY_LOWER_BOUND: Final = 0.98
COMPETITOR_PARITY_UPPER_BOUND: Final = 1.02
AVERAGE_DAYS_PER_YEAR: Final = 365.25
MONTHS_PER_YEAR: Final = 12.0
HOLIDAY_PROXIMITY_WINDOW_DAYS: Final = 7
PERCENTAGE_SCALE: Final = 100.0


class FeatureEngineeringError(RuntimeError):
    """Raised when a feature configuration or dataset is invalid."""


@dataclass(frozen=True, slots=True)
class FeatureEngineeringConfig:
    """Validated algorithm parameters for point-in-time feature generation."""

    lag_days: tuple[int, ...]
    rolling_windows: tuple[int, ...]
    minimum_history_periods: int
    abc_lookback_days: int
    abc_a_threshold: float
    abc_b_threshold: float
    xyz_lookback_days: int
    xyz_x_threshold: float
    xyz_y_threshold: float
    volatility_low_threshold: float
    volatility_high_threshold: float

    def __post_init__(self) -> None:
        """Reject parameters that would create invalid Spark windows."""
        if not self.lag_days or any(days < 1 for days in self.lag_days):
            message = "lag_days must contain positive integers"
            raise FeatureEngineeringError(message)
        if not self.rolling_windows or any(
            days < MINIMUM_STATISTICAL_WINDOW for days in self.rolling_windows
        ):
            message = "rolling_windows must contain integers greater than one"
            raise FeatureEngineeringError(message)
        if self.minimum_history_periods < MINIMUM_STATISTICAL_WINDOW:
            message = "minimum_history_periods must be at least two"
            raise FeatureEngineeringError(message)
        if not 0 < self.abc_a_threshold < self.abc_b_threshold < 1:
            message = "ABC thresholds must satisfy 0 < A < B < 1"
            raise FeatureEngineeringError(message)
        if not 0 < self.xyz_x_threshold < self.xyz_y_threshold:
            message = "XYZ thresholds must satisfy 0 < X < Y"
            raise FeatureEngineeringError(message)
        if not 0 < self.volatility_low_threshold < self.volatility_high_threshold:
            message = "Volatility thresholds must satisfy 0 < low < high"
            raise FeatureEngineeringError(message)
        if self.abc_lookback_days < self.minimum_history_periods:
            message = "ABC lookback must cover the minimum history period"
            raise FeatureEngineeringError(message)
        if self.xyz_lookback_days < self.minimum_history_periods:
            message = "XYZ lookback must cover the minimum history period"
            raise FeatureEngineeringError(message)


@dataclass(frozen=True, slots=True)
class CuratedDatasetBundle:
    """Immutable named datasets ready for curated-layer persistence."""

    datasets: Mapping[str, DataFrame]

    def get(self, dataset_name: str) -> DataFrame:
        """Return a curated dataset by its stable contract name."""
        try:
            return self.datasets[dataset_name]
        except KeyError as error:
            message = f"Curated dataset bundle does not contain {dataset_name}"
            raise FeatureEngineeringError(message) from error


class RetailFeatureEngineer:
    """Build leakage-aware demand, market, and inventory features in Spark."""

    def __init__(
        self,
        *,
        config: FeatureEngineeringConfig,
        logger: BoundLogger,
    ) -> None:
        """Initialize the feature service with immutable thresholds."""
        self._config = config
        self._logger = logger

    def build(self, transformed: TransformedDatasetBundle) -> CuratedDatasetBundle:
        """Build demand features and retain procurement and reference outputs."""
        demand = transformed.demand_base.withColumn(
            "_etl_day_index",
            F.datediff("date", F.lit(EPOCH_DATE)).cast("long"),
        )
        demand = self._add_calendar_cycle_features(demand)
        demand = self._add_lag_features(demand)
        demand = self._add_rolling_features(demand)
        demand = self._add_holiday_features(demand)
        demand = self._add_weather_features(demand)
        demand = self._add_promotion_features(demand)
        demand = self._add_competitor_pricing_features(demand)
        demand = self._add_inventory_features(demand)
        demand = self._add_abc_classification(demand)
        demand = self._add_xyz_classification(demand)
        demand = self._add_volatility_segmentation(demand).drop("_etl_day_index")

        datasets: dict[str, DataFrame] = {
            "demand_features": demand,
            "purchase_order_features": transformed.purchase_orders,
        }
        datasets.update(transformed.reference_tables)
        self._logger.info(
            "pyspark_feature_graph_built",
            dataset_count=len(datasets),
            lag_days=self._config.lag_days,
            rolling_windows=self._config.rolling_windows,
        )
        return CuratedDatasetBundle(datasets=MappingProxyType(datasets))

    def _add_calendar_cycle_features(self, frame: DataFrame) -> DataFrame:
        day_of_year = F.dayofyear("date").cast("double")
        month = F.month("date").cast("double")
        return (
            frame.withColumn(
                "day_of_year_sin",
                F.sin(F.lit(tau) * day_of_year / F.lit(AVERAGE_DAYS_PER_YEAR)),
            )
            .withColumn(
                "day_of_year_cos",
                F.cos(F.lit(tau) * day_of_year / F.lit(AVERAGE_DAYS_PER_YEAR)),
            )
            .withColumn(
                "month_sin",
                F.sin(F.lit(tau) * month / F.lit(MONTHS_PER_YEAR)),
            )
            .withColumn(
                "month_cos",
                F.cos(F.lit(tau) * month / F.lit(MONTHS_PER_YEAR)),
            )
            .withColumn("is_month_start", F.dayofmonth("date") == 1)
            .withColumn("is_month_end", F.last_day("date") == F.col("date"))
        )

    def _add_lag_features(self, frame: DataFrame) -> DataFrame:
        ordered_history = Window.partitionBy(*FEATURE_KEY_COLUMNS).orderBy("date")
        featured = frame
        for lag_days in self._config.lag_days:
            lagged_date = F.lag("date", lag_days).over(ordered_history)
            exact_calendar_lag = F.datediff("date", lagged_date) == lag_days
            for column_name in LAG_VALUE_COLUMNS:
                lagged_value = F.lag(column_name, lag_days).over(ordered_history)
                featured = featured.withColumn(
                    f"{column_name}_lag_{lag_days}d",
                    F.when(exact_calendar_lag, lagged_value),
                )
        return featured

    def _add_rolling_features(self, frame: DataFrame) -> DataFrame:
        featured = frame
        for window_days in self._config.rolling_windows:
            history = (
                Window.partitionBy(*FEATURE_KEY_COLUMNS)
                .orderBy("_etl_day_index")
                .rangeBetween(-window_days, -1)
            )
            featured = (
                featured.withColumn(
                    f"demand_observation_count_{window_days}d",
                    F.count("units_sold").over(history),
                )
                .withColumn(
                    f"units_sold_rolling_mean_{window_days}d",
                    F.avg("units_sold").over(history),
                )
                .withColumn(
                    f"units_sold_rolling_std_{window_days}d",
                    F.stddev_samp("units_sold").over(history),
                )
            )
        return featured

    def _add_holiday_features(self, frame: DataFrame) -> DataFrame:
        return (
            frame.withColumn(
                "is_pre_holiday_7d",
                F.col("days_to_next_holiday").between(
                    1,
                    HOLIDAY_PROXIMITY_WINDOW_DAYS,
                ),
            )
            .withColumn(
                "is_post_holiday_7d",
                F.col("days_since_previous_holiday").between(
                    1,
                    HOLIDAY_PROXIMITY_WINDOW_DAYS,
                ),
            )
            .withColumn(
                "holiday_proximity_days",
                F.least(
                    F.col("days_to_next_holiday"),
                    F.col("days_since_previous_holiday"),
                ),
            )
        )

    def _add_weather_features(self, frame: DataFrame) -> DataFrame:
        return (
            frame.withColumn(
                "is_precipitation",
                (F.col("weather_rainfall") > 0)
                | F.col("weather_condition").isin("Rainy", "Storm"),
            )
            .withColumn(
                "is_extreme_weather",
                F.col("weather_condition").isin(*EXTREME_WEATHER_CONDITIONS),
            )
            .withColumn(
                "weather_severity_score",
                F.when(
                    F.col("weather_condition").isNull(),
                    F.lit(None).cast("integer"),
                )
                .when(F.col("weather_condition") == "Storm", 3)
                .when(
                    F.col("weather_condition").isin("Cold Wave", "Heat Wave"),
                    2,
                )
                .when(F.col("weather_condition").isin("Fog", "Rainy"), 1)
                .otherwise(0),
            )
            .withColumn(
                "weather_humidity_ratio",
                F.col("weather_humidity") / PERCENTAGE_SCALE,
            )
            .withColumn(
                "temperature_source_delta",
                F.col("weather_temperature") - F.col("sales_temperature"),
            )
        )

    def _add_promotion_features(self, frame: DataFrame) -> DataFrame:
        discount_depth = F.coalesce(
            F.col("promotion_discount_pct"),
            F.col("discount"),
            F.lit(0.0),
        )
        return (
            frame.withColumn(
                "is_promotion",
                F.col("promotion_id").isNotNull() | (F.col("discount") > 0),
            )
            .withColumn("promotion_discount_depth", discount_depth)
            .withColumn(
                "promotion_discount_variance",
                F.col("discount") - F.col("promotion_discount_pct"),
            )
            .withColumn(
                "theoretical_discounted_price",
                F.round(F.col("price") * (1 - discount_depth), 2),
            )
        )

    def _add_competitor_pricing_features(self, frame: DataFrame) -> DataFrame:
        price_gap = F.col("competitor_price") - F.col("price")
        price_index = self._safe_divide(
            F.col("price"),
            F.col("competitor_price"),
        )
        return (
            frame.withColumn("competitor_price_gap", price_gap)
            .withColumn(
                "competitor_price_gap_pct",
                self._safe_divide(price_gap, F.col("competitor_price")),
            )
            .withColumn("competitor_price_index", price_index)
            .withColumn(
                "price_position",
                F.when(price_index.isNull(), F.lit(None).cast("string"))
                .when(
                    price_index < COMPETITOR_PARITY_LOWER_BOUND,
                    "Below_Competitor",
                )
                .when(
                    price_index > COMPETITOR_PARITY_UPPER_BOUND,
                    "Above_Competitor",
                )
                .otherwise("At_Parity"),
            )
            .withColumn(
                "is_price_competitive",
                F.col("price") <= F.col("competitor_price"),
            )
        )

    def _add_inventory_features(self, frame: DataFrame) -> DataFrame:
        demand_window = min(self._config.rolling_windows)
        historical_demand = F.col(f"units_sold_rolling_mean_{demand_window}d")
        available_stock = F.col("opening_stock") + F.col("received_stock")
        average_inventory = (F.col("opening_stock") + F.col("closing_stock")) / 2
        return (
            frame.withColumn("is_stockout", F.col("closing_stock") <= 0)
            .withColumn(
                "is_safety_stock_breach",
                F.col("closing_stock") <= F.col("safety_stock"),
            )
            .withColumn(
                "is_reorder_required",
                F.col("closing_stock") <= F.col("reorder_point"),
            )
            .withColumn(
                "days_of_supply",
                self._safe_divide(F.col("closing_stock"), historical_demand),
            )
            .withColumn(
                "sell_through_rate",
                self._safe_divide(F.col("sold"), available_stock),
            )
            .withColumn(
                "inventory_turnover_proxy",
                self._safe_divide(F.col("units_sold"), average_inventory),
            )
            .withColumn(
                "inventory_position",
                F.col("closing_stock") + F.col("units_ordered"),
            )
        )

    def _add_abc_classification(self, frame: DataFrame) -> DataFrame:
        lookback = self._config.abc_lookback_days
        history = (
            Window.partitionBy(*FEATURE_KEY_COLUMNS)
            .orderBy("_etl_day_index")
            .rangeBetween(-lookback, -1)
        )
        daily_portfolio = Window.partitionBy("date", "store_id")
        ranked_portfolio = (
            Window.partitionBy("date", "store_id")
            .orderBy(
                F.col("_abc_historical_revenue").desc_nulls_last(),
                F.col("product_id"),
            )
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        revenue_name = f"abc_revenue_{lookback}d"
        share_name = f"abc_revenue_share_{lookback}d"
        featured = frame.withColumn(
            "_abc_historical_revenue",
            F.sum("revenue").over(history),
        ).withColumn(
            "_abc_total_revenue",
            F.sum("_abc_historical_revenue").over(daily_portfolio),
        )
        featured = featured.withColumn(
            "_abc_cumulative_revenue",
            F.sum("_abc_historical_revenue").over(ranked_portfolio),
        ).withColumn(
            "_abc_prior_share",
            self._safe_divide(
                F.col("_abc_cumulative_revenue") - F.col("_abc_historical_revenue"),
                F.col("_abc_total_revenue"),
            ),
        )
        return (
            featured.withColumn(revenue_name, F.col("_abc_historical_revenue"))
            .withColumn(
                share_name,
                self._safe_divide(
                    F.col("_abc_historical_revenue"),
                    F.col("_abc_total_revenue"),
                ),
            )
            .withColumn(
                "abc_class",
                F.when(F.col("_abc_prior_share").isNull(), F.lit(None))
                .when(
                    F.col("_abc_prior_share") < self._config.abc_a_threshold,
                    "A",
                )
                .when(
                    F.col("_abc_prior_share") < self._config.abc_b_threshold,
                    "B",
                )
                .otherwise("C"),
            )
            .drop(
                "_abc_historical_revenue",
                "_abc_total_revenue",
                "_abc_cumulative_revenue",
                "_abc_prior_share",
            )
        )

    def _add_xyz_classification(self, frame: DataFrame) -> DataFrame:
        lookback = self._config.xyz_lookback_days
        history = (
            Window.partitionBy(*FEATURE_KEY_COLUMNS)
            .orderBy("_etl_day_index")
            .rangeBetween(-lookback, -1)
        )
        count_name = f"xyz_observation_count_{lookback}d"
        mean_name = f"xyz_demand_mean_{lookback}d"
        std_name = f"xyz_demand_std_{lookback}d"
        cv_name = f"xyz_demand_cv_{lookback}d"
        featured = (
            frame.withColumn(count_name, F.count("units_sold").over(history))
            .withColumn(mean_name, F.avg("units_sold").over(history))
            .withColumn(std_name, F.stddev_samp("units_sold").over(history))
        )
        featured = featured.withColumn(
            cv_name,
            self._safe_divide(F.col(std_name), F.col(mean_name)),
        )
        return featured.withColumn(
            "xyz_class",
            F.when(
                F.col(count_name) < self._config.minimum_history_periods,
                F.lit(None).cast("string"),
            )
            .when(F.col(mean_name) <= 0, "Z")
            .when(F.col(cv_name) <= self._config.xyz_x_threshold, "X")
            .when(F.col(cv_name) <= self._config.xyz_y_threshold, "Y")
            .otherwise("Z"),
        )

    def _add_volatility_segmentation(self, frame: DataFrame) -> DataFrame:
        window_days = max(self._config.rolling_windows)
        count_name = f"demand_observation_count_{window_days}d"
        mean_name = f"units_sold_rolling_mean_{window_days}d"
        std_name = f"units_sold_rolling_std_{window_days}d"
        cv_name = f"demand_cv_{window_days}d"
        featured = frame.withColumn(
            cv_name,
            self._safe_divide(F.col(std_name), F.col(mean_name)),
        )
        return featured.withColumn(
            "volatility_segment",
            F.when(
                F.col(count_name) < self._config.minimum_history_periods,
                "Insufficient_History",
            )
            .when(F.col(mean_name) <= 0, "No_Demand")
            .when(
                F.col(cv_name) <= self._config.volatility_low_threshold,
                "Stable",
            )
            .when(
                F.col(cv_name) <= self._config.volatility_high_threshold,
                "Variable",
            )
            .otherwise("Highly_Variable"),
        )

    @staticmethod
    def _safe_divide(numerator: Column, denominator: Column) -> Column:
        return F.when(
            denominator.isNotNull() & (denominator != 0),
            numerator / denominator,
        )
