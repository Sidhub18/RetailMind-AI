"""Data-quality rules and reports for validated ingestion chunks."""

from dataclasses import dataclass
from enum import StrEnum

import numpy as np
import pandas as pd
from etl.ingestion.schema_validator import LogicalType, TableSchema


class QualitySeverity(StrEnum):
    """Severity assigned to a data-quality issue."""

    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class DataQualityIssue:
    """One aggregated data-quality rule violation."""

    rule: str
    severity: QualitySeverity
    failed_rows: int
    message: str


@dataclass(frozen=True, slots=True)
class DataQualityReport:
    """Data-quality outcome for one table chunk."""

    table_name: str
    evaluated_rows: int
    issues: tuple[DataQualityIssue, ...]

    @property
    def has_errors(self) -> bool:
        """Return whether the report contains a blocking issue."""
        return any(
            issue.severity is QualitySeverity.ERROR and issue.failed_rows > 0
            for issue in self.issues
        )

    def raise_for_errors(self) -> None:
        """Raise a consolidated exception for blocking quality issues."""
        if not self.has_errors:
            return
        summary = "; ".join(
            f"{issue.rule}={issue.failed_rows}"
            for issue in self.issues
            if issue.severity is QualitySeverity.ERROR
        )
        message = f"Data-quality validation failed for {self.table_name}: {summary}"
        raise DataQualityError(message, self)


class DataQualityError(ValueError):
    """Raised when a chunk violates blocking data-quality rules."""

    def __init__(self, message: str, report: DataQualityReport) -> None:
        """Initialize the exception with its complete quality report."""
        super().__init__(message)
        self.report = report


class DataQualityValidator:
    """Evaluate generic column constraints and table-specific invariants."""

    def validate(
        self,
        frame: pd.DataFrame,
        schema: TableSchema,
    ) -> DataQualityReport:
        """Evaluate all configured quality rules for one chunk."""
        issues = [*self._validate_column_rules(frame, schema)]
        issues.extend(self._validate_business_rules(frame, schema.name))
        return DataQualityReport(
            table_name=schema.name,
            evaluated_rows=len(frame),
            issues=tuple(issues),
        )

    def _validate_column_rules(
        self,
        frame: pd.DataFrame,
        schema: TableSchema,
    ) -> list[DataQualityIssue]:
        issues: list[DataQualityIssue] = []
        for column in schema.columns:
            series = frame[column.name]
            missing = series.isna()
            if column.logical_type is LogicalType.STRING:
                missing |= series.astype("string").str.strip().eq("").fillna(value=True)
            if column.required:
                self._append_issue(
                    issues,
                    rule=f"{column.name}.required",
                    mask=missing,
                    message=f"{column.name} must be populated",
                )

            populated = ~missing
            if column.minimum is not None:
                self._append_issue(
                    issues,
                    rule=f"{column.name}.minimum",
                    mask=populated & series.lt(column.minimum),
                    message=f"{column.name} must be >= {column.minimum}",
                )
            if column.maximum is not None:
                self._append_issue(
                    issues,
                    rule=f"{column.name}.maximum",
                    mask=populated & series.gt(column.maximum),
                    message=f"{column.name} must be <= {column.maximum}",
                )
            if column.allowed_values:
                values = series.astype("string")
                self._append_issue(
                    issues,
                    rule=f"{column.name}.domain",
                    mask=populated & ~values.isin(column.allowed_values),
                    message=f"{column.name} contains an unsupported value",
                )
            if column.pattern is not None:
                values = series.astype("string")
                matches = values.str.fullmatch(column.pattern, na=False)
                self._append_issue(
                    issues,
                    rule=f"{column.name}.format",
                    mask=populated & ~matches,
                    message=f"{column.name} violates its identifier format",
                )
        return issues

    def _validate_business_rules(
        self,
        frame: pd.DataFrame,
        table_name: str,
    ) -> list[DataQualityIssue]:
        issues: list[DataQualityIssue] = []
        if table_name == "Fact_Sales":
            expected_revenue = frame["Units_Sold"] * frame["Price"]
            matches = np.isclose(
                frame["Revenue"].to_numpy(dtype=float),
                expected_revenue.to_numpy(dtype=float),
                rtol=0,
                atol=0.011,
            )
            self._append_issue(
                issues,
                rule="sales.revenue_calculation",
                mask=pd.Series(~matches, index=frame.index),
                message="Revenue must equal Units_Sold multiplied by Price",
            )
            promotion_missing = frame["Promotion"].astype("string").str.strip().eq("")
            self._append_issue(
                issues,
                rule="sales.discount_requires_promotion",
                mask=frame["Discount"].gt(0) & promotion_missing,
                message="Positive discounts require a promotion label",
            )
            self._append_issue(
                issues,
                rule="sales.promotion_requires_discount",
                mask=frame["Discount"].eq(0) & ~promotion_missing,
                message="Promotion labels require a positive discount",
            )
            self._append_issue(
                issues,
                rule="sales.positive_price",
                mask=frame["Price"].le(0),
                message="Price must be positive",
            )
            self._append_issue(
                issues,
                rule="sales.positive_competitor_price",
                mask=frame["Competitor_Price"].le(0),
                message="Competitor_Price must be positive",
            )
        elif table_name == "Fact_Inventory":
            expected_closing = frame["Opening_Stock"] - frame["Sold"] - frame["Damaged"]
            self._append_issue(
                issues,
                rule="inventory.stock_balance",
                mask=frame["Closing_Stock"].ne(expected_closing),
                message=(
                    "Closing_Stock must equal Opening_Stock minus Sold and Damaged"
                ),
            )
        elif table_name == "Fact_PurchaseOrders":
            self._append_issue(
                issues,
                rule="purchase_order.delivery_sequence",
                mask=frame["Expected_Delivery_Date"].lt(frame["Order_Date"]),
                message="Expected delivery cannot precede the order date",
            )
        elif table_name == "Dim_Product":
            for column_name in (
                "Unit_Cost",
                "Selling_Price",
                "Weight",
                "Volume",
            ):
                self._append_issue(
                    issues,
                    rule=f"product.{column_name}.positive",
                    mask=frame[column_name].le(0),
                    message=f"{column_name} must be positive",
                )
            self._append_issue(
                issues,
                rule="product.positive_margin",
                mask=frame["Selling_Price"].lt(frame["Unit_Cost"]),
                message="Selling_Price cannot be below Unit_Cost",
            )
            self._append_issue(
                issues,
                rule="product.reorder_threshold",
                mask=frame["Reorder_Point"].lt(frame["Safety_Stock"]),
                message="Reorder_Point cannot be below Safety_Stock",
            )
        elif table_name == "Dim_Date":
            self._validate_date_derivations(frame, issues)
        elif table_name == "Promotion":
            self._append_issue(
                issues,
                rule="promotion.date_sequence",
                mask=frame["End_Date"].lt(frame["Start_Date"]),
                message="Promotion end date cannot precede its start date",
            )
        return issues

    def _validate_date_derivations(
        self,
        frame: pd.DataFrame,
        issues: list[DataQualityIssue],
    ) -> None:
        dates = frame["Date"].dt
        checks = (
            ("date.year", frame["Year"].ne(dates.year)),
            ("date.month", frame["Month"].ne(dates.month)),
            ("date.day", frame["Day"].ne(dates.day)),
            ("date.weekday", frame["Weekday"].ne(dates.day_name())),
            ("date.quarter", frame["Quarter"].ne(dates.quarter)),
            ("date.weekend", frame["IsWeekend"].ne(dates.dayofweek.ge(5))),
        )
        for rule, mask in checks:
            self._append_issue(
                issues,
                rule=rule,
                mask=mask,
                message=f"Derived date attribute failed: {rule}",
            )
        expected_season = frame["Month"].map(
            {
                1: "Winter",
                2: "Winter",
                3: "Summer",
                4: "Summer",
                5: "Summer",
                6: "Monsoon",
                7: "Monsoon",
                8: "Monsoon",
                9: "Monsoon",
                10: "Post-Monsoon",
                11: "Post-Monsoon",
                12: "Winter",
            }
        )
        self._append_issue(
            issues,
            rule="date.season",
            mask=frame["Season"].ne(expected_season),
            message="Season must match the validated month-to-season mapping",
        )

    @staticmethod
    def _append_issue(
        issues: list[DataQualityIssue],
        *,
        rule: str,
        mask: pd.Series,
        message: str,
        severity: QualitySeverity = QualitySeverity.ERROR,
    ) -> None:
        failed_rows = int(mask.fillna(value=True).sum())
        if failed_rows == 0:
            return
        issues.append(
            DataQualityIssue(
                rule=rule,
                severity=severity,
                failed_rows=failed_rows,
                message=message,
            )
        )
