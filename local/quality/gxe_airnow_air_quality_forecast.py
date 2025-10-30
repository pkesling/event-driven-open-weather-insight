"""Great Expectations checks for stg.airnow_air_quality_forecast."""
from __future__ import annotations

import os
import sys

from pathlib import Path

import great_expectations as gxe
from great_expectations.core import ExpectationSuite

from weather_insight.utils.logging_utils import get_tagged_logger, setup_logging
from weather_insight.quality.gx_utils import build_local_context, build_data_docs
setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
logger = get_tagged_logger(__name__, tag="gxe_airnow_quality_forecast")


def validate_air_quality_forecast() -> None:
    """
    Run Great Expectations checks against stg.airnow_air_quality_forecast
    using a Postgres datasource.

    Exits 0 on success, 1 on failure.
    """
    table_name = "airnow_air_quality_forecast"
    # GX_STRICT=0 is a test-only leniency that allows skipping when DSN is absent.
    strict = os.getenv("GX_STRICT", "1") != "0"
    dsn = os.getenv("WAREHOUSE_DB_DSN")
    if not dsn:
        msg = "WAREHOUSE_DB_DSN is not set; cannot run expectations."
        if strict:
            logger.error(msg)
            sys.exit(1)
        logger.warning(msg)
        sys.exit(0)

    # Local filesystem context to generate Data Docs under gx_artifacts/.
    context, _ = build_local_context(Path(__file__).resolve().parent)

    # Define a Postgres datasource backed by your warehouse DSN.
    # Under the hood this still uses SQLAlchemy, but GX owns the engine.
    logger.info("Adding Postgres datasource for warehouse")
    ds = context.data_sources.add_postgres(
        name="warehouse",
        connection_string=dsn,
    )

    # Register the table as an asset
    logger.info(f"Adding table asset '{table_name}'")
    asset = ds.add_table_asset(
        name=f"stg_{table_name}",
        table_name=table_name,
        schema_name="stg",
    )

    batch_request = asset.build_batch_request()

    suite_name = f"{table_name}_suite"
    try:
        context.suites.delete(suite_name)
    except Exception:
        pass

    try:
        context.suites.add(ExpectationSuite(name=suite_name))
    except Exception:
        pass

    try:
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite=ExpectationSuite(name=suite_name),
        )
    except TypeError:
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name,
        )

    # ------------------------------------------------------------------
    # Expectations – tweak as you learn more about the data
    # ------------------------------------------------------------------

    # Core identifiers
    validator.expect_column_values_to_not_be_null("event_id")
    validator.expect_column_values_to_be_unique("event_id")

    for col in [
        "source",
        "date_issue_dtz",
        "date_forecast_dtz",
        "reporting_area",
        "state_code",
        "latitude",
        "longitude",
        "parameter_name",
        "aqi",
        "category_number",
        "category_name",
        "raw_data",
        "effective_start_at_dtz",
    ]:
        validator.expect_column_values_to_not_be_null(col)

    # action_day: allow NULL, but if present must be True/False
    validator.expect_column_values_to_be_in_set(
        "action_day",
        value_set=[True, False, None],
        mostly=1.0,
    )

    # Plausible AQI range (-1 meaning the category_name and category_number indicate the AQI level)
    validator.expect_column_values_to_be_between(
        "aqi",
        min_value=-1,
        max_value=500,
        mostly=0.99,
    )

    # AQI category number should be between 1 and 6 (there being six levels)
    validator.expect_column_values_to_be_between(
        "category_number",
        min_value=1,
        max_value=6,
        mostly=1,
    )

    # AQI category name should be one of the following: Good, Moderate, Unhealthy for Sensitive Groups, Unhealthy, Very Unhealthy, Hazardous
    validator.expect_column_values_to_be_in_set(
        "category_name",
        value_set=["Good", "Moderate", "Unhealthy for Sensitive Groups", "Unhealthy", "Very Unhealthy", "Hazardous"],
        mostly=1,
    )

    # Persist the suite with expectations so Data Docs can render them (if supported)
    try:
        validator.save_expectation_suite(discard_failed_expectations=False)
    except AttributeError:
        pass

    # Run validation
    logger.info(f"Running Great Expectations validation for '{table_name}'")
    result = validator.validate()

    # You *can* pretty-print the result here if you want more detail
    overall_success: bool = bool(result.success)

    if not overall_success:
        logger.error(f"Great Expectations validation FAILED for '{table_name}'")
        # If you want, log individual failed expectations:
        for res in result.results:
            if not res.success:
                cfg = res.expectation_config
                exp_type = (
                    getattr(cfg, "expectation_type", None)
                    or getattr(cfg, "type", None)
                    or str(cfg)
                )
                logger.error(f"Failed: {exp_type} -> {res.result}")
        build_data_docs(context)
        sys.exit(1)

    logger.info(f"All expectations passed for stg.{table_name}")
    build_data_docs(context)
    sys.exit(0)


if __name__ == "__main__":
    validate_air_quality_forecast()
