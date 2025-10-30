"""Great Expectations checks for the stg.weather_hourly_forecast table."""
from __future__ import annotations

import os
import sys

from pathlib import Path

import great_expectations as gxe
from great_expectations.core import ExpectationSuite

from weather_insight.utils.logging_utils import get_tagged_logger, setup_logging
from weather_insight.quality.gx_utils import build_local_context, build_data_docs
setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
logger = get_tagged_logger(__name__, tag="gxe_weather_hourly_forecast")

def validate_weather_hourly_forecast() -> None:
    """
    Run Great Expectations checks against stg.weather_hourly_forecast
    using a Postgres datasource.

    Exits 0 on success, 1 on failure.
    """
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
    logger.info("Adding table asset stg.weather_hourly_forecast")
    asset = ds.add_table_asset(
        name="stg_weather_hourly_forecast",
        table_name="weather_hourly_forecast",
        schema_name="stg",
    )

    batch_request = asset.build_batch_request()

    suite_name = "stg_weather_hourly_forecast_suite"
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
        "office",
        "grid_id",
        "grid_x",
        "grid_y",
        "start_time",
        "end_time",
    ]:
        validator.expect_column_values_to_not_be_null(col)

    # is_daytime: allow NULL, but if present must be True/False
    validator.expect_column_values_to_be_in_set(
        "is_daytime",
        value_set=[True, False, None],
        mostly=1.0,
    )

    # Temperature in a plausible Celsius range
    validator.expect_column_values_to_be_between(
        "temperature",
        min_value=-60,
        max_value=60,
        mostly=0.99,
    )

    # Relative humidity 0–100%
    validator.expect_column_values_to_be_between(
        "relative_humidity",
        min_value=0,
        max_value=100,
        mostly=0.99,
    )

    # Probability of precip 0–100%
    validator.expect_column_values_to_be_between(
        "probability_of_precipitation",
        min_value=0,
        max_value=100,
        mostly=0.99,
    )

    # Wind speed (km/h). 0–250 is generous but catches nonsense.
    validator.expect_column_values_to_be_between(
        "wind_speed",
        min_value=0,
        max_value=250,
        mostly=0.99,
    )

    # Persist the suite with expectations so Data Docs can render them (if supported)
    try:
        validator.save_expectation_suite(discard_failed_expectations=False)
    except AttributeError:
        pass

    # Run validation
    logger.info("Running Great Expectations validation for stg.weather_hourly_forecast")
    result = validator.validate()

    # You *can* pretty-print the result here if you want more detail
    overall_success: bool = bool(result.success)

    if not overall_success:
        logger.error("Great Expectations validation FAILED for stg.weather_hourly_forecast")
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

    logger.info("All expectations passed for stg.weather_hourly_forecast")
    build_data_docs(context)
    sys.exit(0)


if __name__ == "__main__":
    validate_weather_hourly_forecast()
