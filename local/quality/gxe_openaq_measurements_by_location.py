"""Great Expectations checks for stg.openaq_measurements_by_location."""
from __future__ import annotations

import os
import sys

from pathlib import Path

import great_expectations as gxe
from great_expectations.core import ExpectationSuite

from weather_insight.utils.logging_utils import get_tagged_logger, setup_logging
from weather_insight.quality.gx_utils import build_local_context, build_data_docs
setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
logger = get_tagged_logger(__name__, tag="gxe_openaq_measurements_by_location_dag")


def validate_openaq_measurements_quality() -> None:
    """
    Run Great Expectations checks against stg.openaq_measurements_by_location
    using a Postgres datasource.

    Exits 0 on success, 1 on failure.
    """
    table_name = "openaq_measurements_by_location"
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
    # Ensure we start fresh each run
    try:
        context.suites.delete(suite_name)
    except Exception:
        pass

    # Best-effort: register suite for contexts/tests that track adds
    try:
        context.suites.add(ExpectationSuite(name=suite_name))
    except Exception:
        pass

    # Build a Validator bound to this asset + an in-memory suite
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
        "openaq_locations_id",
        "openaq_sensors_id",
        "datetime_utc",
        "value",
        "source",
        "source_version",
        "event_type",
        "ingested_at_dtz",
    ]:
        validator.expect_column_values_to_not_be_null(col)

    # normalized values (used downstream) should be non-negative when present
    validator.expect_column_values_to_be_between(
        "value_normalized",
        min_value=0.0,
        mostly=1.0,
    )

    # If raw value is negative, it must be flagged invalid
    # is_valid should be either True/False; quality_status limited to known values/null
    validator.expect_column_values_to_be_in_set(
        column="is_valid",
        value_set=[True, False],
    )
    validator.expect_column_values_to_be_in_set(
        column="quality_status",
        value_set=["negative_value", "missing_value", "non_numeric_value", None],
        mostly=1.0,
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
                logger.error(
                    f"Failed: {exp_type} -> {res.result}",
                )
        build_data_docs(context)
        sys.exit(1)

    logger.info(f"All expectations passed for stg.{table_name}")
    build_data_docs(context)
    sys.exit(0)


if __name__ == "__main__":
    validate_openaq_measurements_quality()
