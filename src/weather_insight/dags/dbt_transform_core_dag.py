"""Airflow DAG to run dbt transforms (Great Expectations removed)."""
import os
from airflow.decorators import dag, task
from datetime import datetime, timedelta

try:
    import sqlalchemy as sa  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - Airflow image should have SQLAlchemy
    sa = None

from weather_insight.db.session import build_dsn
from weather_insight.utils.etl_run_logger import log_failure, log_success
from weather_insight.utils.logging_utils import get_tagged_logger, setup_logging

setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
logger = get_tagged_logger(__name__, tag="dbt_transform_core_dag")

DBT_DIR = os.getenv("DBT_DIR", "/opt/app/src/weather_insight/dbt")
ELEMENTARY_SCHEMA = os.getenv("ELEMENTARY_SCHEMA", "ops")
ELEMENTARY_DATABASE = os.getenv("ELEMENTARY_DATABASE", "opendata")

# DAG to run dbt transformations for OpenAQ datasets.
@dag(
    dag_id="dbt_transform_core",
    description="Run dbt transformations for all datasets (quality checks disabled)",
    schedule="5,35 * * * *",  # Run at 5 and 35 minutes past each hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=5)},
    tags=["dbt", "transform", "openaq", "airnow", "weather", "open_meteo"],
    on_success_callback=log_success,
    on_failure_callback=log_failure,
)
def dbt_transform_core():
    """Run dbt transformations for all datasets (GE removed)."""

    @task.bash
    def run_dbt():
        """Run dbt transformations."""
        logger.info("Running dbt transformations")
        return f"""
        set -e

        # Start fresh each time so we don't fight old installs
        rm -rf /tmp/dbt_venv
        python -m venv /tmp/dbt_venv
        source /tmp/dbt_venv/bin/activate

        # Modern pip
        pip install --no-cache-dir --upgrade pip

        # Use a Python-3.12-supported dbt version, and let it resolve click, etc.
        pip install --no-cache-dir 'dbt-core==1.9.0' 'dbt-postgres==1.9.0'

        cd {DBT_DIR}
        export DBT_PROFILES_DIR={DBT_DIR}

        dbt --version
        dbt deps
        dbt build --target docker --no-use-colors
        """

    @task()
    def verify_marts():
        """Fail the DAG if critical mart tables/views are empty after dbt runs."""
        if sa is None:
            raise RuntimeError("sqlalchemy is required to verify mart tables")

        mart_tables = [
            "mart.fct_open_meteo_current_weather_air_conditions",
            "mart.fct_open_meteo_latest_weather_air_forecast",
            "mart.fct_openaq_latest_by_location",
            "mart.fct_air_weather_forecast",
        ]

        engine = sa.create_engine(build_dsn(), future=True)
        with engine.connect() as conn:
            for table in mart_tables:
                count = conn.execute(sa.text(f"select count(*) from {table}")).scalar_one()
                logger.info("Mart %s row count=%s", table, count)
                if count <= 0:
                    raise RuntimeError(f"{table} is empty after dbt build")
        engine.dispose()

    run_dbt() >> verify_marts()

dbt_transform_core()
