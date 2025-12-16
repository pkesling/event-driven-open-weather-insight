"""Utilities to record DAG run outcomes into the warehouse for observability."""
from __future__ import annotations

import os
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from weather_insight.db.session import build_dsn
from weather_insight.utils.logging_utils import get_tagged_logger

logger = get_tagged_logger(__name__, tag="etl_run_logger")
RUN_LOG_TABLE = os.getenv("ETL_RUN_LOG_TABLE", "ops.etl_job_runs")


def _split_table(identifier: str) -> tuple[str | None, str]:
    """Split a possibly schema-qualified table name."""
    if "." in identifier:
        schema, table = identifier.split(".", 1)
        return schema, table
    return None, identifier


def _ensure_table(conn) -> None:
    """Create the run log table if it doesn't already exist."""
    schema, table = _split_table(RUN_LOG_TABLE)
    if schema:
        conn.execute(text(f'create schema if not exists "{schema}"'))
    conn.execute(
        text(
            f"""
            CREATE TABLE IF NOT EXISTS {RUN_LOG_TABLE} (
                dag_id text NOT NULL,
                run_id text NOT NULL,
                execution_date timestamptz,
                start_time timestamptz,
                end_time timestamptz,
                state text NOT NULL,
                error text,
                created_at timestamptz DEFAULT now()
            )
            """
        )
    )


def log_state_from_context(context: dict[str, Any], state: str, error: str | None = None) -> None:
    """Persist DAG run state into the warehouse; best-effort so failures don't break the DAG."""
    engine = create_engine(build_dsn(), future=True)
    dag_run = context.get("dag_run")
    payload = {
        "dag_id": getattr(dag_run, "dag_id", None) or context.get("dag").dag_id,  # type: ignore[attr-defined]
        "run_id": getattr(dag_run, "run_id", None) or context.get("run_id"),
        "execution_date": getattr(dag_run, "execution_date", None) or context.get("execution_date"),
        "start_time": getattr(dag_run, "start_date", None),
        "end_time": getattr(dag_run, "end_date", None),
        "state": state,
        "error": error,
    }

    try:
        with engine.begin() as conn:
            _ensure_table(conn)
            conn.execute(
                text(
                    f"""
                    INSERT INTO {RUN_LOG_TABLE} (
                        dag_id, run_id, execution_date, start_time, end_time, state, error
                    )
                    VALUES (:dag_id, :run_id, :execution_date, :start_time, :end_time, :state, :error)
                    """
                ),
                payload,
            )
    except SQLAlchemyError as exc:  # pragma: no cover - best effort logging
        logger.warning("Failed to write ETL run log: %s", exc)
    finally:
        engine.dispose()


def log_success(context: dict[str, Any]) -> None:
    """Airflow callback for successful DAG runs."""
    log_state_from_context(context, "success")


def log_failure(context: dict[str, Any]) -> None:
    """Airflow callback for failed DAG runs."""
    error = None
    exc = context.get("exception")
    if exc:
        error = str(exc)
    log_state_from_context(context, "failed", error=error)
