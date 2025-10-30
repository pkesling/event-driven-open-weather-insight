"""Upsert helpers for AirNow forecast records."""
from __future__ import annotations

import os
from datetime import datetime
from typing import Iterable, Mapping, Any, Optional, Dict
from sqlalchemy import select
from sqlalchemy.orm import Session

from weather_insight.db.models import StgAirnowAirQualityForecast
from weather_insight.db.utils import utcnow
from weather_insight.utils.logging_utils import get_tagged_logger, setup_logging
setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
logger = get_tagged_logger(__name__, tag="ops_airnow")


class AirNowMissingValue(Exception):
    """Raised when a required value is missing from the AirNow payload."""


# =========================================================
# Minimal helpers to upsert "current" rows (no SCD-2 yet)
# =========================================================

def ensure_airnow_forecast_record(
    session: Session,
    forecast: Mapping[str, Any],
) -> StgAirnowAirQualityForecast:
    """
    Upsert (current row) for an air quality forecast record.
    Expects UTC datetimes for *_dtz fields if provided.
    """
    logger.info(f"Upserting current air quality forecast event '{forecast.get('event_id')}'")
    stmt = (
        select(StgAirnowAirQualityForecast)
        .where(
            StgAirnowAirQualityForecast.event_id == forecast.get("event_id")
        )
        .limit(1)
    )
    row = session.execute(stmt).scalar_one_or_none()

    if row is None:
        logger.info(f"'{forecast.get('event_id')}' is a new air quality forecast.  Creating a new record...")
        row = StgAirnowAirQualityForecast(
            event_id=forecast.get("event_id"),
            source=forecast.get("source"),
            date_issue_dtz=forecast.get("date_issue_dtz"),
            date_forecast_dtz=forecast.get("date_forecast_dtz"),
            reporting_area=forecast.get("reporting_area"),
            state_code=forecast.get("state_code"),
            latitude=forecast.get("latitude"),
            longitude=forecast.get("longitude"),
            parameter_name=forecast.get("parameter_name"),
            aqi=forecast.get("aqi"),
            category_number=forecast.get("category_number"),
            category_name=forecast.get("category_name"),
            action_day=forecast.get("action_day"),
            discussion=forecast.get("discussion"),
            raw_data=forecast.get("raw_data"),
        )

        session.add(row)
        session.flush()  # to get locations_id_sk
    else:
        logger.info(f"'{forecast.get('event_id')}' is an existing air quality forecast.  Updating the existing record.")

        # Overwrite attrs (current-row upsert semantics)
        row.event_id = forecast.get("event_id")
        row.source = forecast.get("source")
        row.date_issue_dtz = forecast.get("date_issue_dtz")
        row.date_forecast_dtz = forecast.get("date_forecast_dtz")
        row.reporting_area = forecast.get("reporting_area")
        row.state_code = forecast.get("state_code")
        row.latitude = forecast.get("latitude")
        row.longitude = forecast.get("longitude")
        row.parameter_name = forecast.get("parameter_name")
        row.aqi = forecast.get("aqi")
        row.category_number = forecast.get("category_number")
        row.category_name = forecast.get("category_name")
        row.action_day = forecast.get("action_day")
        row.discussion = forecast.get("discussion")
        row.raw_data = forecast.get("raw_data")

    return row
