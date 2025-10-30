"""Helpers to upsert weather forecast records into staging tables."""
from __future__ import annotations

import os
from typing import Mapping, Any
from sqlalchemy import select
from sqlalchemy.orm import Session

from weather_insight.db.models import StgWeatherHourlyForecast
from weather_insight.db.utils import utcnow
from weather_insight.utils.logging_utils import get_tagged_logger, setup_logging
setup_logging(level=os.getenv("LOG_LEVEL", "DEBUG"))
logger = get_tagged_logger(__name__, tag="ops_weather")


class WeatherMissingValue(Exception):
    """Raised when a required value is missing from the Weather payload."""


def float_or_none(value: Any) -> float | None:
    """Return a float or None if the input is None or empty."""
    if not value:
        return None

    if type(value) is float:
        return value

    if type(value) is str:
        return float(value.strip())

    if type(value) is int:
        return float(value)

    raise ValueError(f"Unexpected type for value '{value}' (type='{type(value)}')")


# =========================================================
# Minimal helpers to upsert "current" rows (no SCD-2 yet)
# =========================================================

def ensure_forecast_record(
    session: Session,
    forecast: Mapping[str, Any],
) -> StgWeatherHourlyForecast:
    """
    Upsert (current row) for an hourly forecast record.
    Expects UTC datetimes for *_dtz fields if provided.
    """
    logger.info(f"Upserting current forecast event '{forecast['event_id']}'")
    stmt = (
        select(StgWeatherHourlyForecast)
        .where(
            StgWeatherHourlyForecast.event_id == forecast["event_id"]
        )
        .limit(1)
    )
    row = session.execute(stmt).scalar_one_or_none()

    if row is None:
        logger.info(f"'{forecast['event_id']}' is a new forecast.  Creating a new record...")
        row = StgWeatherHourlyForecast(
            event_id=forecast["event_id"],
            event_type=forecast["event_type"],
            source=forecast["source"],
            office=forecast["office"],
            grid_id=forecast["grid_id"],
            grid_x=forecast["grid_x"],
            grid_y=forecast["grid_y"],
            start_time=forecast["start_time"],
            end_time=forecast["end_time"],
            is_daytime=forecast.get("is_daytime"),
            temperature=float_or_none(forecast["temperature"]),
            temperature_min=float_or_none(forecast.get("temperature_min")),
            temperature_max=float_or_none(forecast.get("temperature_max")),
            temperature_unit=forecast["temperature_unit"],
            temperature_trend=forecast.get("temperature_trend"),
            relative_humidity=float_or_none(forecast["relative_humidity"]),
            relative_humidity_min=float_or_none(forecast.get("relative_humidity_min")),
            relative_humidity_max=float_or_none(forecast.get("relative_humidity_max")),
            relative_humidity_unit=forecast["relative_humidity_unit"],
            dewpoint=float_or_none(forecast["dewpoint"]),
            dewpoint_min=float_or_none(forecast.get("dewpoint_min")),
            dewpoint_max=float_or_none(forecast.get("dewpoint_max")),
            dewpoint_unit=forecast["dewpoint_unit"],
            wind_speed=float_or_none(forecast["wind_speed"]),
            wind_speed_min=float_or_none(forecast.get("wind_speed_min")),
            wind_speed_max=float_or_none(forecast.get("wind_speed_max")),
            wind_speed_unit=forecast["wind_speed_unit"],
            wind_direction=forecast["wind_direction"],
            wind_gust=float_or_none(forecast["wind_gust"]),
            wind_gust_min=float_or_none(forecast.get("wind_gust_min")),
            wind_gust_max=float_or_none(forecast.get("wind_gust_max")),
            wind_gust_unit=forecast["wind_gust_unit"],
            probability_of_precipitation=float_or_none(forecast["probability_of_precipitation"]),
            probability_of_precipitation_min=float_or_none(forecast.get("probability_of_precipitation_min")),
            probability_of_precipitation_max=float_or_none(forecast.get("probability_of_precipitation_max")),
            probability_of_precipitation_unit=forecast["probability_of_precipitation_unit"],
            short_forecast=forecast.get("short_forecast"),
            detailed_forecast=forecast.get("detailed_forecast"),
            ingested_at_dtz=forecast["ingested_at_dtz"],

        )

        session.add(row)
        session.flush()  # to get locations_id_sk
    else:
        logger.info(f"'{forecast['event_id']}' is an existing forecast.  Updating the existing record.")

        # Overwrite attrs (current-row upsert semantics)
        row.event_id = forecast.get("event_id")
        row.event_type = forecast.get("event_type")
        row.source = forecast.get("source")
        row.office = forecast.get("office")
        row.grid_id = forecast.get("grid_id")
        row.grid_x = forecast.get("grid_x")
        row.grid_y = forecast.get("grid_y")
        row.start_time = forecast.get("start_time")
        row.end_time = forecast.get("end_time")
        row.is_daytime = forecast.get("is_daytime")
        row.temperature = float_or_none(forecast.get("temperature"))
        row.temperature_min = float_or_none(forecast.get("temperature_min")) if forecast.get("temperature_min") else None
        row.temperature_max = float_or_none(forecast.get("temperature_max")) if forecast.get("temperature_max") else None
        row.temperature_unit = forecast.get("temperature_unit")
        row.temperature_trend = forecast.get("temperature_trend")
        row.relative_humidity = float_or_none(forecast.get("relative_humidity"))
        row.relative_humidity_min = float_or_none(forecast.get("relative_humidity_min"))
        row.relative_humidity_max = float_or_none(forecast.get("relative_humidity_max"))
        row.relative_humidity_unit = forecast.get("relative_humidity_unit")
        row.dewpoint = float_or_none(forecast.get("dewpoint"))
        row.dewpoint_min = float_or_none(forecast.get("dewpoint_min"))
        row.dewpoint_max = float_or_none(forecast.get("dewpoint_max"))
        row.dewpoint_unit = forecast.get("dewpoint_unit")
        row.wind_speed = float_or_none(forecast.get("wind_speed"))
        row.wind_speed_min = float_or_none(forecast.get("wind_speed_min"))
        row.wind_speed_max = float_or_none(forecast.get("wind_speed_max"))
        row.wind_speed_unit = forecast.get("wind_speed_unit")
        row.wind_direction = forecast.get("wind_direction")
        row.wind_gust = float_or_none(forecast.get("wind_gust"))
        row.wind_gust_min = float_or_none(forecast.get("wind_gust_min"))
        row.wind_gust_max = float_or_none(forecast.get("wind_gust_max"))
        row.wind_gust_unit = forecast.get("wind_gust_unit")
        row.probability_of_precipitation = float_or_none(forecast.get("probability_of_precipitation"))
        row.probability_of_precipitation_min = float_or_none(forecast.get("probability_of_precipitation_min"))
        row.probability_of_precipitation_max = float_or_none(forecast.get("probability_of_precipitation_max"))
        row.probability_of_precipitation_unit = forecast.get("probability_of_precipitation_unit")
        row.short_forecast = forecast.get("short_forecast")
        row.detailed_forecast = forecast.get("detailed_forecast")
        row.ingested_at_dtz = forecast.get("ingested_at_dtz") or row.ingested_at_dtz
        row.last_updated_at_dtz = utcnow()

    return row
