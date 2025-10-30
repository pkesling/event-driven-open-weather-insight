from __future__ import annotations

import os
from typing import Any, Mapping

from sqlalchemy import select
from sqlalchemy.orm import Session

from weather_insight.db.models import StgOpenMeteoAir, StgOpenMeteoWeather
from weather_insight.db.utils import utcnow
from weather_insight.utils.logging_utils import get_tagged_logger, setup_logging

setup_logging(level=os.getenv("LOG_LEVEL", "DEBUG"))
logger = get_tagged_logger(__name__, tag="ops_open_meteo")


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def ensure_open_meteo_weather_record(
    session: Session,
    record: Mapping[str, Any],
) -> StgOpenMeteoWeather:
    """
    Upsert (current row) for an Open-Meteo weather record.
    Expects UTC datetimes for *_time fields if provided.
    """
    logger.info("Upserting open meteo weather record")
    stmt = select(StgOpenMeteoWeather).where(
        StgOpenMeteoWeather.event_id == record["event_id"]
    ).limit(1)
    row = session.execute(stmt).scalar_one_or_none()

    if row is None:
        logger.info(
            "'%s' is a new open meteo weather record. Creating a new record...",
            record["event_id"],
        )
        row = StgOpenMeteoWeather(
            event_id=record["event_id"],
            open_meteo_start_time=record["open_meteo_start_time"],
            open_meteo_end_time=record["open_meteo_end_time"],
            record_type=record["record_type"],
            record_frequency_min=record["record_frequency_min"],
            latitude=record["latitude"],
            longitude=record["longitude"],
            temperature=_float_or_none(record.get("temperature")),
            temperature_unit=record.get("temperature_unit"),
            rel_humidity=_float_or_none(record.get("rel_humidity")),
            rel_humidity_unit=record.get("rel_humidity_unit"),
            dew_point=_float_or_none(record.get("dew_point")),
            dew_point_unit=record.get("dew_point_unit"),
            apparent_temperature=_float_or_none(record.get("apparent_temperature")),
            apparent_temperature_unit=record.get("apparent_temperature_unit"),
            precipitation_prob=_float_or_none(record.get("precipitation_prob")),
            precipitation_prob_unit=record.get("precipitation_prob_unit"),
            precipitation=_float_or_none(record.get("precipitation")),
            precipitation_unit=record.get("precipitation_unit"),
            cloud_cover=_float_or_none(record.get("cloud_cover")),
            cloud_cover_unit=record.get("cloud_cover_unit"),
            wind_speed=_float_or_none(record.get("wind_speed")),
            wind_speed_unit=record.get("wind_speed_unit"),
            wind_gusts=_float_or_none(record.get("wind_gusts")),
            wind_gusts_unit=record.get("wind_gusts_unit"),
            wind_direction=_float_or_none(record.get("wind_direction")),
            wind_direction_unit=record.get("wind_direction_unit"),
            is_day=record.get("is_day"),
            ingested_at_dtz=record.get("ingested_at_dtz") or utcnow(),
        )
        session.add(row)
        session.flush()
    else:
        logger.info(
            "'%s' is an existing open meteo weather record. Updating the existing record.",
            record["event_id"],
        )

        row.open_meteo_start_time = record.get("open_meteo_start_time")
        row.open_meteo_end_time = record.get("open_meteo_end_time")
        row.record_type = record.get("record_type")
        row.record_frequency_min = record.get("record_frequency_min")
        row.latitude = record.get("latitude")
        row.longitude = record.get("longitude")
        row.temperature = _float_or_none(record.get("temperature"))
        row.temperature_unit = record.get("temperature_unit")
        row.rel_humidity = _float_or_none(record.get("rel_humidity"))
        row.rel_humidity_unit = record.get("rel_humidity_unit")
        row.dew_point = _float_or_none(record.get("dew_point"))
        row.dew_point_unit = record.get("dew_point_unit")
        row.apparent_temperature = _float_or_none(record.get("apparent_temperature"))
        row.apparent_temperature_unit = record.get("apparent_temperature_unit")
        row.precipitation_prob = _float_or_none(record.get("precipitation_prob"))
        row.precipitation_prob_unit = record.get("precipitation_prob_unit")
        row.precipitation = _float_or_none(record.get("precipitation"))
        row.precipitation_unit = record.get("precipitation_unit")
        row.cloud_cover = _float_or_none(record.get("cloud_cover"))
        row.cloud_cover_unit = record.get("cloud_cover_unit")
        row.wind_speed = _float_or_none(record.get("wind_speed"))
        row.wind_speed_unit = record.get("wind_speed_unit")
        row.wind_gusts = _float_or_none(record.get("wind_gusts"))
        row.wind_gusts_unit = record.get("wind_gusts_unit")
        row.wind_direction = _float_or_none(record.get("wind_direction"))
        row.wind_direction_unit = record.get("wind_direction_unit")
        row.is_day = record.get("is_day")
        row.ingested_at_dtz = record.get("ingested_at_dtz") or row.ingested_at_dtz
        row.last_updated_at_dtz = utcnow()

    return row


def ensure_open_meteo_air_record(
    session: Session,
    record: Mapping[str, Any],
) -> StgOpenMeteoAir:
    """
    Upsert (current row) for an Open-Meteo air quality record.
    Expects UTC datetimes for *_time fields if provided.
    """
    logger.info("Upserting open meteo air quality record")
    stmt = select(StgOpenMeteoAir).where(
        StgOpenMeteoAir.event_id == record["event_id"]
    ).limit(1)
    row = session.execute(stmt).scalar_one_or_none()

    if row is None:
        logger.info(
            "'%s' is a new open meteo air quality record. Creating a new record...",
            record["event_id"],
        )
        row = StgOpenMeteoAir(
            event_id=record["event_id"],
            open_meteo_start_time=record["open_meteo_start_time"],
            open_meteo_end_time=record["open_meteo_end_time"],
            record_type=record["record_type"],
            record_frequency_min=record["record_frequency_min"],
            latitude=record["latitude"],
            longitude=record["longitude"],
            pm2_5=_float_or_none(record.get("pm2_5")),
            pm2_5_unit=record.get("pm2_5_unit"),
            pm10=_float_or_none(record.get("pm10")),
            pm10_unit=record.get("pm10_unit"),
            us_aqi=_float_or_none(record.get("us_aqi")),
            us_aqi_unit=record.get("us_aqi_unit"),
            ozone=_float_or_none(record.get("ozone")),
            ozone_unit=record.get("ozone_unit"),
            uv_index=_float_or_none(record.get("uv_index")),
            uv_index_unit=record.get("uv_index_unit"),
            ingested_at_dtz=record.get("ingested_at_dtz") or utcnow(),
        )
        session.add(row)
        session.flush()
    else:
        logger.info(
            "'%s' is an existing open meteo air quality record. Updating the existing record.",
            record["event_id"],
        )
        row.open_meteo_start_time = record.get("open_meteo_start_time")
        row.open_meteo_end_time = record.get("open_meteo_end_time")
        row.record_type = record.get("record_type")
        row.record_frequency_min = record.get("record_frequency_min")
        row.latitude = record.get("latitude")
        row.longitude = record.get("longitude")
        row.pm2_5 = _float_or_none(record.get("pm2_5"))
        row.pm2_5_unit = record.get("pm2_5_unit")
        row.pm10 = _float_or_none(record.get("pm10"))
        row.pm10_unit = record.get("pm10_unit")
        row.us_aqi = _float_or_none(record.get("us_aqi"))
        row.us_aqi_unit = record.get("us_aqi_unit")
        row.ozone = _float_or_none(record.get("ozone"))
        row.ozone_unit = record.get("ozone_unit")
        row.uv_index = _float_or_none(record.get("uv_index"))
        row.uv_index_unit = record.get("uv_index_unit")
        row.ingested_at_dtz = record.get("ingested_at_dtz") or row.ingested_at_dtz
        row.last_updated_at_dtz = utcnow()

    return row
