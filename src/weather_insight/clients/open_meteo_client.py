"""Helpers for fetching weather and air-quality data from the Open-Meteo APIs."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from math import ceil
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import requests

from weather_insight.clients.http_session import configure_session
from weather_insight.utils.logging_utils import get_tagged_logger

logger = get_tagged_logger(__name__, tag="open_meteo_client")

OPEN_METEO_WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_AIR_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
DEFAULT_TIMEOUT = 15
DEFAULT_RETRIES = 3
DEFAULT_BACKOFF = 0.2
DEFAULT_FORECAST_HOURS = 48
DEFAULT_TZ = "UTC"


class OpenMeteoClientError(RuntimeError):
    """Raised when Open-Meteo requests fail or return unexpected data."""


@dataclass
class OpenMeteoClientConfig:
    """Configuration for OpenMeteoClient."""

    weather_url: str = OPEN_METEO_WEATHER_URL
    air_url: str = OPEN_METEO_AIR_URL
    timeout_seconds: int = DEFAULT_TIMEOUT
    retries: int = DEFAULT_RETRIES
    retry_backoff_seconds: float = DEFAULT_BACKOFF


def _iso_to_dt_with_tz(s: str, tz_name: str) -> dt.datetime:
    """Interpret Open-Meteo local time string as being in tz_name."""
    naive = dt.datetime.fromisoformat(s)
    return naive.replace(tzinfo=ZoneInfo(tz_name))


def _to_utc(dt_value: dt.datetime) -> dt.datetime:
    """Normalize a timezone-aware datetime to UTC."""
    if dt_value.tzinfo is None:
        raise ValueError("Datetime must be timezone-aware")
    return dt_value.astimezone(dt.timezone.utc)


def _isoformat_utc(dt_value: dt.datetime) -> str:
    """Return an ISO 8601 string in UTC with 'Z' suffix."""
    return _to_utc(dt_value).isoformat().replace("+00:00", "Z")


def _list_lookup(data: Dict[str, Any], key: str, idx: int):
    """Safely read list-style values from the API payload."""
    values = (data or {}).get(key)
    if values is None or idx >= len(values):
        return None
    return values[idx]


def _as_bool(value) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    try:
        return bool(int(value))
    except (TypeError, ValueError):
        return None


def _build_event_id(
    kind: str,
    latitude: float,
    longitude: float,
    start_time: dt.datetime,
    *,
    record_type: str,
) -> str:
    """Consistent event_id for weather/air records, keyed by record_type to avoid overwrites."""
    return f"open-meteo:{kind}:{record_type}:{latitude:.4f}:{longitude:.4f}:{_isoformat_utc(start_time)}"


class OpenMeteoClient:
    """Small wrapper around the Open-Meteo weather + air-quality endpoints."""

    def __init__(
        self,
        config: OpenMeteoClientConfig,
        session: Optional[requests.Session] = None,
    ) -> None:
        self._config = config
        self._session = configure_session(
            session or requests.Session(),
            headers={"User-Agent": "weather-insight/open-meteo"},
            timeout_seconds=self._config.timeout_seconds,
            retries=self._config.retries,
            retry_backoff_seconds=self._config.retry_backoff_seconds,
        )

    # ------------------------------------------------------------------ #
    # HTTP fetchers                                                      #
    # ------------------------------------------------------------------ #
    def fetch_weather(
        self,
        latitude: float,
        longitude: float,
        *,
        timezone: str = DEFAULT_TZ,
        forecast_hours: int = DEFAULT_FORECAST_HOURS,
        temperature_unit: str = "fahrenheit",
        wind_speed_unit: str = "mph",
        precipitation_unit: str = "mm",
        include_current: bool = True,
    ) -> Dict[str, Any]:
        """Fetch hourly weather (and optional current conditions)."""
        hourly_vars = [
            "temperature_2m",
            "relative_humidity_2m",
            "dew_point_2m",
            "apparent_temperature",
            "precipitation_probability",
            "precipitation",
            "cloud_cover",
            "wind_speed_10m",
            "wind_gusts_10m",
            "wind_direction_10m",
            "is_day",
        ]
        current_vars = [
            "temperature_2m",
            "relative_humidity_2m",
            "apparent_temperature",
            "is_day",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
            "precipitation",
            "cloud_cover",
        ]
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": ",".join(hourly_vars),
            "forecast_days": max(1, ceil(forecast_hours / 24)),
            "timezone": timezone,
            "temperature_unit": temperature_unit,
            "wind_speed_unit": wind_speed_unit,
            "precipitation_unit": precipitation_unit,
        }
        if include_current:
            params["current"] = ",".join(current_vars)

        return self._get(self._config.weather_url, params)

    def fetch_air_quality(
        self,
        latitude: float,
        longitude: float,
        *,
        timezone: str = DEFAULT_TZ,
        forecast_hours: int = DEFAULT_FORECAST_HOURS,
        include_current: bool = True,
    ) -> Dict[str, Any]:
        """Fetch hourly air-quality (and optional current conditions)."""
        hourly_vars = ["pm2_5", "pm10", "ozone", "uv_index", "us_aqi"]
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": ",".join(hourly_vars),
            "forecast_days": max(1, ceil(forecast_hours / 24)),
            "timezone": timezone,
        }
        if include_current:
            params["current"] = ",".join(hourly_vars)

        return self._get(self._config.air_url, params)

    # ------------------------------------------------------------------ #
    # Event builders                                                     #
    # ------------------------------------------------------------------ #
    def build_weather_events(
        self,
        latitude: float,
        longitude: float,
        raw: Dict[str, Any],
        *,
        timezone: str,
        record_type: str = "forecast",
        ingest_dt: Optional[dt.datetime] = None,
        record_frequency_min: int = 60,
    ) -> List[Dict[str, Any]]:
        """Normalize hourly/current weather payloads into Kafka-ready events."""
        ingest_dt = ingest_dt or dt.datetime.now(tz=dt.timezone.utc)
        hourly = raw.get("hourly") or {}
        hourly_units = raw.get("hourly_units") or {}
        times = hourly.get("time") or []

        events: List[Dict[str, Any]] = []
        if raw.get("current"):
            events.append(
                self._build_current_weather_event(
                    latitude=latitude,
                    longitude=longitude,
                    raw=raw,
                    timezone=timezone,
                    ingest_dt=ingest_dt,
                )
            )

        for idx, ts in enumerate(times):
            start_local = _iso_to_dt_with_tz(ts, timezone)
            start_utc = _to_utc(start_local)
            end_utc = start_utc + dt.timedelta(minutes=record_frequency_min)

            events.append(
                {
                    "event_id": _build_event_id(
                        "weather",
                        latitude,
                        longitude,
                        start_utc,
                        record_type=record_type,
                    ),
                    "data_kind": "weather",
                    "record_type": record_type,
                    "record_frequency_min": record_frequency_min,
                    "open_meteo_start_time": _isoformat_utc(start_utc),
                    "open_meteo_end_time": _isoformat_utc(end_utc),
                    "latitude": latitude,
                    "longitude": longitude,
                    "temperature": _list_lookup(hourly, "temperature_2m", idx),
                    "temperature_unit": hourly_units.get("temperature_2m"),
                    "rel_humidity": _list_lookup(hourly, "relative_humidity_2m", idx),
                    "rel_humidity_unit": hourly_units.get("relative_humidity_2m"),
                    "dew_point": _list_lookup(hourly, "dew_point_2m", idx),
                    "dew_point_unit": hourly_units.get("dew_point_2m"),
                    "apparent_temperature": _list_lookup(hourly, "apparent_temperature", idx),
                    "apparent_temperature_unit": hourly_units.get("apparent_temperature"),
                    "precipitation_prob": _list_lookup(hourly, "precipitation_probability", idx),
                    "precipitation_prob_unit": hourly_units.get("precipitation_probability"),
                    "precipitation": _list_lookup(hourly, "precipitation", idx),
                    "precipitation_unit": hourly_units.get("precipitation"),
                    "cloud_cover": _list_lookup(hourly, "cloud_cover", idx),
                    "cloud_cover_unit": hourly_units.get("cloud_cover"),
                    "wind_speed": _list_lookup(hourly, "wind_speed_10m", idx),
                    "wind_speed_unit": hourly_units.get("wind_speed_10m"),
                    "wind_gusts": _list_lookup(hourly, "wind_gusts_10m", idx),
                    "wind_gusts_unit": hourly_units.get("wind_gusts_10m"),
                    "wind_direction": _list_lookup(hourly, "wind_direction_10m", idx),
                    "wind_direction_unit": hourly_units.get("wind_direction_10m"),
                    "is_day": _as_bool(_list_lookup(hourly, "is_day", idx)),
                    "ingested_at_dtz": _isoformat_utc(ingest_dt),
                }
            )

        return events

    def build_air_quality_events(
        self,
        latitude: float,
        longitude: float,
        raw: Dict[str, Any],
        *,
        timezone: str,
        record_type: str = "forecast",
        ingest_dt: Optional[dt.datetime] = None,
        record_frequency_min: int = 60,
    ) -> List[Dict[str, Any]]:
        """Normalize hourly/current air-quality payloads into Kafka-ready events."""
        ingest_dt = ingest_dt or dt.datetime.now(tz=dt.timezone.utc)
        hourly = raw.get("hourly") or {}
        hourly_units = raw.get("hourly_units") or {}
        times = hourly.get("time") or []

        events: List[Dict[str, Any]] = []
        if raw.get("current"):
            events.append(
                self._build_current_air_event(
                    latitude=latitude,
                    longitude=longitude,
                    raw=raw,
                    timezone=timezone,
                    ingest_dt=ingest_dt,
                )
            )

        for idx, ts in enumerate(times):
            start_local = _iso_to_dt_with_tz(ts, timezone)
            start_utc = _to_utc(start_local)
            end_utc = start_utc + dt.timedelta(minutes=record_frequency_min)
            events.append(
                {
                    "event_id": _build_event_id(
                        "air_quality",
                        latitude,
                        longitude,
                        start_utc,
                        record_type=record_type,
                    ),
                    "data_kind": "air_quality",
                    "record_type": record_type,
                    "record_frequency_min": record_frequency_min,
                    "open_meteo_start_time": _isoformat_utc(start_utc),
                    "open_meteo_end_time": _isoformat_utc(end_utc),
                    "latitude": latitude,
                    "longitude": longitude,
                    "pm2_5": _list_lookup(hourly, "pm2_5", idx),
                    "pm2_5_unit": hourly_units.get("pm2_5"),
                    "pm10": _list_lookup(hourly, "pm10", idx),
                    "pm10_unit": hourly_units.get("pm10"),
                    "us_aqi": _list_lookup(hourly, "us_aqi", idx),
                    "us_aqi_unit": hourly_units.get("us_aqi"),
                    "ozone": _list_lookup(hourly, "ozone", idx),
                    "ozone_unit": hourly_units.get("ozone"),
                    "uv_index": _list_lookup(hourly, "uv_index", idx),
                    "uv_index_unit": hourly_units.get("uv_index"),
                    "ingested_at_dtz": _isoformat_utc(ingest_dt),
                }
            )
        return events

    # ------------------------------------------------------------------ #
    # Helpers                                                           #
    # ------------------------------------------------------------------ #
    def _get(self, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
        try:
            resp = self._session.get(url, params=params)
            resp.raise_for_status()
        except requests.exceptions.HTTPError as exc:  # pragma: no cover - thin wrapper
            logger.error("Open-Meteo request failed: %s", exc)
            raise OpenMeteoClientError(str(exc)) from exc
        return resp.json()

    def _build_current_weather_event(
        self,
        *,
        latitude: float,
        longitude: float,
        raw: Dict[str, Any],
        timezone: str,
        ingest_dt: dt.datetime,
    ) -> Dict[str, Any]:
        current = raw.get("current") or {}
        units = raw.get("current_units") or {}
        ts = current.get("time")
        start_local = _iso_to_dt_with_tz(ts, timezone)
        start_utc = _to_utc(start_local)
        end_utc = start_utc + dt.timedelta(minutes=15)
        return {
            "event_id": _build_event_id(
                "weather",
                latitude,
                longitude,
                start_utc,
                record_type="current",
            ),
            "data_kind": "weather",
            "record_type": "current",
            "record_frequency_min": 15,
            "open_meteo_start_time": _isoformat_utc(start_utc),
            "open_meteo_end_time": _isoformat_utc(end_utc),
            "latitude": latitude,
            "longitude": longitude,
            "temperature": current.get("temperature_2m"),
            "temperature_unit": units.get("temperature_2m"),
            "rel_humidity": current.get("relative_humidity_2m"),
            "rel_humidity_unit": units.get("relative_humidity_2m"),
            "dew_point": current.get("dew_point_2m"),
            "dew_point_unit": units.get("dew_point_2m"),
            "apparent_temperature": current.get("apparent_temperature"),
            "apparent_temperature_unit": units.get("apparent_temperature"),
            "precipitation_prob": current.get("precipitation_probability"),
            "precipitation_prob_unit": units.get("precipitation_probability"),
            "precipitation": current.get("precipitation"),
            "precipitation_unit": units.get("precipitation"),
            "cloud_cover": current.get("cloud_cover"),
            "cloud_cover_unit": units.get("cloud_cover"),
            "wind_speed": current.get("wind_speed_10m"),
            "wind_speed_unit": units.get("wind_speed_10m"),
            "wind_gusts": current.get("wind_gusts_10m"),
            "wind_gusts_unit": units.get("wind_gusts_10m"),
            "wind_direction": current.get("wind_direction_10m"),
            "wind_direction_unit": units.get("wind_direction_10m"),
            "is_day": _as_bool(current.get("is_day")),
            "ingested_at_dtz": _isoformat_utc(ingest_dt),
        }

    def _build_current_air_event(
        self,
        *,
        latitude: float,
        longitude: float,
        raw: Dict[str, Any],
        timezone: str,
        ingest_dt: dt.datetime,
    ) -> Dict[str, Any]:
        current = raw.get("current") or {}
        units = raw.get("current_units") or {}
        ts = current.get("time")
        start_local = _iso_to_dt_with_tz(ts, timezone)
        start_utc = _to_utc(start_local)
        end_utc = start_utc + dt.timedelta(minutes=15)
        return {
            "event_id": _build_event_id(
                "air_quality",
                latitude,
                longitude,
                start_utc,
                record_type="current",
            ),
            "data_kind": "air_quality",
            "record_type": "current",
            "record_frequency_min": 15,
            "open_meteo_start_time": _isoformat_utc(start_utc),
            "open_meteo_end_time": _isoformat_utc(end_utc),
            "latitude": latitude,
            "longitude": longitude,
            "pm2_5": current.get("pm2_5"),
            "pm2_5_unit": units.get("pm2_5"),
            "pm10": current.get("pm10"),
            "pm10_unit": units.get("pm10"),
            "us_aqi": current.get("us_aqi"),
            "us_aqi_unit": units.get("us_aqi"),
            "ozone": current.get("ozone"),
            "ozone_unit": units.get("ozone"),
            "uv_index": current.get("uv_index"),
            "uv_index_unit": units.get("uv_index"),
            "ingested_at_dtz": _isoformat_utc(ingest_dt),
        }


def make_open_meteo_client_from_env(session: Optional[requests.Session] = None) -> OpenMeteoClient:
    """Convenience factory using environment overrides, mirroring other clients."""
    import os

    config = OpenMeteoClientConfig(
        weather_url=os.getenv("OPEN_METEO_WEATHER_URL", OPEN_METEO_WEATHER_URL),
        air_url=os.getenv("OPEN_METEO_AIR_URL", OPEN_METEO_AIR_URL),
        timeout_seconds=int(os.getenv("OPEN_METEO_TIMEOUT_SEC", DEFAULT_TIMEOUT)),
        retries=int(os.getenv("OPEN_METEO_RETRIES", DEFAULT_RETRIES)),
        retry_backoff_seconds=float(os.getenv("OPEN_METEO_RETRY_BACKOFF", DEFAULT_BACKOFF)),
    )
    return OpenMeteoClient(config=config, session=session)
