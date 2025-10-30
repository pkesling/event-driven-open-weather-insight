"""Pydantic schemas for Kafka event payloads."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Literal

from pydantic import BaseModel, ConfigDict


class AirNowForecastEvent(BaseModel):
    """Normalized AirNow forecast event payload."""

    model_config = ConfigDict(extra="ignore")

    event_id: str
    source: str
    date_issue: Optional[str] = None
    date_forecast: Optional[str] = None
    reporting_area: str
    state_code: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    parameter_name: Optional[str] = None
    aqi: Optional[int] = None
    category_number: Optional[int] = None
    category_name: Optional[str] = None
    action_day: Optional[bool] = None
    discussion: Optional[str] = None
    raw_data: Optional[Dict[str, Any]] = None


def validate_airnow_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Validate and normalize AirNow payloads prior to publishing."""
    return [AirNowForecastEvent.model_validate(ev).model_dump() for ev in events]


class WeatherHourlyForecastEvent(BaseModel):
    """Normalized weather.gov hourly forecast event payload."""

    model_config = ConfigDict(extra="ignore")

    event_id: str
    event_type: str
    source: str
    office: Optional[str] = None
    grid_id: Optional[str] = None
    grid_x: Optional[int] = None
    grid_y: Optional[int] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    is_daytime: Optional[bool] = None
    temperature_trend: Optional[str] = None
    wind_direction: Optional[str] = None
    short_forecast: Optional[str] = None
    detailed_forecast: Optional[str] = None

    temperature: Optional[float] = None
    temperature_min: Optional[float] = None
    temperature_max: Optional[float] = None
    temperature_unit: Optional[str] = None

    relative_humidity: Optional[float] = None
    relative_humidity_min: Optional[float] = None
    relative_humidity_max: Optional[float] = None
    relative_humidity_unit: Optional[str] = None

    dewpoint: Optional[float] = None
    dewpoint_min: Optional[float] = None
    dewpoint_max: Optional[float] = None
    dewpoint_unit: Optional[str] = None

    wind_speed: Optional[float] = None
    wind_speed_min: Optional[float] = None
    wind_speed_max: Optional[float] = None
    wind_speed_unit: Optional[str] = None
    wind_gust: Optional[float] = None
    wind_gust_min: Optional[float] = None
    wind_gust_max: Optional[float] = None
    wind_gust_unit: Optional[str] = None

    probability_of_precipitation: Optional[float] = None
    probability_of_precipitation_min: Optional[float] = None
    probability_of_precipitation_max: Optional[float] = None
    probability_of_precipitation_unit: Optional[str] = None
    ingested_at_dtz: Optional[str] = None


def validate_weather_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Validate and normalize weather.gov payloads prior to publishing."""
    return [
        WeatherHourlyForecastEvent.model_validate(ev).model_dump()
        for ev in events
    ]


class OpenMeteoWeatherEvent(BaseModel):
    """Normalized Open-Meteo weather payload."""

    model_config = ConfigDict(extra="ignore")

    event_id: str
    data_kind: Literal["weather"]
    record_type: str
    record_frequency_min: int
    open_meteo_start_time: str
    open_meteo_end_time: str
    latitude: float
    longitude: float

    temperature: Optional[float] = None
    temperature_unit: Optional[str] = None

    rel_humidity: Optional[float] = None
    rel_humidity_unit: Optional[str] = None

    dew_point: Optional[float] = None
    dew_point_unit: Optional[str] = None

    apparent_temperature: Optional[float] = None
    apparent_temperature_unit: Optional[str] = None

    precipitation_prob: Optional[float] = None
    precipitation_prob_unit: Optional[str] = None

    precipitation: Optional[float] = None
    precipitation_unit: Optional[str] = None

    cloud_cover: Optional[float] = None
    cloud_cover_unit: Optional[str] = None

    wind_speed: Optional[float] = None
    wind_speed_unit: Optional[str] = None

    wind_gusts: Optional[float] = None
    wind_gusts_unit: Optional[str] = None

    wind_direction: Optional[float] = None
    wind_direction_unit: Optional[str] = None

    is_day: Optional[bool] = None
    ingested_at_dtz: Optional[str] = None


def validate_open_meteo_weather_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Validate Open-Meteo weather payloads prior to publishing."""
    return [
        OpenMeteoWeatherEvent.model_validate(ev).model_dump()
        for ev in events
    ]


class OpenMeteoAirQualityEvent(BaseModel):
    """Normalized Open-Meteo air-quality payload."""

    model_config = ConfigDict(extra="ignore")

    event_id: str
    data_kind: Literal["air_quality"]
    record_type: str
    record_frequency_min: int
    open_meteo_start_time: str
    open_meteo_end_time: str
    latitude: float
    longitude: float

    pm2_5: Optional[float] = None
    pm2_5_unit: Optional[str] = None
    pm10: Optional[float] = None
    pm10_unit: Optional[str] = None
    us_aqi: Optional[float] = None
    us_aqi_unit: Optional[str] = None
    ozone: Optional[float] = None
    ozone_unit: Optional[str] = None
    uv_index: Optional[float] = None
    uv_index_unit: Optional[str] = None
    ingested_at_dtz: Optional[str] = None


def validate_open_meteo_air_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Validate Open-Meteo air-quality payloads prior to publishing."""
    return [
        OpenMeteoAirQualityEvent.model_validate(ev).model_dump()
        for ev in events
    ]
