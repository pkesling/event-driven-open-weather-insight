from datetime import datetime, timezone

from weather_insight.clients.open_meteo_client import (
    OpenMeteoClient,
    OpenMeteoClientConfig,
    _iso_to_dt_with_tz,
)


def test_iso_to_dt_with_tz_sets_zoneinfo():
    dt_val = _iso_to_dt_with_tz("2024-01-01T00:00", "America/Chicago")
    assert isinstance(dt_val, datetime)
    assert dt_val.tzinfo is not None
    assert dt_val.tzinfo.key == "America/Chicago"


def _sample_weather_payload():
    return {
        "hourly": {
            "time": ["2024-01-01T00:00", "2024-01-01T01:00"],
            "temperature_2m": [10.5, 11.0],
            "relative_humidity_2m": [80, 82],
            "dew_point_2m": [7.0, 7.5],
            "apparent_temperature": [8.0, 8.5],
            "precipitation_probability": [10, 20],
            "precipitation": [0.1, 0.0],
            "cloud_cover": [50, 60],
            "wind_speed_10m": [5.0, 6.0],
            "wind_gusts_10m": [12.0, 13.0],
            "wind_direction_10m": [180, 190],
            "is_day": [0, 1],
        },
        "hourly_units": {
            "temperature_2m": "C",
            "relative_humidity_2m": "%",
            "dew_point_2m": "C",
            "apparent_temperature": "C",
            "precipitation_probability": "%",
            "precipitation": "mm",
            "cloud_cover": "%",
            "wind_speed_10m": "kmh",
            "wind_gusts_10m": "kmh",
            "wind_direction_10m": "deg",
            "is_day": None,
        },
        "current": {
            "time": "2024-01-01T00:00",
            "temperature_2m": 9.0,
            "relative_humidity_2m": 75,
            "dew_point_2m": 6.0,
            "apparent_temperature": 7.5,
            "precipitation": 0.0,
            "cloud_cover": 40,
            "wind_speed_10m": 4.0,
            "wind_gusts_10m": 10.0,
            "wind_direction_10m": 170,
            "is_day": 0,
        },
        "current_units": {
            "temperature_2m": "C",
            "relative_humidity_2m": "%",
            "dew_point_2m": "C",
            "apparent_temperature": "C",
            "precipitation": "mm",
            "cloud_cover": "%",
            "wind_speed_10m": "kmh",
            "wind_gusts_10m": "kmh",
            "wind_direction_10m": "deg",
            "is_day": None,
        },
    }


def _sample_air_payload():
    return {
        "hourly": {
            "time": ["2024-01-01T00:00"],
            "pm2_5": [5.5],
            "pm10": [7.5],
            "ozone": [40.0],
            "uv_index": [0.8],
            "us_aqi": [21],
        },
        "hourly_units": {
            "pm2_5": "µg/m3",
            "pm10": "µg/m3",
            "ozone": "ppb",
            "uv_index": "idx",
            "us_aqi": "idx",
        },
        "current": {
            "time": "2024-01-01T00:00",
            "pm2_5": 5.0,
            "pm10": 7.0,
            "ozone": 39.0,
            "uv_index": 0.7,
            "us_aqi": 20,
        },
        "current_units": {
            "pm2_5": "µg/m3",
            "pm10": "µg/m3",
            "ozone": "ppb",
            "uv_index": "idx",
            "us_aqi": "idx",
        },
    }


def test_build_weather_events_includes_current_and_hourly():
    client = OpenMeteoClient(OpenMeteoClientConfig())
    payload = _sample_weather_payload()
    ingest_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = client.build_weather_events(
        43.0,
        -89.0,
        payload,
        timezone="America/Chicago",
        ingest_dt=ingest_dt,
    )

    assert len(events) == 3  # current + 2 hourly
    first = events[0]
    assert first["data_kind"] == "weather"
    assert first["record_type"] == "current"
    assert first["temperature"] == 9.0
    assert first["wind_direction"] == 170
    assert first["open_meteo_start_time"].endswith("Z")

    second = events[1]
    assert second["record_type"] == "forecast"
    assert second["precipitation_prob"] == 10
    assert second["wind_gusts_unit"] == "kmh"


def test_build_air_events_includes_current_and_hourly():
    client = OpenMeteoClient(OpenMeteoClientConfig())
    payload = _sample_air_payload()
    ingest_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = client.build_air_quality_events(
        43.0,
        -89.0,
        payload,
        timezone="UTC",
        ingest_dt=ingest_dt,
    )

    assert len(events) == 2  # current + hourly
    event = events[0]
    assert event["data_kind"] == "air_quality"
    assert event["record_type"] == "current"
    assert event["pm2_5"] == 5.0
    assert event["ingested_at_dtz"].endswith("Z")
