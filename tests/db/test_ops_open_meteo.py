from datetime import datetime, timezone

from weather_insight.db import ops_open_meteo as ops
from weather_insight.db.models import StgOpenMeteoAir, StgOpenMeteoWeather


def _weather_payload():
    return {
        "event_id": "open-meteo:weather:forecast:1:2:2024-01-01T00:00:00Z",
        "open_meteo_start_time": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "open_meteo_end_time": datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        "record_type": "forecast",
        "record_frequency_min": 60,
        "latitude": 1.0,
        "longitude": 2.0,
        "temperature": 10.0,
        "temperature_unit": "C",
        "rel_humidity": 75.0,
        "rel_humidity_unit": "%",
        "dew_point": 5.0,
        "dew_point_unit": "C",
        "apparent_temperature": 8.0,
        "apparent_temperature_unit": "C",
        "precipitation_prob": 20.0,
        "precipitation_prob_unit": "%",
        "precipitation": 0.1,
        "precipitation_unit": "mm",
        "cloud_cover": 50.0,
        "cloud_cover_unit": "%",
        "wind_speed": 12.0,
        "wind_speed_unit": "kmh",
        "wind_gusts": 20.0,
        "wind_gusts_unit": "kmh",
        "wind_direction": 180.0,
        "wind_direction_unit": "deg",
        "is_day": True,
        "ingested_at_dtz": datetime(2024, 1, 1, tzinfo=timezone.utc),
    }


def _air_payload():
    return {
        "event_id": "open-meteo:air_quality:forecast:1:2:2024-01-01T00:00:00Z",
        "open_meteo_start_time": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "open_meteo_end_time": datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        "record_type": "forecast",
        "record_frequency_min": 60,
        "latitude": 1.0,
        "longitude": 2.0,
        "pm2_5": 5.5,
        "pm2_5_unit": "ug/m3",
        "pm10": 7.5,
        "pm10_unit": "ug/m3",
        "us_aqi": 20.0,
        "us_aqi_unit": "idx",
        "ozone": 40.0,
        "ozone_unit": "ppb",
        "uv_index": 1.0,
        "uv_index_unit": "idx",
        "ingested_at_dtz": datetime(2024, 1, 1, tzinfo=timezone.utc),
    }


def test_ensure_weather_record_inserts(dummy_session):
    dummy_session.results = [None]
    payload = _weather_payload()

    row = ops.ensure_open_meteo_weather_record(dummy_session, payload)
    assert isinstance(row, StgOpenMeteoWeather)
    assert row.temperature == 10.0
    assert dummy_session.added


def test_ensure_weather_record_updates_existing(dummy_session, monkeypatch):
    payload = _weather_payload()
    existing = StgOpenMeteoWeather(**payload)
    dummy_session.results = [existing]

    monkeypatch.setattr(ops, "utcnow", lambda: datetime(2024, 1, 2, tzinfo=timezone.utc))
    payload["temperature"] = 12.0

    row = ops.ensure_open_meteo_weather_record(dummy_session, payload)
    assert row.temperature == 12.0
    assert row.last_updated_at_dtz == datetime(2024, 1, 2, tzinfo=timezone.utc)


def test_ensure_air_record_inserts(dummy_session):
    dummy_session.results = [None]
    payload = _air_payload()

    row = ops.ensure_open_meteo_air_record(dummy_session, payload)
    assert isinstance(row, StgOpenMeteoAir)
    assert row.pm2_5 == 5.5
    assert dummy_session.added


def test_ensure_air_record_updates_existing(dummy_session):
    payload = _air_payload()
    existing = StgOpenMeteoAir(**payload)
    dummy_session.results = [existing]

    payload["pm2_5"] = 8.0
    row = ops.ensure_open_meteo_air_record(dummy_session, payload)
    assert row.pm2_5 == 8.0
