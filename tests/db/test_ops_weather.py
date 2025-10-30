from datetime import datetime, timezone

from weather_insight.db import ops_weather as ops
from weather_insight.db.models import StgWeatherHourlyForecast


def _forecast_payload():
    return {
        "event_id": "MKX:1,2:2024-01-01T00:00:00Z-2024-01-01T01:00:00Z",
        "event_type": "type",
        "source": "api",
        "office": "MKX",
        "grid_id": "MKX",
        "grid_x": 1,
        "grid_y": 2,
        "start_time": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "end_time": datetime(2024, 1, 1, 1, tzinfo=timezone.utc),
        "is_daytime": True,
        "temperature": 70,
        "temperature_min": 65,
        "temperature_max": 75,
        "temperature_unit": "degF",
        "temperature_trend": None,
        "relative_humidity": 50,
        "relative_humidity_min": 45,
        "relative_humidity_max": 55,
        "relative_humidity_unit": "%",
        "dewpoint": 40,
        "dewpoint_min": 38,
        "dewpoint_max": 42,
        "dewpoint_unit": "degF",
        "wind_speed": 5,
        "wind_speed_min": 3,
        "wind_speed_max": 7,
        "wind_speed_unit": "mph",
        "wind_direction": "N",
        "wind_gust": 10,
        "wind_gust_min": 8,
        "wind_gust_max": 12,
        "wind_gust_unit": "mph",
        "probability_of_precipitation": 20,
        "probability_of_precipitation_min": 10,
        "probability_of_precipitation_max": 30,
        "probability_of_precipitation_unit": "%",
        "short_forecast": "Sunny",
        "detailed_forecast": "Sunny and warm",
        "ingested_at_dtz": datetime(2024, 1, 1, tzinfo=timezone.utc),
    }


def test_ensure_forecast_record_inserts_when_missing(dummy_session):
    dummy_session.results = [None]
    payload = _forecast_payload()
    row = ops.ensure_forecast_record(dummy_session, payload)
    assert row.event_id == payload["event_id"]
    assert dummy_session.added


def test_ensure_forecast_record_updates_existing(dummy_session):
    payload = _forecast_payload()
    existing = StgWeatherHourlyForecast(**payload)
    dummy_session.results = [existing]

    payload["temperature"] = 77
    row = ops.ensure_forecast_record(dummy_session, payload)
    assert row.temperature == 77


def test_ensure_forecast_record_updates_precip_and_timestamps(dummy_session, monkeypatch):
    payload = _forecast_payload()
    existing = StgWeatherHourlyForecast(**payload)
    dummy_session.results = [existing]

    updated_ingested = datetime(2024, 1, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(ops, "utcnow", lambda: datetime(2024, 1, 3, tzinfo=timezone.utc))

    payload["probability_of_precipitation"] = 99
    payload["ingested_at_dtz"] = updated_ingested

    row = ops.ensure_forecast_record(dummy_session, payload)

    assert row.probability_of_precipitation == 99
    assert row.ingested_at_dtz == updated_ingested
    assert row.last_updated_at_dtz == datetime(2024, 1, 3, tzinfo=timezone.utc)
