from datetime import datetime, timezone

from weather_insight.db import ops_airnow as ops
from weather_insight.db.models.airnow import StgAirnowAirQualityForecast


def test_ensure_airnow_forecast_record_inserts(dummy_session):
    forecast = {
        "event_id": "evt-1",
        "source": "airnow",
        "date_issue_dtz": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "date_forecast_dtz": datetime(2024, 1, 2, tzinfo=timezone.utc),
        "reporting_area": "Madison",
        "state_code": "WI",
        "latitude": 43.0,
        "longitude": -89.0,
        "parameter_name": "PM2.5",
        "aqi": 10,
        "category_number": 1,
        "category_name": "Good",
        "action_day": False,
        "discussion": "Calm",
        "raw_data": {"raw": True},
    }
    dummy_session.results = [None]

    row = ops.ensure_airnow_forecast_record(dummy_session, forecast)

    assert isinstance(row, StgAirnowAirQualityForecast)
    assert row.event_id == "evt-1"
    assert row.aqi == 10
    assert dummy_session.added  # inserted


def test_ensure_airnow_forecast_record_updates_existing(dummy_session):
    existing = StgAirnowAirQualityForecast(
        event_id="evt-1",
        source="airnow",
        date_issue_dtz=datetime(2024, 1, 1, tzinfo=timezone.utc),
        date_forecast_dtz=datetime(2024, 1, 2, tzinfo=timezone.utc),
        reporting_area="Old",
        state_code="WI",
        latitude=0.0,
        longitude=0.0,
        parameter_name="PM2.5",
        aqi=5,
        category_number=1,
        category_name="Good",
        action_day=False,
        discussion="",
        raw_data="{}",
    )
    dummy_session.results = [existing]
    forecast = {
        "event_id": "evt-1",
        "source": "airnow",
        "date_issue_dtz": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "date_forecast_dtz": datetime(2024, 1, 2, tzinfo=timezone.utc),
        "reporting_area": "New Area",
        "state_code": "WI",
        "latitude": 43.0,
        "longitude": -89.0,
        "parameter_name": "PM2.5",
        "aqi": 25,
        "category_number": 2,
        "category_name": "Moderate",
        "action_day": True,
        "discussion": "Updated",
        "raw_data": {"raw": True},
    }

    row = ops.ensure_airnow_forecast_record(dummy_session, forecast)

    assert row is existing
    assert row.reporting_area == "New Area"
    assert row.aqi == 25
    assert row.category_name == "Moderate"
