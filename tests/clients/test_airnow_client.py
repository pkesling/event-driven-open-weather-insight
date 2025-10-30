from datetime import date

import pytest

from weather_insight.clients.airnow_client import AirNowClient, AirNowClientConfig


def make_client():
    config = AirNowClientConfig(api_key="dummy", base_url="http://example.com")
    # use a dummy session to avoid HTTP calls; we only exercise normalization
    class DummySession:
        headers = {}
        def __init__(self):
            # mimic requests.Session.request so configure_session can wrap it
            self.request = lambda *args, **kwargs: None

        def mount(self, *_args, **_kwargs):
            return None

        def get(self, *_args, **_kwargs):
            return DummyResponse()

    return AirNowClient(config=config, session=DummySession())


def test_normalize_forecast_item_builds_event_id():
    client = make_client()
    item = {
        "ReportingArea": "Madison",
        "DateIssue": "2024-01-01",
        "DateForecast": "2024-01-02",
        "Category": {"Number": 1, "Name": "Good"},
        "ActionDay": "true",
    }
    normalized = client._normalize_forecast_item(item)
    assert normalized["event_id"] == "airnow:Madison:2024-01-01:2024-01-02"
    assert normalized["action_day"] is True
    assert normalized["category_number"] == 1
    assert normalized["category_name"] == "Good"


def test_get_daily_forecast_handles_empty_payload(monkeypatch):
    client = make_client()

    class DummyResponse:
        ok = True

        def json(self):
            return []

    class DummySession:
        def __init__(self):
            self.headers = {}
            self.request = lambda *args, **kwargs: None

        def mount(self, *_args, **_kwargs):
            return None

        def get(self, *_args, **_kwargs):
            return DummyResponse()

    client = AirNowClient(config=client._config, session=DummySession())
    events = client.get_daily_forecast_by_lat_lon(10, 20, forecast_date=date(2024, 1, 1))
    assert events == []
