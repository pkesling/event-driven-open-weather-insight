from __future__ import annotations

from weather_insight.clients import openaq_client as oc


def make_client(session):
    config = oc.OpenAQClientConfig()
    return oc.OpenAQClient(config=config, session=session)


class DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class DummySession:
    def __init__(self, response):
        self.response = response
        self.headers = {}
        self.params = None
        self.url = None
        self.request = lambda *args, **kwargs: self.response

    def get(self, url, params=None):
        self.url = url
        self.params = params
        return self.response

    def mount(self, *_args, **_kwargs):
        return None


def test_find_nearest_location_sorts_by_distance(monkeypatch):
    data = {
        "results": [
            {"id": 2, "name": "far", "distance": 200},
            {"id": 1, "name": "near", "distance": 50},
        ]
    }
    session = DummySession(DummyResponse(data))
    client = make_client(session)
    nearest = client.find_nearest_location(10.5, -20.2, radius_m=10, limit=2)

    assert nearest["id"] == 1
    assert session.url.endswith("/locations")
    assert session.params["coordinates"] == "10.5,-20.2"
    assert session.params["radius"] == 10


def test_extract_sensor_map_from_location_normalizes_ids():
    location = {
        "sensors": [
            {"id": 123, "parameter": {"name": "pm25", "id": 1, "displayName": "PM 2.5", "units": "ppm"}},
            {"id": None},  # skipped
        ]
    }
    sensor_map = oc.OpenAQClient.extract_sensor_map_from_location(location)
    assert sensor_map == {
        "123": {
            "id": "123",
            "parameter_name": "pm25",
            "parameter_id": 1,
            "parameter_display_name": "PM 2.5",
            "parameter_units": "ppm",
        }
    }


def test_extract_licenses_from_location_fetches_metadata(monkeypatch):
    calls = []

    def fake_fetch(license_id, session=None):
        calls.append(license_id)
        return {
            "name": "Creative Commons",
            "sourceUrl": "https://cc.org",
            "commercialUseAllowed": True,
            "attributionRequired": False,
            "shareAlikeRequired": False,
            "modificationAllowed": True,
            "redistributionAllowed": True,
        }

    session = DummySession(DummyResponse({}))
    client = make_client(session)
    monkeypatch.setattr(client, "fetch_license_meta", fake_fetch)
    location = {
        "licenses": [
            {"id": 7, "dateFrom": "2024-01-01", "attribution": {"name": "Alice", "url": "http://a"}},
        ]
    }
    licenses = client.extract_licenses_from_location(location)

    assert calls == [7]
    assert licenses[0]["openaq_license_id"] == 7
    assert licenses[0]["license_attribution_name"] == "Alice"


def test_build_latest_events_payload_reuses_helpers(monkeypatch):
    nearest = {
        "id": 9,
        "name": "Location",
        "sensors": [{"id": 1, "parameter": {"name": "pm25", "id": 1, "displayName": "PM2.5", "units": "ppm"}}],
    }
    latest = {"latest": {"results": []}}

    session = DummySession(DummyResponse({}))
    client = make_client(session)
    monkeypatch.setattr(
        client,
        "extract_licenses_from_location",
        lambda location: [{"openaq_license_id": 1}],
    )

    envelope = client.build_latest_events_payload(nearest, latest)

    assert set(envelope.keys()) == {"location", "licenses", "sensors", "latest", "meta"}
    assert envelope["meta"]["event_type"] == "openaq.location.latest"
    assert envelope["licenses"] == [{"openaq_license_id": 1}]
