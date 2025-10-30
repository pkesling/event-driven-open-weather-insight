from datetime import datetime, timezone

import pytest

from weather_insight.sinks import openaq_postgres_sink as sink


def test_build_location_payload_extracts_nested_fields():
    payload = {
        "location": {
            "id": "10",
            "name": "Loc",
            "locality": "Town",
            "timezone": "UTC",
            "country": {"code": "US", "id": 1, "name": "USA"},
            "owner": {"id": 2, "name": "Owner"},
            "provider": {"id": 3, "name": "Provider"},
            "coordinates": {"latitude": 1.1, "longitude": 2.2},
            "firstSeenAt": "2024-01-01T00:00:00Z",
            "lastSeenAt": "2024-01-02T00:00:00Z",
        }
    }
    loc = sink.build_location_payload(payload)
    assert loc["openaq_locations_id"] == 10
    assert loc["country_code"] == "US"
    assert loc["first_seen_at_dtz"].tzinfo == timezone.utc


def test_build_license_payloads_normalizes_fields():
    payload = {
        "licenses": [
            {
                "openaq_license_id": "4",
                "name": "License",
                "license_attribution_name": "Alice",
                "license_attribution_url": "https://a",
                "license_date_from": "2024-01-01T00:00:00Z",
                "license_date_to": "2024-01-31T00:00:00Z",
            }
        ]
    }
    licenses = list(sink.build_license_payloads(payload))
    assert licenses[0]["openaq_license_id"] == 4
    assert licenses[0]["license_attribution_name"] == "Alice"


def test_build_sensor_payloads_throws_exception_for_bad_ids():
    payload = {
        "sensors": [
            {"id": "123", "parameter": {"name": "pm25", "id": 1, "displayName": "PM", "units": "ppm"}},
            {"id": "bad"},  # expect exception
        ]
    }

    with pytest.raises(ValueError):
        list(sink.build_sensor_payloads(payload))


def test_build_measurements_payload_includes_meta(monkeypatch):
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)

    monkeypatch.setattr(sink, "utcnow", lambda: now)
    payload = {
        "meta": {"source": "openaq", "version": "v3"},
        "latest": {
            "results": [
                {
                    "locationsId": 10,
                    "sensorsId": 20,
                    "datetime": {"utc": "2024-01-01T00:00:00Z"},
                    "value": 1.5,
                }
            ]
        },
    }
    measurements = list(sink.build_measurements_payload(payload))
    assert measurements[0]["source_version"] == "v3"
    assert measurements[0]["ingested_at_dtz"] == now
