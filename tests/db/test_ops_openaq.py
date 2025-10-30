from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from weather_insight.db import ops_openaq as ops
from weather_insight.db.models import (
    DimOpenAQLicense,
    DimOpenAQLocation,
    OpenAQLocationLicenseBridge,
    StgOpenaqLatestMeasurements,
)


def _make_location(sk: int = 1) -> DimOpenAQLocation:
    loc = DimOpenAQLocation(
        openaq_locations_id=123,
        name="orig",
        effective_start_at_dtz=datetime(2024, 1, 1, tzinfo=timezone.utc),
        is_current=True,
    )
    loc.locations_id_sk = sk
    return loc


def _make_license(sk: int) -> DimOpenAQLicense:
    lic = DimOpenAQLicense(openaq_license_id=sk)
    lic.license_id_sk = sk
    return lic


def test_ensure_current_location_inserts_when_missing(dummy_session):
    payload = {"openaq_locations_id": 1, "name": "Test"}
    dummy_session.results = [None]

    row = ops.ensure_current_location(dummy_session, payload)

    assert row.name == "Test"
    assert row.is_current is True
    assert row.effective_end_at_dtz is None
    assert dummy_session.added  # new row added


def test_ensure_current_location_updates_existing(dummy_session):
    existing = _make_location()
    dummy_session.results = [existing]
    payload = {"openaq_locations_id": 123, "name": "Updated", "timezone": "UTC"}

    row = ops.ensure_current_location(dummy_session, payload)

    # existing row should be closed and new row returned
    assert existing.is_current is False
    assert existing.effective_end_at_dtz is not None
    assert row is not existing
    assert row.name == "Updated"
    assert row.timezone == "UTC"


def test_upsert_location_with_licenses_links_bridges(dummy_session):
    location = _make_location(sk=99)
    lic1 = _make_license(1)
    lic2 = _make_license(2)
    existing_bridge = OpenAQLocationLicenseBridge(
        locations_id_sk=99,
        license_id_sk=2,
        effective_start_at_dtz=datetime(2024, 1, 1, tzinfo=timezone.utc),
        is_current=True,
    )
    dummy_session.results = [location, lic1, None, lic2, existing_bridge]

    payload = {"openaq_locations_id": 123, "name": "Changed"}
    licenses = [
        {"openaq_license_id": 1, "license_attribution_name": "Alice"},
        {"openaq_license_id": 2, "license_attribution_name": "Bob"},
    ]

    row = ops.upsert_location_with_licenses(dummy_session, payload, licenses)

    assert row is not location  # SCD-2 inserts a new current version
    assert location.is_current is False
    # two bridge operations attempted (one insert, one close+insert)
    assert dummy_session.flushed >= 2
    assert dummy_session.committed == 0


def test_ensure_current_measurements_validates_required_fields(dummy_session):
    with pytest.raises(ops.OpenAQMissingValue):
        ops.ensure_current_measurements(dummy_session, {})


def test_ensure_current_measurements_inserts_and_updates(dummy_session, monkeypatch):
    now = datetime.now(tz=timezone.utc)
    monkeypatch.setenv("OPENAQ_LATEST_MEASURES_EVENT_TYPE", "evt")
    measure = {
        "openaq_locations_id": 10,
        "openaq_sensors_id": 20,
        "datetime_utc": "2024-01-01T00:00:00Z",
        "value": 1.23,
        "source": "api",
        "source_version": "v1",
        "ingested_at_dtz": now,
    }
    dummy_session.results = [None]
    row = ops.ensure_current_measurements(dummy_session, measure)
    assert row.event_id == "10:20:evt:2024-01-01T00:00:00Z"
    assert isinstance(row.datetime_utc, datetime)

    existing = StgOpenaqLatestMeasurements(
        event_id=row.event_id,
        openaq_locations_id=10,
        openaq_sensors_id=20,
        datetime_utc=measure["datetime_utc"],
        value=0.5,
        value_normalized=0.5,
        is_valid=True,
        source="old",
        source_version="old",
        event_type="evt",
        ingested_at_dtz=now,
    )
    dummy_session.results = [existing]
    ops.ensure_current_measurements(dummy_session, measure)
    assert existing.value == 1.23
    assert existing.value_normalized == 1.23
    assert existing.is_valid is True


def test_ensure_current_measurements_normalizes_datetime(dummy_session, monkeypatch):
    monkeypatch.setenv("OPENAQ_LATEST_MEASURES_EVENT_TYPE", "evt")
    dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    measure = {
        "openaq_locations_id": 1,
        "openaq_sensors_id": 2,
        "datetime_utc": dt,
        "value": 1,
        "source": "api",
        "source_version": "v1",
        "ingested_at_dtz": dt,
    }
    dummy_session.results = [None]
    row = ops.ensure_current_measurements(dummy_session, measure)
    assert row.event_id == "1:2:evt:2024-01-01T00:00:00Z"
    assert row.datetime_utc == dt


def test_ensure_current_measurements_handles_negative_value(dummy_session, monkeypatch):
    monkeypatch.setenv("OPENAQ_LATEST_MEASURES_EVENT_TYPE", "evt")
    measure = {
        "openaq_locations_id": 1,
        "openaq_sensors_id": 2,
        "datetime_utc": "2024-01-01T00:00:00Z",
        "value": -5.0,
        "source": "api",
        "source_version": "v1",
        "ingested_at_dtz": datetime.now(tz=timezone.utc),
    }
    dummy_session.results = [None]
    row = ops.ensure_current_measurements(dummy_session, measure)
    assert row.value == -5.0
    assert row.value_normalized is None
    assert row.is_valid is False
    assert row.quality_status == "negative_value"
