"""Upsert helpers for OpenAQ locations, sensors, licenses, and measurements."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Iterable, Mapping, Any, Optional, Dict
from sqlalchemy import select
from sqlalchemy.orm import Session

from weather_insight.db.models import (
    DimOpenAQLicense,
    DimOpenAQLocation,
    DimOpenAQSensor,
    OpenAQLocationLicenseBridge,
    StgOpenaqLatestMeasurements,
)
from weather_insight.db.utils import parse_utc, utcnow
from weather_insight.utils.logging_utils import get_tagged_logger, setup_logging
setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
logger = get_tagged_logger(__name__, tag="ops_openaq")


class OpenAQMissingValue(Exception):
    """Raised when a required value is missing from the OpenAQ payload."""


def _normalize_value(raw_value: Any) -> tuple[float | None, bool, str | None]:
    """
    Return (value_normalized, is_valid, quality_status).

    - If raw_value is None -> (None, False, "missing_value")
    - If raw_value < 0 -> (None, False, "negative_value")
    - Non-numeric -> (None, False, "non_numeric_value")
    - Else -> (raw_value, True, None)
    """
    if raw_value is None:
        return None, False, "missing_value"
    try:
        val = float(raw_value)
    except (TypeError, ValueError):
        return None, False, "non_numeric_value"

    if val < 0:
        return None, False, "negative_value"

    return val, True, None

def _has_changes(obj, attrs: dict[str, Any]) -> bool:
    """Return True if any attribute on obj differs from the provided values."""
    return any(getattr(obj, key) != value for key, value in attrs.items())


# =========================================================
# SCD-2 helpers for dimensions
# =========================================================

def ensure_current_location(
    session: Session,
    loc: Mapping[str, Any],
) -> DimOpenAQLocation:
    """
    SCD-2 upsert for a location by business key openaq_locations_id.
    Expects UTC datetimes for *_dtz fields if provided.
    """
    logger.info(f"SCD-2 upsert for location {loc['openaq_locations_id']}")
    ingest_dt = loc.get("effective_start_at_dtz") or utcnow()
    stmt = (
        select(DimOpenAQLocation)
        .where(
            DimOpenAQLocation.openaq_locations_id == loc["openaq_locations_id"],
            DimOpenAQLocation.is_current.is_(True),
        )
        .limit(1)
    )
    row = session.execute(stmt).scalar_one_or_none()

    attrs = {
        "name": loc.get("name"),
        "locality": loc.get("locality"),
        "timezone": loc.get("timezone"),
        "country_code": loc.get("country_code"),
        "country_id": loc.get("country_id"),
        "country_name": loc.get("country_name"),
        "owner_id": loc.get("owner_id"),
        "owner_name": loc.get("owner_name"),
        "provider_id": loc.get("provider_id"),
        "provider_name": loc.get("provider_name"),
        "latitude": loc.get("latitude"),
        "longitude": loc.get("longitude"),
        "first_seen_at_dtz": loc.get("first_seen_at_dtz"),
        "last_seen_at_dtz": loc.get("last_seen_at_dtz"),
    }

    if row is None:
        logger.info(f"{loc['openaq_locations_id']} is new. Creating a new record...")
        row = DimOpenAQLocation(
            openaq_locations_id=loc["openaq_locations_id"],
            effective_start_at_dtz=ingest_dt,
            is_current=True,
            **attrs,
        )
        session.add(row)
        session.flush()
        return row

    if not _has_changes(row, attrs):
        row.last_seen_at_dtz = attrs["last_seen_at_dtz"] or row.last_seen_at_dtz
        row.first_seen_at_dtz = attrs["first_seen_at_dtz"] or row.first_seen_at_dtz
        return row

    logger.info(
        f"Detected changes for {loc['openaq_locations_id']}; closing current row and inserting a new version."
    )
    row.effective_end_at_dtz = ingest_dt
    row.is_current = False
    session.flush()

    new_row = DimOpenAQLocation(
        openaq_locations_id=loc["openaq_locations_id"],
        effective_start_at_dtz=ingest_dt,
        is_current=True,
        **attrs,
    )
    session.add(new_row)
    session.flush()
    return new_row


def ensure_current_license(
    session: Session,
    lic: Mapping[str, Any],
) -> DimOpenAQLicense:
    """
    SCD-2 upsert for a license by business key openaq_license_id.
    """
    logger.info(f"SCD-2 upsert for license {lic['openaq_license_id']}")
    ingest_dt = lic.get("effective_start_at_dtz") or utcnow()
    stmt = (
        select(DimOpenAQLicense)
        .where(
            DimOpenAQLicense.openaq_license_id == lic["openaq_license_id"],
            DimOpenAQLicense.is_current.is_(True),
        )
        .limit(1)
    )
    row = session.execute(stmt).scalar_one_or_none()

    attrs = {
        "name": lic.get("name"),
        "commercial_use_allowed": lic.get("commercial_use_allowed"),
        "attribution_required": lic.get("attribution_required"),
        "share_alike_required": lic.get("share_alike_required"),
        "modification_allowed": lic.get("modification_allowed"),
        "redistribution_allowed": lic.get("redistribution_allowed"),
        "source_url": lic.get("source_url"),
    }

    if row is None:
        logger.info(f"{lic['openaq_license_id']} is new. Creating a new record...")
        row = DimOpenAQLicense(
            openaq_license_id=lic["openaq_license_id"],
            effective_start_at_dtz=ingest_dt,
            is_current=True,
            **attrs,
        )
        session.add(row)
        session.flush()
        return row

    if not _has_changes(row, attrs):
        row.last_updated_at_dtz = utcnow()
        return row

    logger.info(
        f"Detected changes for license {lic['openaq_license_id']}; closing current row and inserting a new version."
    )
    row.effective_end_at_dtz = ingest_dt
    row.is_current = False
    row.last_updated_at_dtz = utcnow()
    session.flush()

    new_row = DimOpenAQLicense(
        openaq_license_id=lic["openaq_license_id"],
        effective_start_at_dtz=ingest_dt,
        is_current=True,
        **attrs,
    )
    session.add(new_row)
    session.flush()
    return new_row


def ensure_location_license_bridge(
    session: Session,
    location_sk: int,
    license_sk: int,
    *,
    license_attribution_name: Optional[str] = None,
    license_attribution_url: Optional[str] = None,
    license_date_from_dtz: Optional[datetime] = None,
    license_date_to_dtz: Optional[datetime] = None,
) -> OpenAQLocationLicenseBridge:
    """
    SCD-2 upsert of mapping row between a location SK and a license SK.
    """
    logger.info(f"Upserting location/license bridge for location_sk={location_sk} and license_sk={license_sk}")
    ingest_dt = utcnow()
    stmt = (
        select(OpenAQLocationLicenseBridge)
        .where(
            OpenAQLocationLicenseBridge.locations_id_sk == location_sk,
            OpenAQLocationLicenseBridge.license_id_sk == license_sk,
            OpenAQLocationLicenseBridge.is_current.is_(True),
        )
        .limit(1)
    )
    row = session.execute(stmt).scalar_one_or_none()

    attrs = {
        "license_attribution_name": license_attribution_name,
        "license_attribution_url": license_attribution_url,
        "license_date_from_dtz": license_date_from_dtz,
        "license_date_to_dtz": license_date_to_dtz,
    }

    if row is None:
        logger.info("No existing bridge found.  Creating a new record...")
        row = OpenAQLocationLicenseBridge(
            locations_id_sk=location_sk,
            license_id_sk=license_sk,
            effective_start_at_dtz=ingest_dt,
            is_current=True,
            **attrs,
        )
        session.add(row)
        session.flush()
        return row

    if not _has_changes(row, attrs):
        row.last_updated_at_dtz = utcnow()
        return row

    logger.info("Existing bridge changed. Closing current and inserting a new version.")
    row.effective_end_at_dtz = ingest_dt
    row.is_current = False
    row.last_updated_at_dtz = utcnow()
    session.flush()

    new_row = OpenAQLocationLicenseBridge(
        locations_id_sk=location_sk,
        license_id_sk=license_sk,
        effective_start_at_dtz=ingest_dt,
        is_current=True,
        **attrs,
    )
    session.add(new_row)
    session.flush()
    return new_row
    return row


# ----------------------------------
# Sensor upsert (SCD-2 semantics)
# -----------------------------------
def ensure_current_sensors(
    session,
    sensor_row: Mapping[str, Any],
    location_sk: Optional[int],
) -> DimOpenAQSensor:
    """
    SCD-2 upsert for sensor rows keyed by openaq_sensors_id.
    """
    logger.info(f"SCD-2 upsert for sensor {sensor_row['openaq_sensors_id']}")
    ingest_dt = sensor_row.get("effective_start_at_dtz") or utcnow()
    stmt = (
        select(DimOpenAQSensor)
        .where(
            DimOpenAQSensor.openaq_sensors_id == sensor_row["openaq_sensors_id"],
            DimOpenAQSensor.is_current.is_(True),
        )
        .limit(1)
    )
    sensor = session.execute(stmt).scalar_one_or_none()

    attrs = {
        "locations_id_sk": location_sk,
        "parameter_name": sensor_row.get("parameter_name"),
        "parameter_id": sensor_row.get("parameter_id"),
        "parameter_display_name": sensor_row.get("parameter_display_name"),
        "parameter_units": sensor_row.get("parameter_units"),
    }

    if sensor is None:
        logger.info(f"{sensor_row['openaq_sensors_id']} is new. Creating a new record...")
        sensor = DimOpenAQSensor(
            openaq_sensors_id=sensor_row["openaq_sensors_id"],
            effective_start_at_dtz=ingest_dt,
            is_current=True,
            **attrs,
        )
        session.add(sensor)
        session.flush()
        return sensor

    if not _has_changes(sensor, attrs):
        sensor.last_updated_at_dtz = utcnow()
        return sensor

    logger.info(
        f"Detected changes for sensor {sensor_row['openaq_sensors_id']}; closing current row and inserting a new version."
    )
    sensor.effective_end_at_dtz = ingest_dt
    sensor.is_current = False
    sensor.last_updated_at_dtz = utcnow()
    session.flush()

    new_sensor = DimOpenAQSensor(
        openaq_sensors_id=sensor_row["openaq_sensors_id"],
        effective_start_at_dtz=ingest_dt,
        is_current=True,
        **attrs,
    )
    session.add(new_sensor)
    session.flush()
    return new_sensor

    return sensor


def upsert_location_with_licenses(
    session: Session,
    location_payload: Mapping[str, Any],
    licenses_payload: Iterable[Mapping[str, Any]],
) -> DimOpenAQLocation:
    """
    Convenience wrapper:
      1) ensure current location row,
      2) ensure each license row,
      3) ensure bridge rows.

    Expects all datetimes already normalized to UTC.
    """
    logger.info("Upserting location with licenses")
    loc_row = ensure_current_location(session, location_payload)

    for lic in licenses_payload:
        lic_row = ensure_current_license(session, lic)
        ensure_location_license_bridge(
            session,
            location_sk=loc_row.locations_id_sk,
            license_sk=lic_row.license_id_sk,
            license_attribution_name=lic.get("license_attribution_name"),
            license_attribution_url=lic.get("license_attribution_url"),
            license_date_from_dtz=lic.get("license_date_from_dtz"),
            license_date_to_dtz=lic.get("license_date_to_dtz"),
        )

    return loc_row


def _build_event_id(location_id: int, sensor_id: str, event_type: str, ts_iso: str) -> str:
    """Builds a stable, idempotent identify ID string for the event"""
    return f"{location_id}:{sensor_id}:{event_type}:{ts_iso}"


def _canonicalize_measurement_ts(value: Any) -> tuple[datetime, str]:
    """Normalize mixed datetime inputs to a UTC datetime and stable string."""
    dt = parse_utc(value)
    if dt is None:
        raise OpenAQMissingValue("datetime_utc is required for measurements")
    dt = dt.astimezone(timezone.utc)
    ts_iso = dt.isoformat().replace("+00:00", "Z")
    return dt, ts_iso


def ensure_current_measurements(
    session: Session,
    measure: Mapping[str, Any],
) -> StgOpenaqLatestMeasurements:
    """
    Upsert (current row) for a measurement.
    """
    locations_id = measure.get("openaq_locations_id")
    sensors_id = measure.get("openaq_sensors_id")
    ts_iso = measure.get("datetime_utc")
    openaq_event_type: str = os.getenv("OPENAQ_LATEST_MEASURES_EVENT_TYPE", "openaq.locations.latest")

    if not locations_id:
        raise OpenAQMissingValue("openaq_locations_id is required for measurements")

    if not sensors_id:
        raise OpenAQMissingValue("openaq_sensors_id is required for measurements")

    if not ts_iso:
        raise OpenAQMissingValue("datetime_utc is required for measurements")

    normalized_dt, normalized_ts = _canonicalize_measurement_ts(ts_iso)

    event_id = _build_event_id(locations_id, sensors_id, openaq_event_type, normalized_ts or "unknown")
    value_normalized, is_valid, quality_status = _normalize_value(measure.get("value"))

    logger.info(f"Upserting current measurements for event {event_id}")
    stmt = (
        select(StgOpenaqLatestMeasurements)
        .where(
            StgOpenaqLatestMeasurements.event_id == event_id,
        )
        .limit(1)
    )
    row = session.execute(stmt).scalar_one_or_none()

    if row is None:
        logger.info("No existing measurement record found.  Creating a new record...")
        row = StgOpenaqLatestMeasurements(
            event_id=event_id,
            openaq_locations_id=measure["openaq_locations_id"],
            openaq_sensors_id=measure["openaq_sensors_id"],
            datetime_utc=normalized_dt,
            value=measure["value"],
            value_normalized=value_normalized,
            is_valid=is_valid,
            quality_status=quality_status,
            source=measure["source"],
            source_version=measure["source_version"],
            event_type=openaq_event_type,
            ingested_at_dtz=measure["ingested_at_dtz"],
        )
        session.add(row)
        session.flush()
    else:
        logger.info("Existing measurement record found.  Updating the existing record.")
        row.value = measure.get("value")
        row.value_normalized = value_normalized
        row.is_valid = is_valid
        row.quality_status = quality_status
        row.source = measure.get("source")
        row.source_version = measure.get("source_version")
        row.event_type = openaq_event_type
        row.ingested_at_dtz = measure.get("ingested_at_dtz")
        row.datetime_utc = normalized_dt
        row.last_updated_at_dtz = utcnow()

    return row
