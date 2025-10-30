""" Kafka sink that loads openaq measurements into Postgres."""
from __future__ import annotations

import os
from typing import Any, Dict, Iterable

from weather_insight.db import SessionLocal
from weather_insight.db.ops_openaq import (
    ensure_current_measurements,
    ensure_current_sensors,
    upsert_location_with_licenses,
)
from weather_insight.db.utils import parse_utc, utcnow

from weather_insight.sinks.base_postgres_sink import BasePostgresSink, SinkConfig
from weather_insight.utils.logging_utils import get_tagged_logger, setup_logging
setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
logger = get_tagged_logger(__name__, tag="openaq_postgres_sink")


# Configuration via environment variables.
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "openaq-postgres-sink")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_OPENAQ", "raw.openaq.latest_by_location")
KAFKA_DEAD_LETTER_TOPIC = os.environ.get("KAFKA_TOPIC_OPENAQ_DLT", "raw.openaq.latest_by_location.dlq")
KAFKA_SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_POLL_TIMEOUT_SEC = float(os.environ.get("KAFKA_POLL_TIMEOUT_SEC", "1.0"))


def build_location_payload(msg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract location fields from a producer event and transform them into the dict
    expected by the upsert_location_with_licenses() helper.

    Assumes the event schema produced by your Airflow @task for OpenAQ v3.
    """
    loc = msg.get("location") or {}
    country = loc.get("country") or {}
    owner = loc.get("owner") or {}
    provider = loc.get("provider") or {}
    coords = (loc.get("coordinates") or {}) or {}

    return {
        "openaq_locations_id": int(loc["id"]),  # let a casting exception be raised if needed
        "name": loc.get("name"),
        "locality": loc.get("locality"),
        "timezone": loc.get("timezone"),
        "country_code": country.get("code"),
        "country_id": country.get("id"),
        "country_name": country.get("name"),
        "owner_id": owner.get("id"),
        "owner_name": owner.get("name"),
        "provider_id": provider.get("id"),
        "provider_name": provider.get("name"),
        "latitude": coords.get("latitude"),
        "longitude": coords.get("longitude"),
        "first_seen_at_dtz": parse_utc(loc.get("firstSeenAt")),
        "last_seen_at_dtz": parse_utc(loc.get("lastSeenAt")),
        # SCD-2 effective_* fields kept default for now (current-row upsert) until implemented.
        # "effective_start_at_dtz": ...,
        # "effective_end_at_dtz": ...,
        # "is_current": ...,
    }


def build_license_payloads(msg: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """
    Extract licenses fields from a producer event and transform them into the dict
    expected by the upsert_location_with_licenses() helper.

    Assumes the event schema produced by your Airflow @task for OpenAQ v3.
    """

    # Transform license array into license rows and bridge attributes.
    for lic in msg.get("licenses") or []:
        yield {
            # license metadata:
            "openaq_license_id": int(lic["openaq_license_id"]),
            "name": lic.get("name"),
            "commercial_use_allowed": lic.get("commercial_use_allowed"),
            "attribution_required": lic.get("attribution_required"),
            "share_alike_required": lic.get("share_alike_required"),
            "modification_allowed": lic.get("modification_allowed"),
            "redistribution_allowed": lic.get("redistribution_allowed"),
            "source_url": lic.get("source_url"),
            # location-to-license bridge metadata:
            "license_attribution_name": lic.get("license_attribution_name"),
            "license_attribution_url": lic.get("license_attribution_url"),
            "license_date_from_dtz": parse_utc(lic.get("license_date_from")),
            "license_date_to_dtz": parse_utc(lic.get("license_date_to")),
        }


def build_sensor_payloads(msg: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """
    Extract sensor fields from a producer event and transform them into the dict
    expected by the ensure_current_sensor() helper.

    Assumes the event schema produced by your Airflow @task for OpenAQ v3.
    """
    for s in msg.get("sensors") or []:
        sensor_id = s.get("id")
        try:
            sensor_id = int(sensor_id)
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"Invalid sensor id in payload: {s.get('id')!r}") from exc

        param = s.get("parameter") or {}
        yield {
            "openaq_sensors_id": sensor_id,
            "parameter_name": param.get("name"),
            "parameter_id": param.get("id"),
            "parameter_display_name": param.get("displayName"),
            "parameter_units": param.get("units"),
            # SCD-2 effective_* fields kept default for now (current-row upsert) until implemented.
            # "effective_start_at_dtz": ...,
            # "effective_end_at_dtz": ...,
            # "is_current": ...,
        }


def build_measurements_payload(msg: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """
    Extract measurement fields from a producer event and transform them into the dict
    expected by the ensure_current_measurements() helper.

    Assumes the event schema produced by your Airflow @task for OpenAQ v3.
    """
    meta: dict = {}
    meta = msg.get("meta")
    results: list = (msg.get("latest") or {}).get("results") or []

    for r in results:
        yield {
            "openaq_locations_id": r.get("locationsId"),
            "openaq_sensors_id": r.get("sensorsId"),
            "datetime_utc": (r.get("datetime") or {}).get("utc"),
            "value": r.get("value"),
            "source": meta.get("source"),
            "source_version": meta.get("version"),
            "ingested_at_dtz": utcnow(),
        }


class OpenAQPostgresSink(BasePostgresSink):
    """Sink implementation for OpenAQ events."""

    def __init__(self) -> None:
        config = SinkConfig(
            bootstrap=KAFKA_BOOTSTRAP,
            group_id=KAFKA_GROUP_ID,
            topic=KAFKA_TOPIC,
            dead_letter_topic=KAFKA_DEAD_LETTER_TOPIC,
            security_protocol=KAFKA_SECURITY_PROTOCOL,
            poll_timeout_sec=KAFKA_POLL_TIMEOUT_SEC,
        )
        super().__init__(config=config, logger=logger)

    def handle_event(self, payload: Dict[str, Any]) -> None:
        """Upsert current location, licenses (+bridge), sensors, and measurements."""
        loc_dict = build_location_payload(payload)
        lic_dicts = list(build_license_payloads(payload))
        sensor_dicts = list(build_sensor_payloads(payload))
        measures_dicts = list(build_measurements_payload(payload))

        with SessionLocal.begin() as session:
            # 1) upsert location + licenses (+ bridge)
            logger.info(f"Upserting location id={loc_dict['openaq_locations_id']} and related licenses")
            loc_row = upsert_location_with_licenses(session, loc_dict, lic_dicts)

            # 2) upsert sensors
            logger.info(f"Upserting {len(sensor_dicts)} sensors for location id={loc_row.locations_id_sk}")
            for s in sensor_dicts:
                ensure_current_sensors(session, s, location_sk=loc_row.locations_id_sk)

            # 3) upsert measurements
            logger.info(f"Upserting {len(measures_dicts)} measurements for location id={loc_row.locations_id_sk}")
            for m in measures_dicts:
                ensure_current_measurements(session, m)


def main() -> int:
    """Run the OpenAQ sink CLI entrypoint."""
    return OpenAQPostgresSink().run()


if __name__ == "__main__":
    raise SystemExit(main())
