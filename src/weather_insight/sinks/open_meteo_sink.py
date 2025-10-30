"""Kafka sink that loads open-meteo current/forecast records into Postgres."""
import os
from typing import Any, Dict

from weather_insight.db import SessionLocal
from weather_insight.db.ops_open_meteo import ensure_open_meteo_weather_record, ensure_open_meteo_air_record
from weather_insight.db.utils import parse_utc
from weather_insight.sinks.base_postgres_sink import BasePostgresSink, SinkConfig
from weather_insight.utils.logging_utils import get_tagged_logger, setup_logging

# setup logging
setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
logger = get_tagged_logger(__name__, tag="open_meteo_postgres_sink")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "weather-postgres-sink")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_OPEN_METEO", "raw.open_meteo.weather_record")
KAFKA_DEAD_LETTER_TOPIC = os.environ.get("KAFKA_TOPIC_OPEN_METEO_DLT", "raw.open_meteo.weather_record.dlq")
KAFKA_SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_POLL_TIMEOUT_SEC = float(os.environ.get("KAFKA_POLL_TIMEOUT_SEC", "1.0"))


class OpenMeteoUnknownRecordTypeException(Exception):
    """Raised when an unknown record type is encountered."""
    pass


def build_weather_record_from_event(event: dict) -> dict:
    """Build a staging weather record from an Open-Meteo ingest event."""
    start_time = parse_utc(event.get("open_meteo_start_time"))
    end_time = parse_utc(event.get("open_meteo_end_time"))
    event_id = (
        event.get("event_id")
        or f"open-meteo:weather:{event['latitude']}:{event['longitude']}:{start_time.isoformat()}"
    )

    weather_record = {
        "event_id": event_id,

        "open_meteo_start_time": start_time,
        "open_meteo_end_time": end_time,

        "record_type": event.get("record_type"),
        "record_frequency_min": event.get("record_frequency_min"),

        "latitude": event.get("latitude"),
        "longitude": event.get("longitude"),

        "temperature": event.get("temperature"),
        "temperature_unit": event.get("temperature_unit"),

        "rel_humidity": event.get("rel_humidity"),
        "rel_humidity_unit": event.get("rel_humidity_unit"),

        "dew_point": event.get("dew_point"),
        "dew_point_unit": event.get("dew_point_unit"),

        "apparent_temperature": event.get("apparent_temperature"),
        "apparent_temperature_unit": event.get("apparent_temperature_unit"),

        "precipitation_prob": event.get("precipitation_prob"),
        "precipitation_prob_unit": event.get("precipitation_prob_unit"),

        "precipitation": event.get("precipitation"),
        "precipitation_unit": event.get("precipitation_unit"),

        "cloud_cover": event.get("cloud_cover"),
        "cloud_cover_unit": event.get("cloud_cover_unit"),

        "wind_speed": event.get("wind_speed"),
        "wind_speed_unit": event.get("wind_speed_unit"),

        "wind_gusts": event.get("wind_gusts"),
        "wind_gusts_unit": event.get("wind_gusts_unit"),

        "wind_direction": event.get("wind_direction"),
        "wind_direction_unit": event.get("wind_direction_unit"),

        "is_day": event.get("is_day"),

        "ingested_at_dtz": parse_utc(event.get("ingested_at_dtz")),
    }

    return weather_record


def build_air_quality_record_from_event(event: dict) -> dict:
    """Build a staging air-quality record from an Open-Meteo ingest event."""
    start_time = parse_utc(event.get("open_meteo_start_time"))
    end_time = parse_utc(event.get("open_meteo_end_time"))
    event_id = (
        event.get("event_id")
        or f"open-meteo:air_quality:{event['latitude']}:{event['longitude']}:{start_time.isoformat()}"
    )

    air_quality_record = {
        "event_id": event_id,

        "open_meteo_start_time": start_time,
        "open_meteo_end_time": end_time,

        "record_type": event.get("record_type"),
        "record_frequency_min": event.get("record_frequency_min"),

        "latitude": event.get("latitude"),
        "longitude": event.get("longitude"),

        "pm2_5": event.get("pm2_5"),
        "pm2_5_unit": event.get("pm2_5_unit"),

        "pm10": event.get("pm10"),
        "pm10_unit": event.get("pm10_unit"),

        "us_aqi": event.get("us_aqi"),
        "us_aqi_unit": event.get("us_aqi_unit"),

        "ozone": event.get("ozone"),
        "ozone_unit": event.get("ozone_unit"),

        "uv_index": event.get("uv_index"),
        "uv_index_unit": event.get("uv_index_unit"),

        "ingested_at_dtz": parse_utc(event.get("ingested_at_dtz")),
    }

    return air_quality_record


class OpenMeteoPostgresSink(BasePostgresSink):
    """Sink implementation for open-meteo weather forecasts and air quality forecasts."""

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
        record_kind = payload.get("data_kind") or payload.get("record_type")
        if record_kind == "weather":
            event_dict = build_weather_record_from_event(payload)
            with SessionLocal() as session:
                logger.info(f"Upserting weather record for location: {event_dict['latitude']}, {event_dict['longitude']}")
                logger.info(f"Upserting weather record for event_id='{event_dict['event_id']}'")

                ensure_open_meteo_weather_record(session, event_dict)
                session.commit()
        elif record_kind == "air_quality":
            event_dict = build_air_quality_record_from_event(payload)
            with SessionLocal() as session:
                logger.info(f"Upserting air quality record for location: {event_dict['latitude']}, {event_dict['longitude']}")
                logger.info(f"Upserting air quality record for event_id='{event_dict['event_id']}'")

                ensure_open_meteo_air_record(session, event_dict)
                session.commit()
        else:
            raise OpenMeteoUnknownRecordTypeException(f"Unknown record type: {record_kind}, event={payload}")


def main() -> int:
    """Run the open meteo sink CLI entrypoint."""
    return OpenMeteoPostgresSink().run()


if __name__ == "__main__":
    raise SystemExit(main())
