"""Kafka sink that loads AirNow forecasts into Postgres."""
import json
import os
from typing import Dict, Any

from weather_insight.db import SessionLocal
from weather_insight.db.ops_airnow import ensure_airnow_forecast_record
from weather_insight.db.utils import parse_utc
from weather_insight.sinks.base_postgres_sink import BasePostgresSink, SinkConfig
from weather_insight.utils.logging_utils import get_tagged_logger, setup_logging

"""
Initialization
"""

# setup logging
setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
logger = get_tagged_logger(__name__, tag="airnow_postgres_sink")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "airnow-postgres-sink")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_AIRNOW", "raw.airnow.air_quality_forecast")
KAFKA_DEAD_LETTER_TOPIC = os.environ.get("KAFKA_TOPIC_AIRNOW_DLT", "raw.airnow.air_quality_forecast.dlq")
KAFKA_SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_POLL_TIMEOUT_SEC = float(os.environ.get("KAFKA_POLL_TIMEOUT_SEC", "1.0"))


def build_forecast_record_from_event(event: dict) -> dict:
    """Build a staging AirNow forecast record from the ingest DAG event."""
    event_id = (event.get("event_id") or
                f"{event['source']}:{event['reporting_area'].replace(' ', '')}:"
                f"{parse_utc(event.get('date_issue')).isoformat()}:"
                f"{parse_utc(event.get('date_forecast')).isoformat()}")

    forecast_record = {
        "event_id": event_id,
        "source": event.get("source"),
        "date_issue_dtz": parse_utc(event.get("date_issue")),
        "date_forecast_dtz": parse_utc(event.get("date_forecast")),
        "reporting_area": event.get("reporting_area"),
        "state_code": event.get("state_code"),
        "latitude": event.get("latitude"),
        "longitude": event.get("longitude"),
        "parameter_name": event.get("parameter_name"),
        "aqi": event.get("aqi"),
        "category_number": event.get("category_number"),
        "category_name": event.get("category_name"),
        "action_day": event.get("action_day"),
        "discussion": event.get("discussion"),
        "raw_data": json.dumps(event.get("raw_data")),
    }

    return forecast_record


class AirNowPostgresSink(BasePostgresSink):
    """Sink implementation for airnow.gov air quality forecasts."""

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
        """Given a Kafka message, model the hourly forecast data and upsert in the database."""
        forecast_dict = build_forecast_record_from_event(payload)
        with SessionLocal.begin() as session:
            logger.info(f"Upserting air quality forecast record for event_id='{payload.get('event_id')}'")
            ensure_airnow_forecast_record(session, forecast_dict)


def main() -> int:
    """Run the AirNow sink CLI entrypoint."""
    return AirNowPostgresSink().run()


if __name__ == "__main__":
    raise SystemExit(main())
