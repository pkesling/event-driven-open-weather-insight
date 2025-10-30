"""Kafka sink that loads weather.gov forecasts into Postgres."""
import os
from typing import Dict, Any

from weather_insight.db import SessionLocal
from weather_insight.db.ops_weather import ensure_forecast_record
from weather_insight.db.utils import parse_utc
from weather_insight.sinks.base_postgres_sink import BasePostgresSink, SinkConfig
from weather_insight.utils.logging_utils import get_tagged_logger, setup_logging


"""
Initialization
"""

# setup logging
setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
logger = get_tagged_logger(__name__, tag="weather_postgres_sink")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "weather-postgres-sink")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_WEATHER", "raw.weather.hourly_forecast")
KAFKA_DEAD_LETTER_TOPIC = os.environ.get("KAFKA_TOPIC_WEATHER_DLT", "raw.weather.hourly_forecast.dlq")
KAFKA_SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_POLL_TIMEOUT_SEC = float(os.environ.get("KAFKA_POLL_TIMEOUT_SEC", "1.0"))


def build_forecast_record_from_event(event: dict) -> dict:
    """Build a staging forecast record from the weather_forecast_ingest DAG event."""
    event_id = (event.get("event_id") or
                f"{event['grid_id']}:{event['grid_x']},{event['grid_y']}:{event['start_time']}-{event['end_time']}")

    forecast_record = {
        "event_id": event_id,
        "event_type": event.get("event_type"),
        "source": event.get("source"),
        "office": event.get("office"),
        "grid_id": event.get("grid_id"),
        "grid_x": event.get("grid_x"),
        "grid_y": event.get("grid_y"),
        "start_time": parse_utc(event.get("start_time")),
        "end_time": parse_utc(event.get("end_time")),
        "is_daytime": event.get("is_daytime"),
        "temperature": event.get("temperature"),
        "temperature_min": event.get("temperature_min"),
        "temperature_max": event.get("temperature_max"),
        "temperature_unit": event.get("temperature_unit"),
        "temperature_trend": event.get("temperature_trend"),
        "relative_humidity": event.get("relative_humidity"),
        "relative_humidity_min": event.get("relative_humidity_min"),
        "relative_humidity_max": event.get("relative_humidity_max"),
        "relative_humidity_unit": event.get("relative_humidity_unit"),
        "dewpoint": event.get("dewpoint"),
        "dewpoint_min": event.get("dewpoint_min"),
        "dewpoint_max": event.get("dewpoint_max"),
        "dewpoint_unit": event.get("dewpoint_unit"),
        "wind_speed": event.get("wind_speed"),
        "wind_speed_min": event.get("wind_speed_min"),
        "wind_speed_max": event.get("wind_speed_max"),
        "wind_speed_unit": event.get("wind_speed_unit"),
        "wind_direction": event.get("wind_direction"),
        "wind_gust": event.get("wind_gust"),
        "wind_gust_min": event.get("wind_gust_min"),
        "wind_gust_max": event.get("wind_gust_max"),
        "wind_gust_unit": event.get("wind_gust_unit"),
        "probability_of_precipitation": event.get("probability_of_precipitation"),
        "probability_of_precipitation_min": event.get("probability_of_precipitation_min"),
        "probability_of_precipitation_max": event.get("probability_of_precipitation_max"),
        "probability_of_precipitation_unit": event.get("probability_of_precipitation_unit"),
        "short_forecast": event.get("short_forecast"),
        "detailed_forecast": event.get("detailed_forecast"),
        "ingested_at_dtz": parse_utc(event.get("ingested_at_dtz"))
    }
    
    return forecast_record


class WeatherPostgresSink(BasePostgresSink):
    """Sink implementation for weather.gov hourly forecasts."""

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
            logger.info(f"Upserting grid location: {forecast_dict['grid_id']}:{forecast_dict['grid_x']},{forecast_dict['grid_y']}")


            logger.info(f"Upserting hourly forecast record for event_id='{payload.get('event_id')}'")
            ensure_forecast_record(session, forecast_dict)


def main() -> int:
    """Run the weather sink CLI entrypoint."""
    return WeatherPostgresSink().run()


if __name__ == "__main__":
    raise SystemExit(main())
