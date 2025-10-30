"""Airflow DAG to ingest Open-Meteo weather + air-quality data into Kafka."""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from confluent_kafka import Producer

from weather_insight.clients.open_meteo_client import (
    DEFAULT_TZ,
    make_open_meteo_client_from_env,
)
from weather_insight.models.events import (
    validate_open_meteo_air_events,
    validate_open_meteo_weather_events,
)
from weather_insight.utils.logging_utils import get_tagged_logger, setup_logging

setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
logger = get_tagged_logger(__name__, tag="open_meteo_dag")

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_OPEN_METEO", "raw.open_meteo.weather_record")


@dag(
    dag_id="open_meteo_ingest",
    description="Fetch hourly weather + air quality from Open-Meteo and publish to Kafka",
    schedule="30 * * * *",  # every hour on the :30 to avoid clashing with other jobs
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=5)},
    tags=["weather", "air_quality", "open_meteo"],
)
def open_meteo_ingest():
    """Fetch Open-Meteo payloads and push them onto Kafka for sinks to consume."""

    @task()
    def t_fetch_events() -> dict:
        """Fetch weather + air-quality events for the configured home coordinates."""
        logger.info("Fetching Open-Meteo weather + air quality events")
        lat = float(os.environ["HOME_LAT"])
        lon = float(os.environ["HOME_LON"])
        tz_name = os.environ.get("HOME_TZ", DEFAULT_TZ)
        forecast_hours = int(os.environ.get("OPEN_METEO_FORECAST_HOURS", "48"))

        client = make_open_meteo_client_from_env()
        ingest_dt = datetime.now(tz=timezone.utc)

        weather_raw = client.fetch_weather(
            lat,
            lon,
            timezone=tz_name,
            forecast_hours=forecast_hours,
        )
        air_raw = client.fetch_air_quality(
            lat,
            lon,
            timezone=tz_name,
            forecast_hours=forecast_hours,
        )

        weather_events = validate_open_meteo_weather_events(
            client.build_weather_events(
                lat, lon, weather_raw, timezone=tz_name, ingest_dt=ingest_dt
            )
        )
        air_events = validate_open_meteo_air_events(
            client.build_air_quality_events(
                lat, lon, air_raw, timezone=tz_name, ingest_dt=ingest_dt
            )
        )

        logger.info(
            "Fetched %d weather and %d air-quality events",
            len(weather_events),
            len(air_events),
        )
        return {"weather": weather_events, "air_quality": air_events}

    @task()
    def t_publish_events(events: dict) -> int:
        """Publish both weather + air-quality payloads to Kafka."""
        all_events = (events.get("weather") or []) + (events.get("air_quality") or [])
        if not all_events:
            logger.warning("No Open-Meteo events to publish")
            return 0

        bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
        producer = Producer({"bootstrap.servers": bootstrap})
        logger.info(
            "Publishing %d Open-Meteo events to topic '%s'", len(all_events), KAFKA_TOPIC
        )

        def _delivery_report(err, msg):
            if err:
                logger.error("Delivery failed for key=%s err=%s", msg.key(), err)
            else:
                logger.debug(
                    "Delivered message to %s [%s] @ %s",
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )

        produced = 0
        for ev in all_events:
            payload = json.dumps(ev).encode("utf-8")
            producer.produce(KAFKA_TOPIC, payload, callback=_delivery_report)
            produced += 1
        producer.flush()
        logger.info("Produced %d Open-Meteo events", produced)
        return produced

    envelopes = t_fetch_events()
    t_publish_events(envelopes)


open_meteo_ingest()
