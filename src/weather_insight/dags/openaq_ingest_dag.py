"""Airflow DAG to ingest latest OpenAQ readings and publish to Kafka."""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os
import json
from confluent_kafka import Producer

from weather_insight.clients.openaq_client import make_openaq_client_from_env
from weather_insight.utils.logging_utils import get_tagged_logger, setup_logging
setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
logger = get_tagged_logger(__name__, tag="openaq_ingest_dag")


@dag(
    dag_id="openaq_ingest",
    description="Find nearest OpenAQ location and publish its latest measurements to Kafka",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["opendata", "kafka", "openaq", "v3"],
)
def openaq_nearest_location_latest():
    """Fetch nearest OpenAQ measurements and push them to Kafka."""
    @task()
    def t_fetch():
        """Find the nearest location to the home coordinates."""
        lat = float(os.environ["HOME_LAT"])
        lon = float(os.environ["HOME_LON"])
        radius_m = float(os.environ.get("HOME_RADIUS_M", "5000"))
        logger.info(f"Fetching OpenAQ envelope for lat={lat} lon={lon} radius_m={radius_m}")
        client = make_openaq_client_from_env()
        envelope = client.fetch_nearest_location_latest(lat, lon, radius_m=radius_m)
        loc = envelope.get("location", {})
        logger.info(
            f"Fetched envelope: location_id={loc.get('id')} name={loc.get('name')}, "
            f"sensors={len(envelope.get('sensors') or [])}",
        )
        return envelope

    @task()
    def t_publish(envelope: dict) -> None:
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP") or "kafka:9092"
        topic = os.getenv('KAFKA_TOPIC_OPENAQ', 'raw.openaq.latest_by_location')
        logger.info(f"Publishing OpenAQ envelope to Kafka topic '{topic}'")
        logger.debug(f"Connecting to Kafka at {bootstrap}")

        # Keep keys stable (helps downstream consumers)
        location_id = (envelope.get("location") or {}).get("id")
        key_bytes = (str(location_id) if location_id is not None else "unknown").encode("utf-8")
        value_bytes = json.dumps(envelope, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

        producer = Producer({"bootstrap.servers": bootstrap})

        def _delivered(err, msg):
            if err:
                logger.error(f"Kafka delivery failed for key={msg.key()} err={err}")
            else:
                logger.debug(
                    f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}"
                )

        logger.info(f"Producing OpenAQ envelope to topic={topic} (bootstrap={bootstrap})")
        producer.produce(topic=topic, key=key_bytes, value=value_bytes, callback=_delivered)
        producer.flush(10)

    t_publish(t_fetch())

openaq_nearest_location_latest()
