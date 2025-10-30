"""Airflow DAG to ingest hourly weather.gov forecasts into Kafka."""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os, json
from confluent_kafka import Producer
from weather_insight.clients.weather_client import make_weather_gov_client_from_env
from weather_insight.models.events import validate_weather_events

from weather_insight.utils.logging_utils import get_tagged_logger, setup_logging
setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
logger = get_tagged_logger(__name__, tag="weather_forecast_dag")

KAFKA_TOPIC = os.getenv('WEATHERGOV_KAFKA_TOPIC', 'raw.weather.hourly_forecast')


# DAG to fetch hourly weather forecast from api.weather.gov and publish to Kafka.
@dag(
    dag_id="weather_forecast_ingest",
    description="Fetch hourly weather forecast from weather.gov and publish to Kafka",
    schedule="0 * * * *",  # hourly
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=5)},
    tags=["weather", "kafka", "nws"],
)
def weather_forecast_ingest():
    """Fetch the hourly weather forecast from api.weather.gov and publish to Kafka."""
    @task()
    def t_fetch_forecast_events() -> list[dict]:
        """Fetch the hourly forecast from weather.gov and return a list of events."""
        logger.info("Fetching hourly forecast from weather.gov APIs")
        lat = float(os.environ["HOME_LAT"])
        lon = float(os.environ["HOME_LON"])

        client = make_weather_gov_client_from_env()
        meta = client.get_forecast_urls(lat, lon)
        raw = client.fetch_hourly_forecast(meta["forecast_hourly"])
        events = validate_weather_events(client.build_forecast_events(meta, raw))

        logger.info(f"Built {len(events)} forecast events")
        if events:
            logger.debug(f"Sample event: {events[0]}")
        return events


    @task()
    def t_publish_forecast_events(events: list[dict]) -> int:
        """Publish forecast events to Kafka."""
        if not events:
            logger.warning("No forecast events to publish")
            return 0

        bootstrap = os.environ.get("KAFKA_BOOTSTRAP") or "kafka:9092"
        producer = Producer({"bootstrap.servers": bootstrap})
        logger.info(f"Publishing {len(events)} forecast events to Kafka topic '{KAFKA_TOPIC}'")
        logger.debug(f"Connecting to Kafka at {bootstrap}")

        def _delivery_report(err, msg):
            if err:
                logger.error(f"Delivery failed for key={msg.key()} err={err}")
            else:
                logger.debug(
                    f"Delivered message to {msg.topic()} [{msg.partition()}] @ {msg.offset()}"
                )

        produced = 0
        for ev in events:
            payload = json.dumps(ev).encode("utf-8")
            producer.produce(KAFKA_TOPIC, payload, callback=_delivery_report)
            produced += 1
        producer.flush()
        logger.info(f"Produced {produced} messages")

        return produced

    evs = t_fetch_forecast_events()
    t_publish_forecast_events(evs)

weather_forecast_ingest()
