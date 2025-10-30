"""Airflow DAG to ingest 'your_source' data and publish to Kafka."""
from datetime import datetime, timedelta
import os
import json

from airflow.decorators import dag, task
from confluent_kafka import Producer

from weather_insight.clients.your_source_client import YourSourceClient, YourSourceClientConfig


KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_YOUR_SOURCE", "raw.your_source.events")


@dag(
    dag_id="your_source_ingest",
    description="Fetch your_source data and publish to Kafka",
    schedule="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=5)},
    tags=["your_source", "kafka"],
)
def your_source_ingest():
    @task()
    def t_fetch() -> list[dict]:
        cfg = YourSourceClientConfig(base_url=os.getenv("YOURSOURCE_BASE_URL", "https://api.your_source.com"))
        client = YourSourceClient(config=cfg)
        return client.fetch_events()

    @task()
    def t_publish(events: list[dict]) -> int:
        if not events:
            return 0
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP") or "kafka:9092"
        producer = Producer({"bootstrap.servers": bootstrap})
        for ev in events:
            producer.produce(KAFKA_TOPIC, json.dumps(ev).encode("utf-8"))
        producer.flush()
        return len(events)

    t_publish(t_fetch())


your_source_ingest()
