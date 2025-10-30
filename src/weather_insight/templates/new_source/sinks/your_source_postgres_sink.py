"""Kafka sink stub for your_source events.

Extends the shared `BasePostgresSink` (Kafka consumer loop) and performs
SQLAlchemy upserts via `ensure_your_source_record`.
"""
import os
from typing import Any, Dict

from weather_insight.db import SessionLocal
from weather_insight.db.ops_your_source import ensure_your_source_record
from weather_insight.sinks.base_postgres_sink import BasePostgresSink, SinkConfig
from weather_insight.utils.logging_utils import get_tagged_logger, setup_logging

setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
logger = get_tagged_logger(__name__, tag="your_source_postgres_sink")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "your-source-sink")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_YOUR_SOURCE", "raw.your_source.events")
KAFKA_DEAD_LETTER_TOPIC = os.environ.get("KAFKA_TOPIC_YOUR_SOURCE_DLT", "raw.your_source.events.dlq")
KAFKA_SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
KAFKA_POLL_TIMEOUT_SEC = float(os.environ.get("KAFKA_POLL_TIMEOUT_SEC", "1.0"))


class YourSourcePostgresSink(BasePostgresSink):
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
        with SessionLocal() as session:
            ensure_your_source_record(session, payload)
            session.commit()


def main() -> int:
    return YourSourcePostgresSink().run()


if __name__ == "__main__":
    raise SystemExit(main())
