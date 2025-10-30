import json
import logging
from types import SimpleNamespace

from weather_insight.sinks.base_postgres_sink import BasePostgresSink, SinkConfig


class DummySink(BasePostgresSink):
    def __init__(self):
        cfg = SinkConfig(
            bootstrap="kafka:9092",
            group_id="group",
            topic="topic",
            dead_letter_topic="dlq",
        )
        super().__init__(config=cfg, logger=logging.getLogger("dummy"))

    def handle_event(self, payload):
        raise NotImplementedError


class DummyProducer:
    def __init__(self):
        self.produced = []
        self.flushed = False

    def produce(self, topic, key, value):
        self.produced.append((topic, key, value))

    def flush(self, timeout):
        self.flushed = True
        self.timeout = timeout


def test_handle_processing_error_writes_dead_letter():
    sink = DummySink()
    producer = DummyProducer()
    msg = SimpleNamespace(
        topic=lambda: "src",
        partition=lambda: 0,
        offset=lambda: 1,
        key=lambda: b"k",
        value=lambda: b'{"bad": "json"}',
    )

    sink._handle_processing_error(ValueError("boom"), msg, dead_letter_producer=producer)  # noqa: SLF001

    assert producer.produced
    topic, key, value = producer.produced[0]
    assert topic == "dlq"
    payload = json.loads(value.decode("utf-8"))
    assert payload["error"] == "boom"
    assert producer.flushed
