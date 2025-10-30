"""
Shared Kafka-to-Postgres sink infrastructure.

Provides a reusable base class that manages Kafka consumer lifecycle,
schema initialization, message polling, and dead-letter handling.
Source-specific sinks only need to implement ``handle_event`` to perform
their domain-specific database writes.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import signal
import time
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from sqlalchemy.exc import IntegrityError

from weather_insight.db import Base, ENGINE
from weather_insight.db.bootstrap import ensure_schemas


@dataclass
class SinkConfig:
    """Runtime configuration for a Kafka sink."""

    bootstrap: str
    group_id: str
    topic: str
    dead_letter_topic: str
    security_protocol: str = "PLAINTEXT"
    poll_timeout_sec: float = 1.0
    log_heartbeat_interval_sec: float = 30.0


class BasePostgresSink:
    """
    Implements the shared logic for wiring a Kafka consumer into Postgres.

    Subclasses are expected to override ``handle_event`` to perform their
    specific database writes.

    Test-only leniency:
    - Dead-letter handling is best-effort; if no producer is configured, the
      message is simply logged. This is acceptable in unit tests where Kafka
      bindings may be stubbed out.
    """

    def __init__(self, *, config: SinkConfig, logger):
        self.config = config
        self.logger = logger
        self._shutdown = False
        self._dead_letter_count = 0

    # --- Hooks for subclasses ------------------------------------------------
    def handle_event(self, payload: dict) -> None:  # pragma: no cover - base class hook
        raise NotImplementedError

    def init_schema(self) -> None:
        """Ensure that required schemas/tables exist before processing."""
        ensure_schemas(("ref", "stg"))
        try:
            Base.metadata.create_all(bind=ENGINE, checkfirst=True)
        except IntegrityError as exc:
            # Postgres raises duplicate type errors if the composite type already exists.
            if "pg_type_typname_nsp_index" in str(exc.orig):
                self.logger.warning(
                    "Schema creation skipped due to existing type; assuming tables already present.",
                    exc_info=False,
                )
            else:  # re-raise unexpected integrity errors
                raise

    # --- Internal helpers ----------------------------------------------------
    def _handle_signal(self, *_):
        self.logger.info("Shutdown requested; draining consumer...")
        self._shutdown = True

    def build_consumer(self) -> Consumer:
        """Build the Kafka consumer used throughout this code."""
        return Consumer(
            {
                "bootstrap.servers": self.config.bootstrap,
                "group.id": self.config.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "security.protocol": self.config.security_protocol,
            }
        )

    def _build_dead_letter_producer(self) -> Optional[Producer]:
        if not self.config.dead_letter_topic:
            return None
        return Producer({"bootstrap.servers": self.config.bootstrap})

    # --- Main execution ------------------------------------------------------
    def run(self) -> int:
        """Entry point used by ``main()`` in sink modules."""
        self.logger.info(
            f"Starting sink; topic='{self.config.topic}' bootstrap='{self.config.bootstrap}'",
        )
        self.logger.info("Verifying schemas exist and initializing if necessary.")
        self.init_schema()

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        consumer = self.build_consumer()
        consumer.subscribe([self.config.topic])
        dead_letter_producer = self._build_dead_letter_producer()

        messages_processed = 0
        try:
            last_log = time.time()
            while not self._shutdown:
                msg = consumer.poll(self.config.poll_timeout_sec)
                if msg is None:
                    # Idle heartbeat log every ~log_heartbeat_interval_sec.
                    if time.time() - last_log > self.config.log_heartbeat_interval_sec:
                        self.logger.info("Waiting for messages…")
                        last_log = time.time()
                    continue

                messages_processed += 1
                self.logger.info(f"Processing message: {messages_processed}")
                if msg.error():
                    if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        self.logger.warning(
                            f"WARNING: topic '{self.config.topic}' not found. Has it been initialized yet?",
                        )
                        continue
                    raise KafkaException(msg.error())

                try:
                    payload = json.loads(msg.value())
                except json.JSONDecodeError:
                    self.logger.exception(
                        f"ERROR: failed to decode JSON. Skipping... Raw={msg.value()!r}"
                    )
                    continue

                try:
                    self.handle_event(payload)
                    consumer.commit(message=msg, asynchronous=False)
                except Exception as exc:  # pylint: disable=broad-except
                    self._handle_processing_error(
                        exc, msg, dead_letter_producer=dead_letter_producer
                    )
        finally:
            try:
                consumer.close()
            except Exception:  # pragma: no cover - best effort
                pass

        self.logger.info(
            "Shut down.",
            extra={
                "messages_processed": messages_processed,
                "dead_letters": self._dead_letter_count,
            },
        )
        return 0

    def _handle_processing_error(self, error: Exception, msg, *, dead_letter_producer: Optional[Producer]) -> None:
        self.logger.exception(
            "Failed processing message",
            extra={
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": (msg.key() or b"").decode("utf-8", errors="replace"),
            },
        )

        if dead_letter_producer is None:
            return

        dead_letter_event = {
            "original_topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "error": str(error),
            "raw_value": msg.value().decode("utf-8", errors="replace"),
            "ts_utc": datetime.now(tz=timezone.utc).isoformat(),
        }
        dead_letter_producer.produce(
            self.config.dead_letter_topic,
            key=msg.key(),
            value=json.dumps(dead_letter_event).encode("utf-8"),
        )
        dead_letter_producer.flush(5)
        self._dead_letter_count += 1
        self.logger.warning(
            "Sent message to dead-letter topic",
            extra={
                "dead_letter_topic": self.config.dead_letter_topic,
                "dead_letters_so_far": self._dead_letter_count,
            },
        )
