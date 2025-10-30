"""Helper utilities and stubs for unit testing without external dependencies.

This module provides a set of utilities and lightweight stubs for mocking
libraries like Airflow, Confluent Kafka, or database sessions during unit
testing. These are intended to simplify testing in environments where the
actual dependencies are not present.

The module suppresses excessive logging noise during tests, initializes
minimal stub implementations for Airflow and Confluent Kafka, and provides
a dummy database session for test cases.

Classes:
    _NullHandler: No-op logging handler for silencing loggers during tests.
    DummyDag: Minimal implementation of an Airflow DAG object for testing.
    DummyTask: Representation of a generic task used in test DAGs.
    TaskDecorator: A simple task decorator for dummy tasks in Airflow stubs.
    DummyProducer: Stubbed implementation of a Kafka producer for tests.
    DummyConsumer: Stubbed implementation of a Kafka consumer for tests.
    KafkaError: Exception class for emulating Kafka errors during tests.
    KafkaException: Exception class for generic Kafka-related exceptions.
    DummyResult: A lightweight representation of a database result.
    DummySession: Database session stub for unit tests.

Fixtures:
    dummy_session: Pytest fixture that provides a dummy database session.
"""
import logging
import sys
import types
from dataclasses import dataclass, field
from typing import Any, List
import pytest


# ---------------------------------------------------------------------------
#
# ---------------------------------------------------------------------------

class _NullHandler(logging.Handler):
    """Configure logging so tests don't try writing to pytest's closed streams."""

    def emit(self, record):  # pragma: no cover
        """Ignore log records to keep test output clean."""
        pass


_null_handler = _NullHandler()
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.handlers = [_null_handler]

# Silence noisy third-party libraries (e.g., Airflow) during tests.
logging.getLogger("airflow").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Lightweight Airflow stub so DAG modules can be imported without the real pkg
# ---------------------------------------------------------------------------
try:  # pragma: no cover - exercised when Airflow is not installed
    import airflow  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - executed in dev/test envs
    airflow = types.ModuleType("airflow")
    decorators = types.ModuleType("airflow.decorators")
    _dag_stack: list[Any] = []

    class DummyDag:
        """Minimal DAG object used by tests."""

        def __init__(self, dag_id: str, **kwargs: Any) -> None:
            self.dag_id = dag_id
            self.kwargs = kwargs
            self.tasks: list[Any] = []

    def dag(**dag_kwargs: Any):
        """Return a decorator that registers a dummy DAG for test imports."""

        def decorator(func):
            def wrapper(*args: Any, **kwargs: Any) -> DummyDag:
                dag_obj = DummyDag(dag_kwargs.get("dag_id", func.__name__), **dag_kwargs)
                _dag_stack.append(dag_obj)
                try:
                    func(*args, **kwargs)
                finally:
                    _dag_stack.pop()
                return dag_obj

            wrapper.dag_id = dag_kwargs.get("dag_id", func.__name__)
            wrapper.__doc__ = func.__doc__
            return wrapper

        return decorator

    class DummyTask:
        """Represent a simple callable task used by dummy DAGs."""

        def __init__(self, func):
            self.func = func
            self.task_id = func.__name__

        def __call__(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
            if _dag_stack:
                _dag_stack[-1].tasks.append(self)
            return {"task_id": self.task_id, "args": args, "kwargs": kwargs}

    class TaskDecorator:
        """Mimic Airflow's task decorator for testing DAG modules."""

        def __call__(self, *dargs: Any, **dkwargs: Any):
            def decorator(func):
                return DummyTask(func)

            return decorator

        @property
        def bash(self):
            def decorator(func):
                return DummyTask(func)

            return decorator

    decorators.dag = dag
    decorators.task = TaskDecorator()
    airflow.decorators = decorators
    sys.modules["airflow"] = airflow
    sys.modules["airflow.decorators"] = decorators


# ---------------------------------------------------------------------------
# Minimal confluent_kafka stub (for local tests without Kafka bindings)
# ---------------------------------------------------------------------------

try:  # pragma: no cover - only executed when confluent_kafka missing
    import confluent_kafka  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    confluent_kafka = types.ModuleType("confluent_kafka")

    class DummyProducer:
        """Simplified Kafka producer that records produced messages."""

        def __init__(self, config=None):
            self.config = config
            self.messages: list[Any] = []

        def produce(self, *args, **kwargs):
            self.messages.append((args, kwargs))

        def flush(self, timeout=None):
            return 0

    class DummyConsumer:
        """Simplified Kafka consumer that tracks subscriptions."""

        def __init__(self, config=None):
            self.config = config
            self.subscriptions: list[Any] = []

        def subscribe(self, topics):
            self.subscriptions.extend(topics)

        def poll(self, timeout):
            return None

        def close(self):
            return None

        def commit(self, *_, **__):
            return None

    class KafkaError(Exception):
        UNKNOWN_TOPIC_OR_PART = 1

        def __init__(self, code=None):
            super().__init__(code)
            self._code = code

        def code(self):
            return self._code

    class KafkaException(Exception):
        pass

    confluent_kafka.Producer = DummyProducer
    confluent_kafka.Consumer = DummyConsumer
    confluent_kafka.KafkaError = KafkaError
    confluent_kafka.KafkaException = KafkaException
    sys.modules["confluent_kafka"] = confluent_kafka


# ---------------------------------------------------------------------------
# Helper session used by DB unit tests
# ---------------------------------------------------------------------------


@dataclass
class DummyResult:
    """Lightweight result wrapper that returns a configured scalar value."""

    value: Any

    def scalar_one_or_none(self):
        return self.value


@dataclass
class DummySession:
    """In-memory stub for a database session used in unit tests."""

    results: List[Any] = field(default_factory=list)
    added: List[Any] = field(default_factory=list)
    flushed: int = 0
    committed: int = 0

    def execute(self, _stmt):
        value: Any = self.results.pop(0) if self.results else None
        return DummyResult(value)

    def add(self, obj: Any):
        self.added.append(obj)

    def flush(self):
        self.flushed += 1

    def commit(self):
        self.committed += 1


@pytest.fixture
def dummy_session():
    """Provide a dummy database session fixture for unit tests."""
    return DummySession()
