import logging

from weather_insight.utils import logging_utils as lu


def test_build_logging_config_contains_handlers():
    config = lu.build_logging_config(level="DEBUG", job_name="test")
    assert "handlers" in config
    assert "stdout" in config["handlers"]
    assert config["root"]["level"] == "DEBUG"


def test_get_tagged_logger_injects_tag(caplog):
    caplog.set_level(logging.INFO)
    logger = lu.get_tagged_logger("weather_insight.test", tag="unit")
    logger.info("hello")
    assert any("hello" in message for message in caplog.messages)
