from datetime import datetime, timezone

import pytest

from weather_insight.db import utils


def test_parse_utc_handles_str_and_naive_datetime():
    dt = utils.parse_utc("2024-01-01T12:00:00Z")
    assert dt.tzinfo == timezone.utc
    naive = datetime(2024, 1, 1, 12, 0, 0)
    dt2 = utils.parse_utc(naive)
    assert dt2.tzinfo == timezone.utc


def test_parse_utc_handles_timestamp():
    stamp = utils.parse_utc(0)
    assert stamp.year == 1970


def test_parse_utc_rejects_unknown_types():
    with pytest.raises(TypeError):
        utils.parse_utc(object())
