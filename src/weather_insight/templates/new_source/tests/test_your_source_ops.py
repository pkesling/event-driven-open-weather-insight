from weather_insight.db.ops_your_source import ensure_your_source_record
from weather_insight.db.models.your_source import StgYourSource


class DummySession:
    def __init__(self, result=None):
        self.result = result
        self.added = []

    def execute(self, _stmt):
        class R:
            def __init__(self, value):
                self.value = value

            def scalar_one_or_none(self):
                return self.value

        return R(self.result)

    def add(self, obj):
        self.added.append(obj)


def test_ensure_your_source_inserts_when_missing():
    payload = {
        "event_id": "evt-1",
        "source": "demo",
        "observed_at": None,
        "metric_value": 1.0,
        "metric_unit": "unit",
    }
    sess = DummySession(result=None)
    row = ensure_your_source_record(sess, payload)
    assert isinstance(row, StgYourSource)
    assert sess.added
