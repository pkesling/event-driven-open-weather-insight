import types

from weather_insight.clients.http_session import configure_session


class DummySession:
    def __init__(self):
        self.headers = {}
        self.mounted = {}

    def mount(self, prefix, adapter):
        self.mounted[prefix] = adapter


def test_configure_session_sets_headers_and_timeout():
    sess = DummySession()

    def dummy_request(*args, **kwargs):
        return kwargs

    sess.request = dummy_request  # type: ignore[attr-defined]

    configured = configure_session(
        sess,
        headers={"X-Test": "1"},
        timeout_seconds=5,
        retries=1,
        retry_backoff_seconds=0.1,
        allowed_methods=("GET",),
    )

    # Headers applied
    assert configured.headers["X-Test"] == "1"
    # Mounts set for both schemes
    assert "https://" in configured.mounted
    assert "http://" in configured.mounted

    # Wrapped request should default timeout
    kwargs = configured.request("GET", "http://example.com")
    assert kwargs["timeout"] == 5
