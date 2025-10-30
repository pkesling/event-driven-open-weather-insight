"""Lightweight requests.Session configuration helpers with retries and timeouts."""
from __future__ import annotations

from typing import Callable, Iterable, Mapping, ParamSpec, TypeVar

import requests
from requests.adapters import HTTPAdapter, Retry

P = ParamSpec("P")
R = TypeVar("R")


def configure_session(
    session: requests.Session,
    *,
    headers: Mapping[str, str] | None,
    timeout_seconds: int,
    retries: int,
    retry_backoff_seconds: float,
    allowed_methods: Iterable[str] = ("GET",),
    status_forcelist: Iterable[int] = (429, 500, 502, 503, 504),
) -> requests.Session:
    """Apply shared retry/timeout/header configuration to a requests session."""
    if headers:
        session.headers.update(headers)

    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=retry_backoff_seconds,
        status_forcelist=frozenset(status_forcelist),
        allowed_methods=frozenset(allowed_methods),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.request = _with_timeout(session.request, timeout_seconds)  # type: ignore[assignment]
    return session


def _with_timeout(fn: Callable[P, R], default_timeout: int) -> Callable[P, R]:
    """Wrap requests methods to default the timeout."""
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        kwargs.setdefault("timeout", default_timeout)
        return fn(*args, **kwargs)

    return wrapper
