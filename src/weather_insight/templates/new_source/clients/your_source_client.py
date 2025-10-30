"""Stub API client for a new source.

This is a simple `requests`-based client. Replace `fetch_events` with calls
to your API and return a list of normalized event dicts.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import requests

from weather_insight.clients.http_session import configure_session


@dataclass
class YourSourceClientConfig:
    base_url: str
    timeout_seconds: int = 15
    retries: int = 3
    retry_backoff_seconds: float = 0.5
    user_agent: str = "airflow/your_source_client"


class YourSourceClient:
    def __init__(self, config: YourSourceClientConfig, session: requests.Session | None = None) -> None:
        self._config = config
        headers = {"User-Agent": config.user_agent}
        self._session = configure_session(
            session or requests.Session(),
            headers=headers,
            timeout_seconds=config.timeout_seconds,
            retries=config.retries,
            retry_backoff_seconds=config.retry_backoff_seconds,
        )

    def fetch_events(self, **kwargs) -> List[Dict[str, Any]]:
        """Return normalized event payloads."""
        # TODO: call your API, normalize to a stable internal schema
        raise NotImplementedError
