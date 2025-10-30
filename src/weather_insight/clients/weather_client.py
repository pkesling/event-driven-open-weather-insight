"""Client utilities for interacting with the weather.gov API."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from weather_insight.clients.http_session import configure_session
from weather_insight.utils.logging_utils import (
    get_tagged_logger,
    setup_logging,
)

logger = get_tagged_logger(__name__, tag="weather_client")

DEFAULT_BASE_URL = "https://api.weather.gov"
DEFAULT_USER_AGENT = "airflow/weather_client"
DEFAULT_TIMEOUT = 15  # seconds
DEFAULT_RETRIES = 3
DEFAULT_BACKOFF = 0.5  # seconds
DEFAULT_FEATURE_FLAGS = [
    "forecast_temperature_qv",
    "forecast_wind_speed_qv",
]


class WeatherGovClientError(RuntimeError):
    """Raised when weather.gov API calls fail or return unexpected data."""


@dataclass
class WeatherGovClientConfig:
    """
    Configuration container for WeatherGovClient.

    Attributes
    ----------
    base_url:
        Base URL for the weather.gov API.
    user_agent:
        User-Agent header sent on every request.
    feature_flags:
        Optional Feature-Flags header used to enable hourly forecast extras.
    timeout_seconds:
        Default HTTP timeout for requests.
    retries:
        Number of retry attempts for transient 5xx/429 errors.
    retry_backoff_seconds:
        Backoff factor for the retry adapter.
    """

    base_url: str = DEFAULT_BASE_URL
    user_agent: str = DEFAULT_USER_AGENT
    feature_flags: List[str] = field(default_factory=lambda: list(DEFAULT_FEATURE_FLAGS))
    timeout_seconds: int = DEFAULT_TIMEOUT
    retries: int = DEFAULT_RETRIES
    retry_backoff_seconds: float = DEFAULT_BACKOFF


class WeatherGovClient:
    """
    Dataclass-driven client for weather.gov that mirrors the legacy helpers.
    """

    def __init__(
        self,
        config: WeatherGovClientConfig,
        session: Optional[requests.Session] = None,
    ) -> None:
        self._config = config
        headers = {
            "User-Agent": self._config.user_agent,
        }
        if self._config.feature_flags:
            headers["Feature-Flags"] = ",".join(self._config.feature_flags)

        self._session = configure_session(
            session or requests.Session(),
            headers=headers,
            timeout_seconds=self._config.timeout_seconds,
            retries=self._config.retries,
            retry_backoff_seconds=self._config.retry_backoff_seconds,
        )

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #

    def get_forecast_urls(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Call /points/{lat},{lon} and return the forecast and hourly forecast URLs.
        """
        url = f"{self._config.base_url.rstrip('/')}/points/{lat},{lon}"
        logger.info(
            f"Fetching forecast URLs for lat={lat}, lon={lon} from {url}",
        )
        try:
            resp = self._session.get(url)
            resp.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            logger.error(f"Error fetching forecast URLs: {exc}")
            raise WeatherGovClientError(
                f"Failed to fetch forecast URLs: {exc}"
            ) from exc

        props = resp.json().get("properties", {})
        return {
            "forecast": props.get("forecast"),
            "forecast_hourly": props.get("forecastHourly"),
            "grid_id": props.get("gridId"),
            "grid_x": props.get("gridX"),
            "grid_y": props.get("gridY"),
            "office": props.get("cwa"),
        }

    def fetch_hourly_forecast(self, forecast_hourly_url: str) -> Dict[str, Any]:
        """Fetch the hourly forecast from the given URL."""
        logger.info(f"Fetching hourly forecast from {forecast_hourly_url}")
        try:
            resp = self._session.get(forecast_hourly_url)
            resp.raise_for_status()
        except requests.exceptions.HTTPError as exc:
            logger.error(f"Error fetching hourly forecast: {exc}")
            raise WeatherGovClientError(
                f"Failed to fetch hourly forecast: {exc}"
            ) from exc
        return resp.json()

    def build_forecast_events(
        self,
        meta: Dict[str, Any],
        raw: Dict[str, Any],
        ingest_dt: datetime | None = None,
    ) -> List[Dict[str, Any]]:
        """Enrich the hourly forecast with metadata into event payloads."""
        if ingest_dt is None:
            ingest_dt = datetime.now(tz=timezone.utc)

        ingest_dt_str = ingest_dt.astimezone(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        quant_fields = [
            "temperature",
            "probabilityOfPrecipitation",
            "relativeHumidity",
            "dewpoint",
            "windSpeed",
            "windGust",
        ]
        periods = (raw.get("properties") or {}).get("periods") or []
        events: List[Dict[str, Any]] = []
        for period in periods:
            start = period.get("startTime")
            end = period.get("endTime")
            grid_id = meta.get('grid_id', 'unknown')
            grid_x = meta.get('grid_x', 'unknown')
            grid_y = meta.get('grid_y', 'unknown')

            forecast_id = f"{grid_id}:{grid_x},{grid_y}:{start}-{end}"

            event: Dict[str, Any] = {
                "event_id": forecast_id,
                "event_type": "nws.hourly_forecast",
                "ingested_at": ingest_dt_str,
                "source": "api.weather.gov",
                "office": meta.get("office"),
                "grid_id": meta.get("grid_id"),
                "grid_x": meta.get("grid_x"),
                "grid_y": meta.get("grid_y"),
                "start_time": start,
                "end_time": period.get("endTime"),
                "is_daytime": period.get("isDaytime"),
                "temperature_trend": period.get("temperatureTrend"),
                "wind_direction": period.get("windDirection"),
                "short_forecast": period.get("shortForecast"),
                "detailed_forecast": period.get("detailedForecast"),
            }
            for qv in quant_fields:
                event.update(self.flatten_quantitative_dict(qv, period.get(qv)))

            event["ingested_at_dtz"] = ingest_dt_str
            events.append(event)

        return events

    # ------------------------------------------------------------------ #
    # Static helpers                                                     #
    # ------------------------------------------------------------------ #
    @staticmethod
    def flatten_quantitative_dict(
        name: str,
        obj: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Flatten quantitative values into a single dict."""

        def camel_to_snake(value: str) -> str:
            return re.sub(r"(?<!^)(?=[A-Z])", "_", value).lower()

        if not obj or not isinstance(obj, dict):
            return {}

        basename = camel_to_snake(name)
        return {
            basename: obj.get("value"),
            f"{basename}_min": obj.get("minValue"),
            f"{basename}_max": obj.get("maxValue"),
            f"{basename}_unit": obj.get("unitCode"),
        }

def make_weather_gov_client_from_env(
    session: Optional[requests.Session] = None,
) -> WeatherGovClient:
    """Convenient factory to construct a WeatherGov client using environment variables."""
    config = WeatherGovClientConfig(
        base_url=os.getenv("WEATHERGOV_BASE_URL", DEFAULT_BASE_URL),
        user_agent=os.getenv("WEATHERGOV_USER_AGENT", DEFAULT_USER_AGENT),
    )
    return WeatherGovClient(config=config, session=session)


def main() -> None:
    """Manual test helper to print forecast sample data."""
    setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))

    lat = float(os.environ.get("HOME_LAT", "43.0731"))
    lon = float(os.environ.get("HOME_LON", "-89.4012"))

    client = make_weather_gov_client_from_env()
    forecast_urls = client.get_forecast_urls(lat, lon)
    logger.info(f"Forecast URLs: {forecast_urls!r}")

    hourly_forecast = client.fetch_hourly_forecast(forecast_urls["forecast_hourly"])
    logger.info(f"Hourly forecast: {hourly_forecast!r}")

    events = client.build_forecast_events(forecast_urls, hourly_forecast)
    if events:
        logger.info(f"Events: {events[0]!r}")


if __name__ == "__main__":
    main()
