"""Client for AirNow forecasts with minimal normalization helpers."""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Mapping, Optional

import requests

from weather_insight.clients.http_session import configure_session
from weather_insight.utils.logging_utils import (
    get_tagged_logger,
    setup_logging,
)
logger = get_tagged_logger(__name__, tag="airnow_client")

DEFAULT_BASE_URL = "https://www.airnowapi.org"
DEFAULT_USER_AGENT = "airflow/airnow_client"
DEFAULT_TIMEOUT = 15  # seconds
DEFAULT_RETRIES = 3
DEFAULT_BACKOFF = 0.5  # seconds


class AirNowError(RuntimeError):
    """Raised when an AirNow API call fails or returns unexpected data."""


@dataclass
class AirNowClientConfig:
    """
    Configuration for AirNowClient.

    Attributes
    ----------
    base_url:
        Base URL for the AirNow API. The default points at the public API host.
    user_agent:
        User-Agent header sent with requests.
    api_key:
        Your AirNow API key. This is required.
    timeout_seconds:
        Default HTTP timeout for requests.
    retries:
        Number of retry attempts for transient errors.
    retry_backoff_seconds:
        Backoff factor between retries.
    """

    base_url: str = DEFAULT_BASE_URL
    user_agent: str = DEFAULT_USER_AGENT
    api_key: Optional[str] = None
    timeout_seconds: int = DEFAULT_TIMEOUT
    retries: int = DEFAULT_RETRIES
    retry_backoff_seconds: float = DEFAULT_BACKOFF


class AirNowClient:
    """
    Thin wrapper around the EPA AirNow API.

    Notes
    -----
    - AirNow observational and forecast data are *preliminary* and are not
      validated regulatory data. They are intended for real-time AQI reporting
      and public information, not for regulatory decisions or trend analysis.
    - You must follow the AirNow data use guidelines:
      * Do not alter official AQI values, categories, or advisory text.
      * Clearly attribute data to EPA AirNow and partner agencies.
      * Mark analyses and products as based on preliminary data.
    - This client is deliberately small and opinionated: we keep the HTTP
      mechanics here and normalize the response into a stable internal shape
      that downstream code can rely on.

    This client currently focuses on *forecast-by-lat/long* endpoints because
    they’re the most directly useful for ride planning. We can add observation
    endpoints and site metadata later.
    """

    def __init__(
        self,
        config: AirNowClientConfig,
        session: Optional[requests.Session] = None,
    ) -> None:
        self._config = config
        headers = {
            "Accept": "application/json",
            "User-Agent": self._config.user_agent,
        }

        self._session = configure_session(
            session or requests.Session(),
            headers=headers,
            timeout_seconds=self._config.timeout_seconds,
            retries=self._config.retries,
            retry_backoff_seconds=self._config.retry_backoff_seconds,
        )

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def get_daily_forecast_by_lat_lon(
        self,
        lat: float,
        lon: float,
        *,
        forecast_date: Optional[date] = None,
        distance_km: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Fetch daily AQI forecasts for a given latitude/longitude.

        Parameters
        ----------
        lat, lon:
            Latitude and longitude of the point of interest.
        forecast_date:
            Date for which to request the forecast. If None, uses today's date
            in the local AirNow sense (API expects a YYYY-MM-DD string).
        distance_km:
            Search radius in kilometers for matching forecast reporting areas.

        Returns
        -------
        List[Dict[str, Any]]
            A list of normalized forecast dicts. Each dict is *not* modified
            AQI data; we keep AQI values and categories as-is and just wrap
            them in a consistent schema for downstream use:

            {
                "source": "airnow",
                "date_issue": "YYYY-MM-DD",
                "date_forecast": "YYYY-MM-DD",
                "reporting_area": str,
                "state_code": str,
                "latitude": float | None,
                "longitude": float | None,
                "parameter_name": str | None,   # e.g., "O3", "PM2.5"
                "aqi": int | None,
                "category_number": int | None,
                "category_name": str | None,    # e.g., "Good", "Moderate"
                "action_day": bool | None,      # AirNow action-day flag
                "discussion": str | None,       # free-text forecast discussion
                "raw_data": dict,               # raw AirNow record for traceability
            }

        Raises
        ------
        AirNowError
            On HTTP failures, parse errors, or unexpected payload structure.
        """
        if forecast_date is None:
            forecast_date = date.today()

        date_str = forecast_date.strftime("%Y-%m-%d")

        logger.info(
            f"Requesting AirNow daily forecast for lat={lat}, lon={lon}, date={date_str}, distance_km={distance_km}",
        )

        params = {
            "latitude": lat,
            "longitude": lon,
            "date": date_str,
            "distance": distance_km,
        }

        raw = self._get("/aq/forecast/latLong/", params=params)

        # Some locations/days may have no forecast; that's a valid "empty list".
        if not raw:
            logger.info(
                f"AirNow returned no forecast records for lat={lat}, lon={lon}, date={date_str}",
            )
            return []

        normalized: List[Dict[str, Any]] = []
        for item in raw:
            try:
                normalized.append(self._normalize_forecast_item(item))
            except Exception as exc:  # noqa: BLE001
                # Be strict, but don't crash the whole run on a single bad record.
                logger.warning(
                    f"Skipping malformed AirNow forecast record: {item} (error={exc})",
                )

        return normalized

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _get(
        self,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        """Low-level GET wrapper with basic error handling and JSON parsing."""
        url = self._config.base_url.rstrip("/") + "/" + path.lstrip("/")
        q: Dict[str, Any] = {}

        if params:
            q.update(params)

        # AirNow API convention: `format=application/json` and `API_KEY`
        q.setdefault("format", "application/json")
        q.setdefault("API_KEY", self._config.api_key)

        logger.debug(f"AirNow GET {url}?{q}")
        try:
            resp = self._session.get(
                url,
                params=q,
                timeout=self._config.timeout_seconds,
            )
        except requests.RequestException as exc:
            logger.error(f"AirNow GET {url} failed at HTTP layer: {exc}")
            raise AirNowError(f"HTTP error calling AirNow: {exc}") from exc

        if not resp.ok:
            logger.error(
                f"AirNow GET {resp.url} failed with status={resp.status_code}, body={resp.text}",
            )
            raise AirNowError(
                f"AirNow API error: HTTP {resp.status_code} for {resp.url}"
            )

        try:
            data = resp.json()
        except ValueError as exc:
            logger.error(
                f"AirNow GET {resp.url} returned invalid JSON: {exc}"
            )
            raise AirNowError("Failed to parse AirNow JSON response") from exc

        return data

    @staticmethod
    def _normalize_forecast_item(item: Mapping[str, Any]) -> Dict[str, Any]:
        """
        Normalize a single AirNow forecast record into our internal schema.

        This assumes a structure similar to the documented AirNow forecast
        response, but uses .get() everywhere so minor schema changes are
        non-fatal.
        """
        # AirNow forecast fields described in docs and examples typically include:
        # - DateIssue: "YYYY-MM-DD"
        # - DateForecast: "YYYY-MM-DD"
        # - ReportingArea: "City / Region"
        # - StateCode: "XX"
        # - Latitude, Longitude: floats
        # - ParameterName: "O3", "PM2.5", etc.
        # - AQI: integer
        # - Category: { "Number": int, "Name": "Good" | "Moderate" | ... }
        # - ActionDay: "True"/"False" or boolean
        # - Discussion: free text
        category = item.get("Category") or {}

        # Some implementations return ActionDay as a string; normalize to bool|None.
        action_day_raw = item.get("ActionDay")
        if isinstance(action_day_raw, bool):
            action_day = action_day_raw
        elif isinstance(action_day_raw, str):
            action_day = action_day_raw.strip().lower() == "true"
        else:
            action_day = None

        source = 'airnow'
        reporting_area = item.get('ReportingArea', 'unknown')
        date_issue = item.get('DateIssue')
        date_forecast = item.get('DateForecast')
        event_id = (f"{source}:"
                    f"{reporting_area.replace(' ', '')}:"
                    f"{date_issue}:"
                    f"{date_forecast}")

        normalized: Dict[str, Any] = {
            "event_id": event_id,
            "source": source,
            "date_issue": date_issue,
            "date_forecast": date_forecast,
            "reporting_area": reporting_area,
            "state_code": item.get("StateCode"),
            "latitude": item.get("Latitude"),
            "longitude": item.get("Longitude"),
            "parameter_name": item.get("ParameterName"),
            "aqi": item.get("AQI"),
            "category_number": category.get("Number"),
            "category_name": category.get("Name"),
            "action_day": action_day,
            "discussion": item.get("Discussion"),
            # Keep raw record for traceability and future debugging:
            "raw_data": dict(item),
        }

        return normalized


def make_airnow_client_from_env(
    session: Optional[requests.Session] = None,
) -> AirNowClient:
    """
    Convenient factory to construct an AirNowClient using environment variables.

    Expected environment variables
    ------------------------------
    AIRNOW_API_KEY:
        Your AirNow API key. **Required**.
    AIRNOW_BASE_URL:
        Optional override for the base URL. Defaults to the standard public API
        host if not set.

    This is meant to mirror the pattern you're already using for other clients
    (e.g., OpenAQ, NWS).
    """
    api_key = os.getenv("AIRNOW_API_KEY")
    if not api_key:
        raise AirNowError(
            "AIRNOW_API_KEY environment variable must be set to use AirNowClient"
        )

    base_url = os.getenv("AIRNOW_BASE_URL", "https://www.airnowapi.org")

    config = AirNowClientConfig(
        api_key=api_key,
        base_url=base_url,
    )
    return AirNowClient(config=config, session=session)


def main():
    """Simple CLI helper to print a sample AirNow forecast."""
    import json
    setup_logging(level="DEBUG")
    client = make_airnow_client_from_env()
    print(json.dumps(client.get_daily_forecast_by_lat_lon(lat=43.0731, lon=-89.4012), indent=4))


if __name__ == "__main__":
    main()
