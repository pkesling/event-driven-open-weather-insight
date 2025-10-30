"""Client wrapper for OpenAQ v3 with helpers to build Kafka payloads."""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import requests

from weather_insight.clients.http_session import configure_session
from weather_insight.utils.logging_utils import (
    get_tagged_logger,
    setup_logging,
)

logger = get_tagged_logger(__name__, tag="openaq_client")

DEFAULT_BASE_URL = "https://api.openaq.org/v3"
DEFAULT_USER_AGENT = "airflow/weather_client"
DEFAULT_TIMEOUT = 15  # seconds
DEFAULT_RETRIES = 3
DEFAULT_BACKOFF = 0.5  # seconds


class OpenAQClientError(RuntimeError):
    """Raised when OpenAQ API calls fail or return unexpected data."""


@dataclass
class OpenAQClientConfig:
    """
    Configuration for OpenAQClient.

    Attributes
    ----------
    base_url:
        Base URL for the OpenAQ API.
    user_agent:
        User-Agent header sent with requests.
    api_key:
        Optional API key used for authenticated requests.
    timeout_seconds:
        Default timeout applied to HTTP requests.
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


class OpenAQClient:
    """
    This wraps the OpenAQ HTTP calls used by downstream Airflow tasks and provides
    helpers that normalize OpenAQ payloads for Kafka events.
    """

    def __init__(
        self,
        config: OpenAQClientConfig,
        session: Optional[requests.Session] = None,
    ) -> None:
        self._config = config
        headers = {
            "Accept": "application/json",
            "User-Agent": self._config.user_agent,
        }
        if self._config.api_key:
            headers["X-API-Key"] = self._config.api_key

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
    def find_nearest_location(
        self,
        lat: float,
        lon: float,
        *,
        radius_m: float = 25.0,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """
        Query /locations near (lat, lon) and return the nearest location dict.
        """
        params = {
            "coordinates": f"{lat},{lon}",  # OpenAQ expects lat first.
            "radius": int(radius_m),
            "limit": limit,
        }
        url = f"{self._config.base_url.rstrip('/')}/locations"
        logger.info(f"Fetching locations from {url} with {params}")
        resp = self._session.get(url, params=params)
        resp.raise_for_status()

        data = resp.json()
        locations = data.get("results") or []
        if not locations:
            raise OpenAQClientError(
                "No locations returned for the given coordinates."
            )

        # API does not guarantee sort by distance → sort client-side
        locations.sort(key=lambda x: (x.get("distance") or float("inf")))
        nearest = locations[0]
        logger.info(
            f"Nearest location id={nearest.get('id')} "
            f"name={nearest.get('name')} "
            f"distance={(nearest.get('distance') or -1.0):.2f}",
        )
        return nearest

    def fetch_license_meta(self, license_id: int) -> Dict[str, Any]:
        """Call /licenses/{license_id} and return the license metadata."""
        url = f"{self._config.base_url.rstrip('/')}/licenses/{license_id}"
        logger.info(
            f"Fetching license information for openaq license id={license_id} from {url}",
        )
        resp = self._session.get(url, timeout=30)
        resp.raise_for_status()
        licenses = resp.json().get("results", [])
        logger.info(f"API call returned {len(licenses)} licenses ...")

        return licenses[0] if licenses else {}

    def get_latest_for_location(self, location_id: int | str) -> Dict[str, Any]:
        """
        GET /locations/{id}/latest (OpenAQ v3). Returns the full JSON body.
        """
        url = (
            f"{self._config.base_url.rstrip('/')}/locations/{location_id}/latest"
        )
        logger.info(f"Fetching latest measurements for location {location_id} from {url}")
        resp = self._session.get(url)
        resp.raise_for_status()
        latest_json = resp.json()
        logger.debug(f"Latest measurements: {json.dumps(latest_json, indent=4)}")
        return latest_json

    def build_latest_events_payload(
        self,
        nearest_location: Dict[str, Any],
        latest_json: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Build the event envelope published to Kafka with location, license,
        sensor, and latest reading details.
        """
        sensors_list = []
        for sid, meta in self.extract_sensor_map_from_location(
            nearest_location
        ).items():
            sensors_list.append(
                {
                    "id": sid,
                    "parameter": {
                        "name": meta.get("parameter_name"),
                        "id": meta.get("parameter_id"),
                        "displayName": meta.get("parameter_display_name"),
                        "units": meta.get("parameter_units"),
                    },
                }
            )

        logger.debug(f"Sensors list: {json.dumps(sensors_list, indent=4)}")
        locations = self.extract_location_minimal(nearest_location)
        logger.debug(f"Location: {json.dumps(locations, indent=4)}")
        licenses = self.extract_licenses_from_location(nearest_location)
        logger.debug(f"Licenses: {json.dumps(licenses, indent=4)}")
        envelope = {
            "location": locations,
            "licenses": licenses,
            "sensors": sensors_list,
            "latest": latest_json,
            "meta": {
                "source": "openaq",
                "event_type": "openaq.location.latest",
                "version": "v3",
                "builtAt": int(time.time()),
            },
        }

        logger.debug(f"Envelope: {json.dumps(envelope, indent=4)}")
        return envelope

    def extract_licenses_from_location(
        self,
        location: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        If the v3 location payload includes licenses, normalize them.
        """
        out: List[Dict[str, Any]] = []
        for lic in location.get("licenses") or []:
            license_id = lic.get("id") or None
            if not license_id:
                logger.warning("Found a license without an id. Skipping ...")
                continue

            logger.info(f"Fetching license metadata for license id={license_id}")
            license_meta = self.fetch_license_meta(license_id)
            attribution = lic.get("attribution") or {}

            out.append(
                {
                    "openaq_license_id": license_id,
                    "name": license_meta.get("name"),
                    "source_url": license_meta.get("sourceUrl"),
                    "commercial_use_allowed": (
                        license_meta.get("commercialUseAllowed") or False
                    ),
                    "attribution_required": (
                        license_meta.get("attributionRequired") or False
                    ),
                    "share_alike_required": (
                        license_meta.get("shareAlikeRequired") or False
                    ),
                    "modification_allowed": (
                        license_meta.get("modificationAllowed") or False
                    ),
                    "redistribution_allowed": (
                        license_meta.get("redistributionAllowed") or False
                    ),
                    "license_date_from": lic.get("dateFrom") or None,
                    "license_date_to": lic.get("dateTo") or None,
                    "license_attribution_name": attribution.get("name") or "",
                    "license_attribution_url": attribution.get("url") or "",
                }
            )

        logger.info(f"Extracted {len(out)} licenses ...")
        first_license = json.dumps(out[0], indent=4) if out else "None"
        logger.debug(f"First license: {first_license}")
        return out

    def fetch_nearest_location_latest(
        self,
        lat: float,
        lon: float,
        *,
        radius_m: float = 25000.0,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """
        High-level helper combining nearest location lookup + latest readings.
        """
        logger.info(
            f"Finding nearest location for lat={lat}, lon={lon}, radius={radius_m}m, limit={limit}",
        )
        nearest = self.find_nearest_location(
            lat,
            lon,
            radius_m=radius_m,
            limit=limit,
        )

        logger.info(
            f"Fetching latest measurements for location '{nearest['name']}' (id={nearest['id']})",
        )
        latest = self.get_latest_for_location(nearest["id"])
        logger.debug(f"Latest measurements: {json.dumps(latest, indent=4)}")

        return self.build_latest_events_payload(nearest, latest)

    # ------------------------------------------------------------------ #
    # Static helpers                                                     #
    # ------------------------------------------------------------------ #
    @staticmethod
    def extract_sensor_map_from_location(
        location: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Build a {sensor_id: {...}} map from the location object so we do not
        need an extra API call. IDs are coerced to strings.
        """
        sensor_map: Dict[str, Dict[str, Any]] = {}
        sensors: Iterable[Dict[str, Any]] = location.get("sensors") or []
        for sensor in sensors:
            sid_raw = sensor.get("id")
            sid = str(sid_raw) if sid_raw is not None else None
            if not sid:
                continue
            param = sensor.get("parameter") or {}
            sensor_map[sid] = {
                "id": sid,
                "parameter_name": param.get("name"),
                "parameter_id": param.get("id"),
                "parameter_display_name": param.get("displayName"),
                "parameter_units": param.get("units"),
            }
        return sensor_map

    @staticmethod
    def extract_location_minimal(location: Dict[str, Any]) -> Dict[str, Any]:
        """
        Pick the fields our sink/models care about. Leave timestamps as strings.
        """
        country = location.get("country") or {}
        owner = location.get("owner") or {}
        provider = location.get("provider") or {}
        coords = location.get("coordinates") or {}
        out = {
            "id": location.get("id"),
            "name": location.get("name"),
            "locality": location.get("locality"),
            "timezone": location.get("timezone"),
            "country": {
                "id": country.get("id"),
                "code": country.get("code"),
                "name": country.get("name"),
            },
            "owner": {
                "id": owner.get("id"),
                "name": owner.get("name"),
            },
            "provider": {
                "id": provider.get("id"),
                "name": provider.get("name"),
            },
            "coordinates": {
                "latitude": coords.get("latitude"),
                "longitude": coords.get("longitude"),
            },
            "firstSeenAt": (location.get("datetimeFirst") or {}).get("utc"),
            "lastSeenAt": (location.get("datetimeLast") or {}).get("utc"),
        }
        return out


def make_openaq_client_from_env(
    session: Optional[requests.Session] = None,
) -> OpenAQClient:
    """
    Convenient factory to construct an OpenAQ client using environment variables.
    """
    config = OpenAQClientConfig(
        base_url=os.getenv("OPENAQ_BASE_URL", DEFAULT_BASE_URL),
        user_agent=os.getenv("OPENAQ_USER_AGENT", DEFAULT_USER_AGENT),
        api_key=os.getenv("OPENAQ_API_KEY"),
    )
    return OpenAQClient(config=config, session=session)


def main() -> None:
    """Manual test helper to fetch and print the nearest location data."""
    setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
    lat = float(os.environ.get("HOME_LAT", "43.0731"))
    lon = float(os.environ.get("HOME_LON", "-89.4012"))
    radius_m = int(os.environ.get("HOME_RADIUS_M", "25000"))

    client = make_openaq_client_from_env()
    events = client.fetch_nearest_location_latest(lat, lon, radius_m=radius_m)
    logger.info(f"Event: {json.dumps(events, indent=4)}")


if __name__ == "__main__":
    main()
