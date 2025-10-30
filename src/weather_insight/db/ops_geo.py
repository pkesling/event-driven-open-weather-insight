# src/weather_insight/db/ops_openaq.py
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import select, func, and_
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select

from weather_insight.db.models.openaq import DimOpenAQLocation
from weather_insight.db.models.geo import DimGeoLocation


EARTH_RADIUS_KM = 6371.0


def build_canonical_name_for_openaq(loc: DimOpenAQLocation) -> str:
    """
    Simple heuristic for a human-readable name.
    You can tune this later or override per source.
    """
    parts = [
        loc.name or "",
        loc.locality or "",
        loc.country_name or loc.country_code or "",
    ]
    # Drop empties, join with " - "
    parts = [p for p in (p.strip() for p in parts) if p]
    return " - ".join(parts) if parts else f"OpenAQ location {loc.openaq_locations_id}"


def get_or_create_geo_for_openaq_location(
    session: Session,
    loc: DimOpenAQLocation,
    *,
    commit: bool = False,
) -> DimGeoLocation:
    """
    Ensure loc.geo_location_id_sk is set to a canonical DimGeoLocation,
    keyed by (latitude, longitude).

    Assumes loc.latitude / loc.longitude are populated.
    """
    if loc.latitude is None or loc.longitude is None:
        raise ValueError(
            f"OpenAQ location {loc.openaq_locations_id} missing lat/lon; "
            "cannot map to canonical geo location."
        )

    # Already linked? Just fetch and return.
    if loc.geo_location_id_sk is not None and loc.geo_location is not None:
        return loc.geo_location

    stmt = (
        select(DimGeoLocation)
        .where(
            DimGeoLocation.latitude == loc.latitude,
            DimGeoLocation.longitude == loc.longitude,
        )
        .limit(1)
    )
    geo: Optional[DimGeoLocation] = session.execute(stmt).scalar_one_or_none()

    if geo is None:
        geo = DimGeoLocation(
            latitude=loc.latitude,
            longitude=loc.longitude,
            canonical_name=build_canonical_name_for_openaq(loc),
            timezone=loc.timezone,
            country_code=loc.country_code,
            admin1_name=None,  # can be populated later via reverse-geocoding
            admin2_name=None,
        )
        session.add(geo)
        session.flush()  # assign geo_location_id_sk

    loc.geo_location = geo  # sets FK + relationship

    if commit:
        session.commit()

    return geo


@dataclass
class NearestGeoResult:
    geo: DimGeoLocation
    distance_km: float


def _distance_expr_km(lat: float, lon: float) -> Select:
    """
    Haversine-ish great-circle distance (km) between a constant (lat, lon)
    and DimGeoLocation.latitude/longitude.
    """
    lat_rad = func.radians(lat)
    lon_rad = func.radians(lon)
    geo_lat_rad = func.radians(DimGeoLocation.latitude)
    geo_lon_rad = func.radians(DimGeoLocation.longitude)

    return EARTH_RADIUS_KM * func.acos(
        func.cos(lat_rad) * func.cos(geo_lat_rad) *
        func.cos(geo_lon_rad - lon_rad) +
        func.sin(lat_rad) * func.sin(geo_lat_rad)
    )


def find_nearest_geo_location(
    session: Session,
    lat: float,
    lon: float,
    *,
    max_distance_km: float = 2.0,
    search_padding_deg: float = 0.05,
) -> Optional[NearestGeoResult]:
    """
    Find the nearest DimGeoLocation within `max_distance_km` of (lat, lon).

    - `search_padding_deg` is a quick bounding-box to avoid scanning the whole table.
      0.05 degrees ~ 5-6 km in latitude.
    """
    # First, cheap bounding-box filter
    bbox_filter = and_(
        DimGeoLocation.latitude.between(lat - search_padding_deg, lat + search_padding_deg),
        DimGeoLocation.longitude.between(lon - search_padding_deg, lon + search_padding_deg),
    )

    distance_expr = _distance_expr_km(lat, lon).label("distance_km")

    stmt = (
        select(DimGeoLocation, distance_expr)
        .where(bbox_filter)
        .order_by(distance_expr)
        .limit(1)
    )

    row = session.execute(stmt).first()
    if row is None:
        return None

    geo, distance_km = row[0], float(row[1])
    if distance_km > max_distance_km:
        return None

    return NearestGeoResult(geo=geo, distance_km=distance_km)
