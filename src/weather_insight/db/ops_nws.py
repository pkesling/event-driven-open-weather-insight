# src/weather_insight/db/ops_nws.py
from __future__ import annotations

from typing import Optional

from sqlalchemy.orm import Session

from weather_insight.db.models.nws import DimNWSGridpoint
from weather_insight.db.models.geo import DimGeoLocation
from weather_insight.db.ops_geo import find_nearest_geo_location


def build_canonical_name_for_nws_gridpoint(gp: DimNWSGridpoint) -> str:
    """
    Human-readable label for this gridpoint.
    You can get fancier later (e.g., city name, zone, etc.).
    """
    return f"NWS {gp.office_id} {gp.grid_x},{gp.grid_y}"


def get_or_create_geo_for_nws_gridpoint(
    session: Session,
    gp: DimNWSGridpoint,
    *,
    max_distance_km: float = 2.0,
    commit: bool = False,
) -> DimGeoLocation:
    """
    Ensure gp.geo_location_id_sk points to a canonical DimGeoLocation.

    Strategy:
    - If already linked, just return it.
    - Else, look for a nearby canonical geo row within `max_distance_km`.
    - If found, reuse it.
    - If not, create a new canonical geo row at the NWS lat/lon.
    """
    if gp.latitude is None or gp.longitude is None:
        raise ValueError(
            f"NWS gridpoint {gp.office_id} {gp.grid_x},{gp.grid_y} missing lat/lon; "
            "cannot map to canonical geo location."
        )

    # Already linked? great.
    if gp.geo_location_id_sk is not None and gp.geo_location is not None:
        return gp.geo_location

    # Try to find an existing canonical location nearby
    nearest = find_nearest_geo_location(
        session,
        gp.latitude,
        gp.longitude,
        max_distance_km=max_distance_km,
    )

    if nearest is not None:
        geo = nearest.geo
    else:
        # No nearby canonical location: create one
        geo = DimGeoLocation(
            latitude=gp.latitude,
            longitude=gp.longitude,
            canonical_name=build_canonical_name_for_nws_gridpoint(gp),
            timezone=gp.timezone,
            country_code=None,   # can be patched later via reverse-geocoding
            admin1_name=None,
            admin2_name=None,
        )
        session.add(geo)
        session.flush()  # assign geo_location_id_sk

    gp.geo_location = geo  # sets FK + relationship on the NWS side

    if commit:
        session.commit()

    return geo
