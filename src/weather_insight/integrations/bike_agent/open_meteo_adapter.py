"""Postgres-backed Open-Meteo reader for the BikeAgent project.

This adapter mirrors the live Open-Meteo API payload shape but serves the data
from the warehouse. BikeAgent can import this module and swap the live API
client for the warehouse-backed source by calling ``fetch_weather_and_air``.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Mapping, Optional

from sqlalchemy import and_, between, create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from weather_insight.db.models import StgOpenMeteoAir, StgOpenMeteoWeather


@dataclass
class OpenMeteoWarehouseAdapter:
    """Lightweight helper to read Open-Meteo weather + air quality from Postgres."""

    engine: Engine
    location_tolerance: float = 0.05  # ~5km latitude variance

    @classmethod
    def from_url(cls, database_url: str, *, location_tolerance: float = 0.05) -> "OpenMeteoWarehouseAdapter":
        """Construct an adapter from a database URL."""
        engine = create_engine(database_url, future=True)
        return cls(engine=engine, location_tolerance=location_tolerance)

    def _location_filters(self, lat: float, lon: float):
        return (
            between(StgOpenMeteoWeather.latitude, lat - self.location_tolerance, lat + self.location_tolerance),
            between(StgOpenMeteoWeather.longitude, lon - self.location_tolerance, lon + self.location_tolerance),
        )

    def fetch_weather_and_air(
        self,
        latitude: float,
        longitude: float,
        *,
        hours_ahead: int = 24,
        start_utc: Optional[datetime] = None,
    ) -> List[Mapping[str, object]]:
        """
        Return combined weather + air-quality rows for the requested window.

        Rows are ordered by start time and include both the raw values and their units
        so BikeAgent can decide how/if to convert them.
        """
        start = start_utc or datetime.now(timezone.utc)
        end = start + timedelta(hours=hours_ahead)

        with Session(self.engine) as session:
            weather_rows = (
                session.execute(
                    select(StgOpenMeteoWeather)
                    .where(
                        and_(
                            *self._location_filters(latitude, longitude),
                            StgOpenMeteoWeather.open_meteo_start_time >= start,
                            StgOpenMeteoWeather.open_meteo_start_time < end,
                        )
                    )
                    .order_by(StgOpenMeteoWeather.open_meteo_start_time)
                )
                .scalars()
                .all()
            )
            air_rows = (
                session.execute(
                    select(StgOpenMeteoAir).where(
                        and_(
                            between(StgOpenMeteoAir.latitude, latitude - self.location_tolerance, latitude + self.location_tolerance),
                            between(StgOpenMeteoAir.longitude, longitude - self.location_tolerance, longitude + self.location_tolerance),
                            StgOpenMeteoAir.open_meteo_start_time >= start,
                            StgOpenMeteoAir.open_meteo_start_time < end,
                        )
                    )
                )
                .scalars()
                .all()
            )

        return self._merge_weather_and_air(weather_rows, air_rows)

    @staticmethod
    def _merge_weather_and_air(
        weather_rows: Iterable[StgOpenMeteoWeather],
        air_rows: Iterable[StgOpenMeteoAir],
    ) -> List[Mapping[str, object]]:
        """Combine weather + air rows on start time for convenience."""
        air_by_time = {
            row.open_meteo_start_time: row
            for row in air_rows
        }

        merged: List[Mapping[str, object]] = []
        for w in weather_rows:
            a = air_by_time.get(w.open_meteo_start_time)
            merged.append(
                {
                    "start_time_utc": w.open_meteo_start_time,
                    "end_time_utc": w.open_meteo_end_time,
                    "record_type": w.record_type,
                    "latitude": w.latitude,
                    "longitude": w.longitude,
                    "temperature": w.temperature,
                    "temperature_unit": w.temperature_unit,
                    "rel_humidity": w.rel_humidity,
                    "rel_humidity_unit": w.rel_humidity_unit,
                    "dew_point": w.dew_point,
                    "dew_point_unit": w.dew_point_unit,
                    "apparent_temperature": w.apparent_temperature,
                    "apparent_temperature_unit": w.apparent_temperature_unit,
                    "precipitation_prob": w.precipitation_prob,
                    "precipitation_prob_unit": w.precipitation_prob_unit,
                    "precipitation": w.precipitation,
                    "precipitation_unit": w.precipitation_unit,
                    "cloud_cover": w.cloud_cover,
                    "cloud_cover_unit": w.cloud_cover_unit,
                    "wind_speed": w.wind_speed,
                    "wind_speed_unit": w.wind_speed_unit,
                    "wind_gusts": w.wind_gusts,
                    "wind_gusts_unit": w.wind_gusts_unit,
                    "wind_direction": w.wind_direction,
                    "wind_direction_unit": w.wind_direction_unit,
                    "is_day": w.is_day,
                    "pm2_5": a.pm2_5 if a else None,
                    "pm2_5_unit": a.pm2_5_unit if a else None,
                    "pm10": a.pm10 if a else None,
                    "pm10_unit": a.pm10_unit if a else None,
                    "us_aqi": a.us_aqi if a else None,
                    "us_aqi_unit": a.us_aqi_unit if a else None,
                    "ozone": a.ozone if a else None,
                    "ozone_unit": a.ozone_unit if a else None,
                    "uv_index": a.uv_index if a else None,
                    "uv_index_unit": a.uv_index_unit if a else None,
                }
            )
        return merged
