from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, Float, Integer, String, Column, Text
from sqlalchemy.orm import Mapped
from sqlalchemy.sql import func

from weather_insight.db.models.base import Base


class StgOpenMeteoWeather(Base):
    """Normalized weather readings returned by Open-Meteo."""
    __tablename__ = "open_meteo_weather"
    __table_args__ = {"schema": "stg"}

    event_id: Mapped[str] = Column(Text, primary_key=True)

    # Core fields
    open_meteo_start_time: Mapped[datetime] = Column(DateTime(timezone=True), nullable=False, index=True)
    open_meteo_end_time: Mapped[datetime] = Column(DateTime(timezone=True), nullable=False, index=True)
    record_type: Mapped[str] = Column(String(32), nullable=False)  # current or forecast
    record_frequency_min: Mapped[int] = Column(Integer, nullable=False)  # 15, 60, 86,400

    latitude: Mapped[float] = Column(Float, nullable=False)
    longitude: Mapped[float] = Column(Float, nullable=False)

    temperature: Mapped[float] = Column(Float, nullable=False)
    temperature_unit: Mapped[str] = Column(String(32), nullable=False)

    rel_humidity: Mapped[Optional[float]] = Column(Float, nullable=True)
    rel_humidity_unit: Mapped[Optional[str]] = Column(String(32), nullable=True)

    dew_point: Mapped[Optional[float]] = Column(Float, nullable=True)
    dew_point_unit: Mapped[Optional[str]] = Column(String(32), nullable=True)

    apparent_temperature: Mapped[Optional[float]] = Column(Float, nullable=True)
    apparent_temperature_unit: Mapped[Optional[str]] = Column(String(32), nullable=True)

    precipitation_prob: Mapped[Optional[float]] = Column(Float, nullable=True)
    precipitation_prob_unit: Mapped[Optional[str]] = Column(String(32), nullable=True)

    precipitation: Mapped[Optional[float]] = Column(Float, nullable=True)
    precipitation_unit: Mapped[Optional[str]] = Column(String(32), nullable=True)

    cloud_cover: Mapped[Optional[float]] = Column(Float, nullable=True)
    cloud_cover_unit: Mapped[Optional[str]] = Column(String(32), nullable=True)

    wind_speed: Mapped[Optional[float]] = Column(Float, nullable=True)
    wind_speed_unit: Mapped[Optional[str]] = Column(String(32), nullable=True)

    wind_gusts: Mapped[Optional[float]] = Column(Float, nullable=True)
    wind_gusts_unit: Mapped[Optional[str]] = Column(String(32), nullable=True)

    wind_direction: Mapped[Optional[float]] = Column(Float, nullable=True)
    wind_direction_unit: Mapped[Optional[str]] = Column(String(32), nullable=True)

    is_day: Mapped[Optional[bool]] = Column(Boolean, nullable=True)
    ingested_at_dtz: Mapped[datetime] = Column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_updated_at_dtz: Mapped[datetime] = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

class StgOpenMeteoAir(Base):
    """Normalized air-quality reading returned by Open-Meteo."""
    __tablename__ = "open_meteo_air"
    __table_args__ = {"schema": "stg"}

    event_id: Mapped[str] = Column(Text, primary_key=True)

    open_meteo_start_time: Mapped[datetime] = Column(DateTime(timezone=True), nullable=False, index=True)
    open_meteo_end_time: Mapped[datetime] = Column(DateTime(timezone=True), nullable=False, index=True)
    record_type: Mapped[str] = Column(String(32), nullable=False)  # current or forecast
    record_frequency_min: Mapped[int] = Column(Integer, nullable=False)  # 15, 60, 86,400

    latitude: Mapped[float] = Column(Float, nullable=False)
    longitude: Mapped[float] = Column(Float, nullable=False)

    pm2_5: Mapped[Optional[float]] = Column(Float, nullable=True)
    pm2_5_unit: Mapped[Optional[str]] = Column(String(32), nullable=True)

    pm10: Mapped[Optional[float]] = Column(Float, nullable=True)
    pm10_unit: Mapped[Optional[str]] = Column(String(32), nullable=True)

    us_aqi: Mapped[Optional[int]] = Column(Integer, nullable=True)
    us_aqi_unit: Mapped[Optional[str]] = Column(String(32), nullable=True)

    ozone: Mapped[Optional[float]] = Column(Float, nullable=True)
    ozone_unit: Mapped[Optional[str]] = Column(String(32), nullable=True)

    uv_index: Mapped[Optional[float]] = Column(Float, nullable=True)
    uv_index_unit: Mapped[Optional[str]] = Column(String(32), nullable=True)

    ingested_at_dtz: Mapped[datetime] = Column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_updated_at_dtz: Mapped[datetime] = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )