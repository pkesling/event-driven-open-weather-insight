"""Weather forecast staging models."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    Numeric,
    Text,
)
from sqlalchemy.orm import Mapped
from sqlalchemy.sql import func

from weather_insight.db.models.base import Base


class StgWeatherHourlyForecast(Base):
    """Staging table for hourly weather.gov forecasts."""
    __tablename__ = "weather_hourly_forecast"
    __table_args__ = {"schema": "stg"}

    event_id: Mapped[str] = Column(Text, primary_key=True)
    event_type: Mapped[str] = Column(Text)
    source: Mapped[str] = Column(Text)
    office: Mapped[str] = Column(Text)
    grid_id: Mapped[str] = Column(Text, nullable=True)
    grid_x: Mapped[int] = Column(Integer)
    grid_y: Mapped[int] = Column(Integer)
    start_time: Mapped[datetime] = Column(DateTime(timezone=True))
    end_time: Mapped[datetime] = Column(DateTime(timezone=True))
    is_daytime: Mapped[bool] = Column(Boolean)

    temperature: Mapped[float] = Column(Numeric)
    temperature_min: Mapped[float] = Column(Numeric, nullable=True)
    temperature_max: Mapped[float] = Column(Numeric, nullable=True)
    temperature_unit: Mapped[str] = Column(Text)
    temperature_trend: Mapped[str] = Column(Text, nullable=True)

    relative_humidity: Mapped[float] = Column(Numeric)
    relative_humidity_min: Mapped[float] = Column(Numeric, nullable=True)
    relative_humidity_max: Mapped[float] = Column(Numeric, nullable=True)
    relative_humidity_unit: Mapped[str] = Column(Text)

    dewpoint: Mapped[float] = Column(Numeric)
    dewpoint_min: Mapped[float] = Column(Numeric, nullable=True)
    dewpoint_max: Mapped[float] = Column(Numeric, nullable=True)
    dewpoint_unit: Mapped[str] = Column(Text)

    wind_speed: Mapped[float] = Column(Numeric)
    wind_speed_min: Mapped[float] = Column(Numeric, nullable=True)
    wind_speed_max: Mapped[float] = Column(Numeric, nullable=True)
    wind_speed_unit: Mapped[str] = Column(Text)
    wind_direction: Mapped[str] = Column(Text)

    wind_gust: Mapped[float] = Column(Numeric)
    wind_gust_min: Mapped[float] = Column(Numeric, nullable=True)
    wind_gust_max: Mapped[float] = Column(Numeric, nullable=True)
    wind_gust_unit: Mapped[str] = Column(Text)

    probability_of_precipitation: Mapped[float] = Column(Numeric)
    probability_of_precipitation_min: Mapped[float] = Column(Numeric, nullable=True)
    probability_of_precipitation_max: Mapped[float] = Column(Numeric, nullable=True)
    probability_of_precipitation_unit: Mapped[str] = Column(Text)

    short_forecast: Mapped[str] = Column(Text, nullable=True)
    detailed_forecast: Mapped[str] = Column(Text, nullable=True)

    ingested_at_dtz: Mapped[datetime] = Column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_updated_at_dtz: Mapped[datetime] = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


__all__ = ["StgWeatherHourlyForecast"]
