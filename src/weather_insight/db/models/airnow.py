"""Airnow staging models."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    Text,
)
from sqlalchemy.orm import Mapped

from weather_insight.db.models.base import Base
from weather_insight.db.utils import utcnow


class StgAirnowAirQualityForecast(Base):
    """Staging table for AirNow forecast events."""
    __tablename__ = "airnow_air_quality_forecast"
    __table_args__ = {"schema": "stg"}

    event_id: Mapped[str] = Column(Text, primary_key=True)
    source: Mapped[str] = Column(Text)
    date_issue_dtz: Mapped[datetime] = Column(DateTime(timezone=True))
    date_forecast_dtz: Mapped[datetime] = Column(DateTime(timezone=True))
    reporting_area: Mapped[str] = Column(Text)
    state_code: Mapped[str] = Column(Text)
    latitude: Mapped[float] = Column(Float(53))
    longitude: Mapped[float] = Column(Float(53))
    parameter_name: Mapped[str] = Column(Text)
    aqi: Mapped[int] = Column(Integer)
    category_number: Mapped[int] = Column(Integer)
    category_name: Mapped[str] = Column(Text)
    action_day: Mapped[bool] = Column(Boolean)
    discussion: Mapped[str] = Column(Text)
    raw_data: Mapped[str] = Column(Text)

    effective_start_at_dtz: Mapped[datetime] = Column(
        DateTime(timezone=True), default=utcnow, nullable=False
    )
    effective_end_at_dtz: Mapped[Optional[datetime]] = Column(DateTime(timezone=True))
    is_current: Mapped[bool] = Column(Boolean, default=True, nullable=False)

__all__ = [
    "StgAirnowAirQualityForecast",
]
