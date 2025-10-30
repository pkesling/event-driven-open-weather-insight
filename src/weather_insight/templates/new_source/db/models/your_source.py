from __future__ import annotations

from sqlalchemy import Column, DateTime, Float, Integer, Text
from sqlalchemy.orm import Mapped

from weather_insight.db.models.base import Base


class StgYourSource(Base):
    """SQLAlchemy model for staging your_source events."""
    __tablename__ = "your_source"
    __table_args__ = {"schema": "stg"}

    event_id: Mapped[str] = Column(Text, primary_key=True)
    source: Mapped[str] = Column(Text)
    observed_at: Mapped[DateTime] = Column(DateTime(timezone=True))
    metric_value: Mapped[float | None] = Column(Float, nullable=True)
    metric_unit: Mapped[str | None] = Column(Text, nullable=True)


__all__ = ["StgYourSource"]
