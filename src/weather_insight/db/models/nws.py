# src/weather_insight/db/models/nws.py
from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, relationship

from weather_insight.db.models.base import Base
from weather_insight.db.models.geo import DimGeoLocation


class DimNWSGridpoint(Base):
    __tablename__ = "dim_nws_gridpoints"
    __table_args__ = (
        UniqueConstraint(
            "office_id", "grid_x", "grid_y", name="uq_nws_gridpoint_unique"
        ),
        {"schema": "ref"},
    )

    nws_gridpoint_id_sk: Mapped[int] = Column(BigInteger, primary_key=True)

    office_id: Mapped[str] = Column(String(16), nullable=False)
    grid_x: Mapped[int] = Column(Integer, nullable=False)
    grid_y: Mapped[int] = Column(Integer, nullable=False)

    latitude: Mapped[float] = Column(Float, nullable=False)
    longitude: Mapped[float] = Column(Float, nullable=False)
    elevation_m: Mapped[Optional[float]] = Column(Float, nullable=True)

    timezone: Mapped[Optional[str]] = Column(Text, nullable=True)
    forecast_base_url: Mapped[Optional[str]] = Column(Text, nullable=True)

    geo_location_id_sk: Mapped[Optional[int]] = Column(
        BigInteger,
        ForeignKey("ref.dim_geo_locations.geo_location_id_sk"),
        nullable=True,
    )

    geo_location: Mapped[Optional[DimGeoLocation]] = relationship(
        "DimGeoLocation",
        lazy="joined",
    )

    created_at_dtz: Mapped[datetime] = Column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at_dtz: Mapped[datetime] = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
