"""
Canonical geospatial location dimension.

Author: Phil Kesling
License: MIT
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    Float,
    Text,
    func,
    Column
)
from sqlalchemy.orm import Mapped

from .base import Base


class DimGeoLocation(Base):
    __tablename__ = "dim_geo_locations"
    __table_args__ = {"schema": "ref"}

    geo_location_id_sk: Mapped[int] = Column(BigInteger, primary_key=True)
    canonical_name: Mapped[str | None] = Column(Text, nullable=True)

    latitude: Mapped[float] = Column(Float, nullable=False)
    longitude: Mapped[float] = Column(Float, nullable=False)

    timezone: Mapped[str | None] = Column(Text, nullable=True)
    country_code: Mapped[str | None] = Column(Text, nullable=True)
    admin1_name: Mapped[str | None] = Column(Text, nullable=True)
    admin2_name: Mapped[str | None] = Column(Text, nullable=True)

    created_at_dtz: Mapped[datetime] = Column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at_dtz: Mapped[datetime] = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
