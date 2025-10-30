"""OpenAQ dimension and staging models."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    Numeric,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy.sql import func

from weather_insight.db.models.base import Base
from weather_insight.db.utils import utcnow


class DimOpenAQLocation(Base):
    """Current-location dimension rows for OpenAQ sites."""
    __tablename__ = "dim_openaq_locations"
    __table_args__ = (
        UniqueConstraint("openaq_locations_id", "is_current", name="uq_location_current"),
        {"schema": "ref"},
    )

    locations_id_sk: Mapped[int] = Column(BigInteger, primary_key=True)
    openaq_locations_id: Mapped[int] = Column(Integer, nullable=False)
    name: Mapped[Optional[str]] = Column(Text)
    locality: Mapped[Optional[str]] = Column(Text)
    timezone: Mapped[Optional[str]] = Column(Text)
    country_code: Mapped[Optional[str]] = Column(Text)
    country_id: Mapped[Optional[int]] = Column(Integer)
    country_name: Mapped[Optional[str]] = Column(Text)
    owner_id: Mapped[Optional[int]] = Column(Integer)
    owner_name: Mapped[Optional[str]] = Column(Text)
    provider_id: Mapped[Optional[int]] = Column(Integer)
    provider_name: Mapped[Optional[str]] = Column(Text)
    latitude: Mapped[Optional[float]] = Column(Float(53))
    longitude: Mapped[Optional[float]] = Column(Float(53))
    first_seen_at_dtz: Mapped[Optional[datetime]] = Column(DateTime(timezone=True))
    last_seen_at_dtz: Mapped[Optional[datetime]] = Column(DateTime(timezone=True))
    effective_start_at_dtz: Mapped[datetime] = Column(
        DateTime(timezone=True), default=utcnow, nullable=False
    )
    effective_end_at_dtz: Mapped[Optional[datetime]] = Column(DateTime(timezone=True))
    is_current: Mapped[bool] = Column(Boolean, default=True, nullable=False)

    licenses: Mapped[list["OpenAQLocationLicenseBridge"]] = relationship(
        "OpenAQLocationLicenseBridge",
        back_populates="location",
        cascade="all, delete-orphan",
    )


class DimOpenAQSensor(Base):
    """Current sensor dimension rows tied to OpenAQ locations."""
    __tablename__ = "dim_openaq_sensors"
    __table_args__ = (
        UniqueConstraint("openaq_sensors_id", "is_current", name="uq_sensor_current"),
        {"schema": "ref"},
    )

    sensors_id_sk: Mapped[int] = Column(BigInteger, primary_key=True)
    openaq_sensors_id: Mapped[int] = Column(Integer, nullable=False)
    locations_id_sk: Mapped[Optional[int]] = Column(
        ForeignKey("ref.dim_openaq_locations.locations_id_sk")
    )
    parameter_name: Mapped[Optional[str]] = Column(Text)
    parameter_id: Mapped[Optional[int]] = Column(Integer)
    parameter_display_name: Mapped[Optional[str]] = Column(Text)
    parameter_units: Mapped[Optional[str]] = Column(Text)
    last_updated_at_dtz: Mapped[datetime] = Column(
        DateTime(timezone=True), default=utcnow, nullable=False
    )
    effective_start_at_dtz: Mapped[datetime] = Column(
        DateTime(timezone=True), default=utcnow, nullable=False
    )
    effective_end_at_dtz: Mapped[Optional[datetime]] = Column(DateTime(timezone=True))
    is_current: Mapped[bool] = Column(Boolean, default=True, nullable=False)


class DimOpenAQLicense(Base):
    """License dimension rows describing OpenAQ data usage terms."""
    __tablename__ = "dim_openaq_licenses"
    __table_args__ = (
        UniqueConstraint("openaq_license_id", "is_current", name="uq_license_current"),
        {"schema": "ref"},
    )

    license_id_sk: Mapped[int] = Column(BigInteger, primary_key=True)
    openaq_license_id: Mapped[int] = Column(Integer, nullable=False)
    name: Mapped[Optional[str]] = Column(Text)
    commercial_use_allowed: Mapped[Optional[bool]] = Column(Boolean)
    attribution_required: Mapped[Optional[bool]] = Column(Boolean)
    share_alike_required: Mapped[Optional[bool]] = Column(Boolean)
    modification_allowed: Mapped[Optional[bool]] = Column(Boolean)
    redistribution_allowed: Mapped[Optional[bool]] = Column(Boolean)
    source_url: Mapped[Optional[str]] = Column(Text)
    last_updated_at_dtz: Mapped[datetime] = Column(
        DateTime(timezone=True), default=utcnow, nullable=False
    )
    effective_start_at_dtz: Mapped[datetime] = Column(
        DateTime(timezone=True), default=utcnow, nullable=False
    )
    effective_end_at_dtz: Mapped[Optional[datetime]] = Column(DateTime(timezone=True))
    is_current: Mapped[bool] = Column(Boolean, default=True, nullable=False)

    locations: Mapped[list["OpenAQLocationLicenseBridge"]] = relationship(
        "OpenAQLocationLicenseBridge",
        back_populates="license",
        cascade="all, delete-orphan",
    )


class OpenAQLocationLicenseBridge(Base):
    """Bridge table linking locations and licenses with SCD-2 semantics."""
    __tablename__ = "openaq_locations_license_bridge"
    __table_args__ = (
        UniqueConstraint("locations_id_sk", "license_id_sk", "is_current", name="uq_loc_lic_current"),
        {"schema": "ref"},
    )

    bridge_id: Mapped[int] = Column(BigInteger, primary_key=True)
    locations_id_sk: Mapped[int] = Column(
        ForeignKey("ref.dim_openaq_locations.locations_id_sk"), nullable=False
    )
    license_id_sk: Mapped[int] = Column(
        ForeignKey("ref.dim_openaq_licenses.license_id_sk"), nullable=False
    )
    license_attribution_name: Mapped[Optional[str]] = Column(Text)
    license_attribution_url: Mapped[Optional[str]] = Column(Text)
    license_date_from_dtz: Mapped[Optional[datetime]] = Column(DateTime(timezone=True))
    license_date_to_dtz: Mapped[Optional[datetime]] = Column(DateTime(timezone=True))
    last_updated_at_dtz: Mapped[datetime] = Column(
        DateTime(timezone=True), default=utcnow, nullable=False
    )
    effective_start_at_dtz: Mapped[datetime] = Column(
        DateTime(timezone=True), default=utcnow, nullable=False
    )
    effective_end_at_dtz: Mapped[Optional[datetime]] = Column(DateTime(timezone=True))
    is_current: Mapped[bool] = Column(Boolean, default=True, nullable=False)
    location: Mapped["DimOpenAQLocation"] = relationship(
        "DimOpenAQLocation", back_populates="licenses"
    )
    license: Mapped["DimOpenAQLicense"] = relationship(
        "DimOpenAQLicense", back_populates="locations"
    )


class StgOpenaqLatestMeasurements(Base):
    """Staging table for latest OpenAQ measurements by location and sensor."""
    __tablename__ = "openaq_measurements_by_location"
    __table_args__ = {"schema": "stg"}

    event_id: Mapped[str] = Column(Text, primary_key=True)
    openaq_locations_id: Mapped[int] = Column(BigInteger, index=True)
    openaq_sensors_id: Mapped[int] = Column(BigInteger, index=True)
    datetime_utc: Mapped[datetime] = Column(DateTime(timezone=True))
    # raw value from source (may be negative / invalid)
    value: Mapped[float | None] = Column(Numeric, nullable=True)
    # cleaned value used by downstream marts (e.g., null when invalid)
    value_normalized: Mapped[float | None] = Column(Numeric, nullable=True)
    is_valid: Mapped[bool] = Column(Boolean, default=True, nullable=False)
    quality_status: Mapped[str | None] = Column(Text, nullable=True)
    source: Mapped[str | None] = Column(Text, nullable=True)
    event_type: Mapped[str | None] = Column(Text, nullable=True)
    source_version: Mapped[str | None] = Column(Text, nullable=True)
    ingested_at_dtz: Mapped[datetime] = Column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_updated_at_dtz: Mapped[datetime] = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


__all__ = [
    "DimOpenAQLocation",
    "DimOpenAQSensor",
    "DimOpenAQLicense",
    "OpenAQLocationLicenseBridge",
    "StgOpenaqLatestMeasurements",
]
