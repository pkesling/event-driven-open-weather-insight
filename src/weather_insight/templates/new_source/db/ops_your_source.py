from __future__ import annotations

from typing import Any, Mapping

from sqlalchemy import select
from sqlalchemy.orm import Session

from weather_insight.db.models.your_source import StgYourSource


def ensure_your_source_record(session: Session, payload: Mapping[str, Any]) -> StgYourSource:
    """Upsert a single your_source record into staging (SQLAlchemy)."""
    stmt = select(StgYourSource).where(StgYourSource.event_id == payload["event_id"]).limit(1)
    row = session.execute(stmt).scalar_one_or_none()

    if row is None:
        row = StgYourSource(**payload)
        session.add(row)
    else:
        for k, v in payload.items():
            setattr(row, k, v)
    return row
