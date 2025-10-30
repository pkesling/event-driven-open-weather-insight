"""Session and engine factories for the warehouse database."""
from __future__ import annotations
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def build_dsn() -> str:
    """Build a Postgres DSN from environment variables."""
    host = os.getenv("WAREHOUSE_DB_HOST", "postgres-warehouse")
    port = os.getenv("WAREHOUSE_DB_PORT", "5432")
    user = os.getenv("WAREHOUSE_DB_USER", "analytics")
    password = os.getenv("WAREHOUSE_DB_PASSWORD", "analytics")
    db = os.getenv("WAREHOUSE_DB_DB", "opendata")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

ENGINE = create_engine(
    build_dsn(),
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    future=True,
)


def get_session_factory() -> sessionmaker:
    """Return a configured SQLAlchemy session factory."""
    return sessionmaker(bind=ENGINE, autoflush=False, autocommit=False, future=True)

SessionLocal = sessionmaker(bind=ENGINE, autoflush=False, autocommit=False, future=True)
