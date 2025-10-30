"""Schema bootstrap utilities for warehouse databases."""
import os

from sqlalchemy import text
from . import ENGINE

from weather_insight.utils.logging_utils import get_tagged_logger, setup_logging
setup_logging(level=os.getenv("LOG_LEVEL", "INFO"))
logger = get_tagged_logger(__name__, tag="openaq_ingest_dag")


def ensure_schemas(schemas=("ref", "stg")) -> None:
    """Create schemas if they do not already exist."""
    logger.debug("Ensuring schemas exist")
    with ENGINE.begin() as conn:
        for s in schemas:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{s}"'))
            # Optional: set privileges/ownership as needed
            # conn.execute(text(f'ALTER SCHEMA "{s}" OWNER TO analytics'))
