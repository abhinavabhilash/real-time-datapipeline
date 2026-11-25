"""PostgreSQL connection management and schema initialization."""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import psycopg2
from psycopg2.extensions import connection as PgConnection
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from config.settings import settings

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"


@contextmanager
def get_connection() -> Iterator[PgConnection]:
    """Yield a PostgreSQL connection that commits on success and rolls back on error."""
    conn = psycopg2.connect(
        host=settings.postgres.host,
        port=settings.postgres.port,
        dbname=settings.postgres.database,
        user=settings.postgres.user,
        password=settings.postgres.password,
        connect_timeout=10,
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_sqlalchemy_engine() -> Engine:
    """Return a SQLAlchemy Engine for analytical queries (e.g. Streamlit)."""
    return create_engine(settings.postgres.sqlalchemy_url, pool_pre_ping=True)


def init_database(max_retries: int = 3, retry_delay_sec: float = 2.0) -> None:
    """Create tables and indexes defined in schema.sql idempotently."""
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")

    for attempt in range(1, max_retries + 1):
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(schema_sql)
            logger.info(
                "PostgreSQL schema initialized successfully on %s:%s/%s.",
                settings.postgres.host,
                settings.postgres.port,
                settings.postgres.database,
            )
            return
        except psycopg2.OperationalError as exc:
            logger.warning(
                "Attempt %d/%d: PostgreSQL connection failed (%s). Retrying in %.1fs...",
                attempt,
                max_retries,
                exc,
                retry_delay_sec,
            )
            if attempt == max_retries:
                raise
            time.sleep(retry_delay_sec)


def test_connection() -> dict[str, int]:
    """Verify PostgreSQL connectivity and return row counts of pipeline tables."""
    tables = (
        "ecommerce_events",
        "windowed_metrics",
        "product_window_metrics",
        "event_type_window_metrics",
        "dead_letter_events",
    )
    counts: dict[str, int] = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM {table};")
                counts[table] = int(cur.fetchone()[0])
    return counts


if __name__ == "__main__":
    init_database()
    table_counts = test_connection()
    for name, count in table_counts.items():
        logger.info("Table %-28s | rows=%d", name, count)
