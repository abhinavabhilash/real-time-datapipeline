"""Idempotent batch insert and upsert operations for PostgreSQL."""

from __future__ import annotations

import logging
from typing import Iterable, Sequence

from psycopg2.extras import execute_values

from database.db_connection import get_connection

logger = logging.getLogger(__name__)


def insert_valid_events(rows: Sequence[tuple]) -> int:
    """Insert deduplicated valid events into `ecommerce_events` without duplicates."""
    if not rows:
        return 0

    sql = """
        INSERT INTO ecommerce_events (
            event_id, event_type, event_time, user_id, session_id,
            product_id, product_name, category, quantity, unit_price,
            total_amount, recognized_revenue, currency, order_id, payment_status,
            device_type, country, is_payment_event, is_order_event,
            kafka_partition, kafka_offset, processed_at
        )
        VALUES %s
        ON CONFLICT (event_id) DO NOTHING;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
            affected = cur.rowcount
    return affected


def upsert_windowed_metrics(rows: Sequence[tuple]) -> int:
    """Upsert 1-minute global KPIs into `windowed_metrics`."""
    if not rows:
        return 0

    sql = """
        INSERT INTO windowed_metrics (
            window_start, window_end, total_events, total_orders,
            successful_payments, cancelled_orders, total_revenue, active_users
        )
        VALUES %s
        ON CONFLICT (window_start) DO UPDATE SET
            window_end          = EXCLUDED.window_end,
            total_events        = EXCLUDED.total_events,
            total_orders        = EXCLUDED.total_orders,
            successful_payments = EXCLUDED.successful_payments,
            cancelled_orders    = EXCLUDED.cancelled_orders,
            total_revenue       = EXCLUDED.total_revenue,
            active_users        = EXCLUDED.active_users,
            updated_at          = NOW();
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=200)
            affected = cur.rowcount
    return affected


def upsert_product_metrics(rows: Sequence[tuple]) -> int:
    """Upsert product-level windowed sales and traffic into `product_window_metrics`."""
    if not rows:
        return 0

    sql = """
        INSERT INTO product_window_metrics (
            window_start, window_end, product_id, product_name, category,
            view_count, cart_add_count, order_count, units_sold, product_revenue
        )
        VALUES %s
        ON CONFLICT (window_start, product_id) DO UPDATE SET
            window_end      = EXCLUDED.window_end,
            product_name    = EXCLUDED.product_name,
            category        = EXCLUDED.category,
            view_count      = EXCLUDED.view_count,
            cart_add_count  = EXCLUDED.cart_add_count,
            order_count     = EXCLUDED.order_count,
            units_sold      = EXCLUDED.units_sold,
            product_revenue = EXCLUDED.product_revenue,
            updated_at      = NOW();
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
            affected = cur.rowcount
    return affected


def upsert_event_type_metrics(rows: Sequence[tuple]) -> int:
    """Upsert event-type distribution metrics into `event_type_window_metrics`."""
    if not rows:
        return 0

    sql = """
        INSERT INTO event_type_window_metrics (
            window_start, window_end, event_type, event_count, recognized_revenue
        )
        VALUES %s
        ON CONFLICT (window_start, event_type) DO UPDATE SET
            window_end         = EXCLUDED.window_end,
            event_count        = EXCLUDED.event_count,
            recognized_revenue = EXCLUDED.recognized_revenue,
            updated_at         = NOW();
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=200)
            affected = cur.rowcount
    return affected


def insert_dead_letter_events(rows: Iterable[tuple]) -> int:
    """Insert invalid/malformed records into `dead_letter_events` idempotently."""
    row_list = list(rows)
    if not row_list:
        return 0

    sql = """
        INSERT INTO dead_letter_events (
            raw_key, raw_value, validation_error,
            kafka_topic, kafka_partition, kafka_offset, failed_at
        )
        VALUES %s
        ON CONFLICT (kafka_topic, kafka_partition, kafka_offset)
        WHERE kafka_topic IS NOT NULL DO NOTHING;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, row_list, page_size=500)
            affected = cur.rowcount
    return affected
