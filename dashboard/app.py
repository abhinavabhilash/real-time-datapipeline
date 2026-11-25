"""Interactive real-time e-commerce analytics dashboard built with Streamlit."""

from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from database.db_connection import get_sqlalchemy_engine

st.set_page_config(
    page_title="Real-Time E-Commerce Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

TIME_WINDOWS = {
    "Last 15 minutes": "15 minutes",
    "Last 1 hour": "1 hour",
    "Last 24 hours": "24 hours",
    "All time": None,
}


@st.cache_resource
def _get_engine():
    return get_sqlalchemy_engine()


def _build_time_filter(column: str, interval_str: str | None) -> str:
    if interval_str is None:
        return "1=1"
    return f"{column} >= NOW() - INTERVAL '{interval_str}'"


def load_kpis(interval_str: str | None) -> dict[str, float | int]:
    """Fetch top-level KPIs from curated events (zero double-counting)."""
    engine = _get_engine()
    where_clause = _build_time_filter("event_time", interval_str)
    query = text(
        f"""
        SELECT
            COUNT(*) AS total_events,
            COUNT(*) FILTER (WHERE event_type = 'order_placed') AS total_orders,
            COUNT(*) FILTER (WHERE is_payment_event) AS successful_payments,
            COUNT(*) FILTER (WHERE event_type = 'order_cancelled') AS cancelled_orders,
            COALESCE(SUM(recognized_revenue), 0.0) AS total_revenue,
            COUNT(DISTINCT user_id) AS active_users
        FROM ecommerce_events
        WHERE {where_clause};
        """
    )
    with engine.connect() as conn:
        row = conn.execute(query).mappings().first()
    return dict(row) if row else {
        "total_events": 0,
        "total_orders": 0,
        "successful_payments": 0,
        "cancelled_orders": 0,
        "total_revenue": 0.0,
        "active_users": 0,
    }


def load_windowed_trends(interval_str: str | None) -> pd.DataFrame:
    """Fetch 1-minute event-time windowed revenue and transaction trends."""
    engine = _get_engine()
    where_clause = _build_time_filter("window_start", interval_str)
    query = text(
        f"""
        SELECT
            window_start,
            window_end,
            total_events,
            total_orders,
            successful_payments,
            cancelled_orders,
            total_revenue::float AS total_revenue,
            active_users
        FROM windowed_metrics
        WHERE {where_clause}
        ORDER BY window_start ASC;
        """
    )
    return pd.read_sql(query, engine)


def load_product_sales(interval_str: str | None) -> pd.DataFrame:
    """Fetch product-wise views, cart adds, orders, units sold, and revenue."""
    engine = _get_engine()
    where_clause = _build_time_filter("event_time", interval_str)
    query = text(
        f"""
        SELECT
            product_id,
            product_name,
            category,
            COUNT(*) FILTER (WHERE event_type = 'product_view') AS views,
            COUNT(*) FILTER (WHERE event_type = 'cart_update') AS cart_updates,
            COUNT(*) FILTER (WHERE event_type = 'order_placed') AS orders,
            COALESCE(SUM(quantity) FILTER (WHERE is_payment_event), 0) AS units_sold,
            COALESCE(SUM(recognized_revenue), 0.0)::float AS revenue
        FROM ecommerce_events
        WHERE {where_clause}
        GROUP BY product_id, product_name, category
        ORDER BY revenue DESC, orders DESC;
        """
    )
    return pd.read_sql(query, engine)


def load_event_distribution(interval_str: str | None) -> pd.DataFrame:
    """Fetch counts grouped by event_type."""
    engine = _get_engine()
    where_clause = _build_time_filter("event_time", interval_str)
    query = text(
        f"""
        SELECT
            event_type,
            COUNT(*) AS event_count
        FROM ecommerce_events
        WHERE {where_clause}
        GROUP BY event_type
        ORDER BY event_count DESC;
        """
    )
    return pd.read_sql(query, engine)


def load_recent_events(limit: int = 20) -> pd.DataFrame:
    """Fetch the most recent curated events."""
    engine = _get_engine()
    query = text(
        """
        SELECT
            event_time,
            event_type,
            user_id,
            product_name,
            quantity,
            total_amount::float AS total_amount,
            recognized_revenue::float AS recognized_revenue,
            payment_status,
            country
        FROM ecommerce_events
        ORDER BY event_time DESC
        LIMIT :limit;
        """
    )
    return pd.read_sql(query, engine, params={"limit": limit})


def load_dlq_summary() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch DLQ error counts and recent dead-letter payloads."""
    engine = _get_engine()
    summary_q = text(
        """
        SELECT validation_error, COUNT(*) AS error_count
        FROM dead_letter_events
        GROUP BY validation_error
        ORDER BY error_count DESC;
        """
    )
    recent_q = text(
        """
        SELECT failed_at, validation_error, kafka_partition, kafka_offset, raw_value
        FROM dead_letter_events
        ORDER BY id DESC
        LIMIT 10;
        """
    )
    return pd.read_sql(summary_q, engine), pd.read_sql(recent_q, engine)


def main() -> None:
    st.title("Real-Time E-Commerce Streaming Analytics")
    st.caption(
        "Live pipeline: Python Producer → Apache Kafka → Spark Structured Streaming → PostgreSQL"
    )

    with st.sidebar:
        st.header("Dashboard Controls")
        selected_window = st.selectbox(
            "Time Range",
            options=list(TIME_WINDOWS.keys()),
            index=3,
        )
        interval_str = TIME_WINDOWS[selected_window]

        auto_refresh = st.checkbox("Auto-refresh", value=True)
        refresh_sec = st.slider(
            "Refresh interval (seconds)",
            min_value=3,
            max_value=30,
            value=5,
            disabled=not auto_refresh,
        )
        if st.button("Refresh Now"):
            st.rerun()

        st.markdown("---")
        st.markdown(
            "**Revenue Business Rule:**\n"
            "Revenue includes only `payment_success` events (`payment_status = 'SUCCESS'`) "
            "to prevent double-counting `order_placed` records."
        )
        st.caption(
            f"Last updated (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}"
        )

    try:
        kpis = load_kpis(interval_str)
        trends_df = load_windowed_trends(interval_str)
        products_df = load_product_sales(interval_str)
        dist_df = load_event_distribution(interval_str)
        recent_df = load_recent_events(limit=20)
        dlq_summary_df, dlq_recent_df = load_dlq_summary()
    except Exception as exc:
        st.error(
            f"Unable to query PostgreSQL: {exc}. "
            "Make sure Docker PostgreSQL is running and `python -m database.db_connection` succeeded."
        )
        return

    # --- KPI Cards ---
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Events", f"{int(kpis['total_events']):,}")
    c2.metric("Total Orders", f"{int(kpis['total_orders']):,}")
    c3.metric("Successful Payments", f"{int(kpis['successful_payments']):,}")
    c4.metric("Total Revenue", f"${float(kpis['total_revenue']):,.2f}")
    c5.metric("Active Users", f"{int(kpis['active_users']):,}")

    st.markdown("---")

    # --- Row 1: Revenue Trend & Transaction Activity Over Time ---
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Revenue Trend (1-Min Event-Time Windows)")
        if trends_df.empty:
            st.info("Waiting for windowed revenue data from Spark...")
        else:
            fig_rev = px.area(
                trends_df,
                x="window_start",
                y="total_revenue",
                markers=True,
                labels={"window_start": "Window Start (UTC)", "total_revenue": "Revenue ($)"},
            )
            fig_rev.update_layout(margin=dict(l=10, r=10, t=20, b=10), height=340)
            st.plotly_chart(fig_rev, use_container_width=True)

    with col_right:
        st.subheader("Transaction Activity Over Time")
        if trends_df.empty:
            st.info("Waiting for windowed transaction data from Spark...")
        else:
            fig_tx = go.Figure()
            fig_tx.add_trace(
                go.Scatter(
                    x=trends_df["window_start"],
                    y=trends_df["total_events"],
                    mode="lines+markers",
                    name="All Events",
                )
            )
            fig_tx.add_trace(
                go.Scatter(
                    x=trends_df["window_start"],
                    y=trends_df["total_orders"],
                    mode="lines+markers",
                    name="Orders Placed",
                )
            )
            fig_tx.add_trace(
                go.Scatter(
                    x=trends_df["window_start"],
                    y=trends_df["successful_payments"],
                    mode="lines+markers",
                    name="Successful Payments",
                )
            )
            fig_tx.add_trace(
                go.Scatter(
                    x=trends_df["window_start"],
                    y=trends_df["cancelled_orders"],
                    mode="lines+markers",
                    name="Cancelled Orders",
                )
            )
            fig_tx.update_layout(
                xaxis_title="Window Start (UTC)",
                yaxis_title="Count",
                margin=dict(l=10, r=10, t=20, b=10),
                height=340,
            )
            st.plotly_chart(fig_tx, use_container_width=True)

    # --- Row 2: Product-Wise Sales & Event Type Distribution ---
    col_prod, col_dist = st.columns((1.4, 1))

    with col_prod:
        st.subheader("Product-Wise Revenue")
        if products_df.empty:
            st.info("No product sales recorded yet.")
        else:
            fig_prod = px.bar(
                products_df,
                x="revenue",
                y="product_name",
                color="category",
                orientation="h",
                labels={"revenue": "Recognized Revenue ($)", "product_name": "Product"},
            )
            fig_prod.update_layout(
                yaxis={"categoryorder": "total ascending"},
                margin=dict(l=10, r=10, t=20, b=10),
                height=380,
            )
            st.plotly_chart(fig_prod, use_container_width=True)

    with col_dist:
        st.subheader("Event Type Distribution")
        if dist_df.empty:
            st.info("No events processed yet.")
        else:
            fig_dist = px.pie(
                dist_df,
                names="event_type",
                values="event_count",
                hole=0.45,
            )
            fig_dist.update_layout(margin=dict(l=10, r=10, t=20, b=10), height=380)
            st.plotly_chart(fig_dist, use_container_width=True)

    # --- Row 3: Product Performance Table ---
    st.subheader("Product Performance Breakdown")
    if not products_df.empty:
        st.dataframe(products_df, use_container_width=True, hide_index=True)

    # --- Row 4: Recent Events & Dead-Letter Queue Monitoring ---
    tab_events, tab_dlq = st.tabs(["Recent Valid Events", "Dead-Letter Queue (Invalid Events)"])

    with tab_events:
        if recent_df.empty:
            st.info("No curated events in PostgreSQL yet.")
        else:
            st.dataframe(recent_df, use_container_width=True, hide_index=True)

    with tab_dlq:
        dlq_c1, dlq_c2 = st.columns((1, 2))
        with dlq_c1:
            st.markdown("**Validation Error Breakdown**")
            if dlq_summary_df.empty:
                st.success("No invalid events captured.")
            else:
                st.dataframe(dlq_summary_df, use_container_width=True, hide_index=True)
        with dlq_c2:
            st.markdown("**Latest Dead-Letter Records**")
            if not dlq_recent_df.empty:
                st.dataframe(dlq_recent_df, use_container_width=True, hide_index=True)

    if auto_refresh:
        time.sleep(refresh_sec)
        st.rerun()


if __name__ == "__main__":
    main()
